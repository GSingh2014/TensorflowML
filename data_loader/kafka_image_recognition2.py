import base64
import os
import json
import numpy as np
from io import BytesIO, StringIO
from timeit import default_timer as timer
from PIL import Image
import datetime as dt
from random import randint

# Streaming imports
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer

# Object detection imports
import tensorflow as tf
from object_detection.utils import ops as utils_ops
from object_detection.utils import label_map_util
from object_detection.utils import visualization_utils as vis_util

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 pyspark-shell'


class SparkObjectDetector:
    """Stream WebCam Images to Kafka Endpoint.
    Keyword arguments:
    source -- Index of Video Device or Filename of Video-File
    interval -- Interval for capturing images in seconds (default 5)
    server -- Host + Port of Kafka Endpoint (default '127.0.0.1:9092')
    """

    def __init__(self,
                 interval=10,
                 model_file='',
                 labels_file='',
                 number_classes=90,
                 detect_treshold=.1,
                 topic_to_consume='sftp-topic',
                 topic_for_produce='resultstream',
                 kafka_endpoint='localhost:29092'):
        """Initialize Spark & TensorFlow environment."""
        self.topic_to_consume = topic_to_consume
        self.topic_for_produce = topic_for_produce
        self.kafka_endpoint = kafka_endpoint
        self.treshold = detect_treshold
        self.v_sectors = ['top', 'middle', 'bottom']
        self.h_sectors = ['left', 'center', 'right']

        # Create Kafka Producer for sending results
        # the value serializer for parsing JSON string should encode to utf-8 if it is not a dict (i.e. string),
        # else it should dump it as json
        self.producer = KafkaProducer(bootstrap_servers=kafka_endpoint,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict)
                                      else v.encode('utf-8'),
                                      max_request_size=204857600)

        # Load Labels & Categories
        label_map = label_map_util.load_labelmap(labels_file)
        categories = label_map_util.convert_label_map_to_categories(label_map,
                                                                    max_num_classes=number_classes,
                                                                    use_display_name=True
                                                                    )
        self.category_index = label_map_util.create_category_index(categories)

        #print('***Category Index****')
        #for idx in self.category_index:
        #    print(self.category_index[idx]['name'])

        # Load Spark Context
        sc = SparkContext("local[*]", "Streaming Video", {"spark.executor.heartbeatInterval": "3600s"})
        self.ssc = StreamingContext(sc, interval)  # , 3)

        # Make Spark logging less extensive
        log4jLogger = sc._jvm.org.apache.log4j
        log_level = log4jLogger.Level.ERROR
        log4jLogger.LogManager.getLogger('org').setLevel(log_level)
        log4jLogger.LogManager.getLogger('akka').setLevel(log_level)
        log4jLogger.LogManager.getLogger('kafka').setLevel(log_level)
        self.logger = log4jLogger.LogManager.getLogger(__name__)

        # Load Frozen Network Model & Broadcast to Worker Nodes
        with tf.gfile.FastGFile(model_file, 'rb') as f:
            model_data = f.read()
        self.model_data_bc = sc.broadcast(model_data)

        # Load Graph Definition
        self.graph_def = tf.GraphDef()
        self.graph_def.ParseFromString(self.model_data_bc.value)

    def start_processing(self):
        """Start consuming from Kafka endpoint and detect objects."""
        kvs = KafkaUtils.createDirectStream(self.ssc,
                                            [self.topic_to_consume],
                                            kafkaParams={'metadata.broker.list': self.kafka_endpoint,
                                                         "max.partition.fetch.bytes": "104857600",
                                                         "fetch.message.max.bytes": "204857600"}
                                            )
        kvs.foreachRDD(self.handler)
        self.ssc.start()
        self.ssc.awaitTermination()

    def load_image_into_numpy_array(self, image):
        """Convert PIL image to numpy array."""
        (im_width, im_height) = image.size
        return np.array(image.getdata()).reshape(
            (im_height, im_width, 3)).astype(np.uint8)

    def box_to_sector(self, box):
        """Transform object box in image into sector description."""
        width = box[3] - box[1]
        h_pos = box[1] + width / 2.0
        height = box[2] - box[0]
        v_pos = box[0] + height / 2.0
        h_sector = min(int(h_pos * 3), 2)  # 0: left, 1: center, 2: right
        v_sector = min(int(v_pos * 3), 2)  # 0: top, 1: middle, 2: bottom
        return (self.v_sectors[v_sector], self.h_sectors[h_sector])

    def get_annotated_image_as_text(self, image_np, output):
        """Paint the annotations on the image and serialize it into text."""
        vis_util.visualize_boxes_and_labels_on_image_array(
            image_np,
            output['detection_boxes'],
            output['detection_classes'],
            output['detection_scores'],
            self.category_index,
            instance_masks=output.get('detection_masks'),
            use_normalized_coordinates=True,
            line_thickness=3)

        # Serialize into text, so it can be send as json
        img = Image.fromarray(image_np)
        text_stream = BytesIO()
        img.save(text_stream, 'JPEG')
        contents = text_stream.getvalue()
        text_stream.close()
        img_as_text = base64.b64encode(contents).decode('utf-8')
        return img_as_text

    def format_object_desc(self, output):
        """Transform object detection output into nice list of dicts."""
        objs = []
        for i in range(len(output['detection_classes'])):
            # just for for nice output
            score = round(output['detection_scores'][i], 2)
            #print('****Score****')
            #print(score)
            if score > self.treshold:  # Only keep objects over treshold
                # Get label for category id
                #print('***INSIDE SCORE CONDITION***')
                cat_id = output['detection_classes'][i]
                #print('***cat_id***')
                #print(cat_id)
                label = self.category_index[cat_id]['name']
                #print('***label***')
                #print(label)

                # Get position of object box as [ymin, xmin, ymax, xmax]
                box = output['detection_boxes'][i]

                objs.append({
                    'label': label,
                    'score': str(score),
                    'sector': self.box_to_sector(box)
                })
        if len(objs) == 0:
            print('***DID NOT MEET THE SCORE***')

        return objs

    def detect_objects(self, event):
        """Use TensorFlow Model to detect objects."""
        # Load the image data from the json into PIL image & numpy array
        print(event['filename'])
        image_file_ext = str(event['filename']).split('.')[1]
        print(image_file_ext)
        image_file = event['filename'] if (image_file_ext in ['jpg', 'mp4']) else ''
        print(image_file)
        decoded = base64.b64decode(event['image'])
        print(decoded)
        stream = BytesIO(decoded)
        #print(stream)
        image = Image.open(stream)
        #print(image)
        image_np = self.load_image_into_numpy_array(image)
        #print(image_np)
        stream.close()

        # Load Network Graph
        tf.import_graph_def(self.graph_def, name='')  # ~ 2.7 sec

        # Runs a tensor flow session
        with tf.Session() as sess:
            # Get handles to input and output tensors
            ops = tf.get_default_graph().get_operations()
            #print('****ops****')
            #print(ops)
            all_tensor_names = {
                output.name for op in ops for output in op.outputs
            }
            #print('****all_tensor_names****')
            #print(all_tensor_names)
            tensor_dict = {}
            for key in ['num_detections',
                        'detection_boxes',
                        'detection_scores',
                        'detection_classes',
                        'detection_masks'
                        ]:
                tensor_name = key + ':0'
                if tensor_name in all_tensor_names:
                    #print('****tensor_name****')
                    #print(tensor_name)
                    tensor_dict[key] = (tf.get_default_graph()
                                        .get_tensor_by_name(tensor_name)
                                        )
            if 'detection_masks' in tensor_dict:
                # The following processing is only for single image
                detection_boxes = tf.squeeze(
                    tensor_dict['detection_boxes'], [0]
                )
                detection_masks = tf.squeeze(
                    tensor_dict['detection_masks'], [0]
                )
                # Reframe is required to translate mask from box coordinates to image coordinates and fit the image size.
                real_num_detection = tf.cast(
                    tensor_dict['num_detections'][0], tf.int32
                )
                detection_boxes = tf.slice(
                    detection_boxes,
                    [0, 0],
                    [real_num_detection, -1]
                )
                detection_masks = tf.slice(
                    detection_masks,
                    [0, 0, 0],
                    [real_num_detection, -1, -1]
                )
                detection_masks_reframed = utils_ops.reframe_box_masks_to_image_masks(
                    detection_masks,
                    detection_boxes,
                    image.shape[0],
                    image.shape[1]
                )
                detection_masks_reframed = tf.cast(
                    tf.greater(detection_masks_reframed, 0.5),
                    tf.uint8
                )
                # Follow the convention by adding back the batch dimension
                tensor_dict['detection_masks'] = tf.expand_dims(
                    detection_masks_reframed,
                    0
                )

            image_tensor = (tf.get_default_graph()
                            .get_tensor_by_name('image_tensor:0')
                            )
            #print('****image_tensor****')
            #print(image_tensor)

            #print('****tensor_dict****')
            #print(tensor_dict)

            # Run inference
            output = sess.run(
                tensor_dict,
                feed_dict={image_tensor: np.expand_dims(image, 0)}
            )  # ~4.7 sec

            # all outputs are float32 numpy arrays, so convert types as appropriate
            output['num_detections'] = int(output['num_detections'][0])
            output['detection_classes'] = output['detection_classes'][0].astype(
                np.uint8)
            output['detection_boxes'] = output['detection_boxes'][0]
            output['detection_scores'] = output['detection_scores'][0]
        # END tf.session

        #print('****output****')
        #print(output)
        # Prevent Memory Leaking
        tf.reset_default_graph()

        # Prepare object for sending to endpoint
        result = {'timestamp': event['timestamp'],
                  'filename': event['filename'],
                  'objects': self.format_object_desc(output),
                  'image': self.get_annotated_image_as_text(image_np, output)
                  }
        #result = {'ts': event['timestamp']
        #          }
        return json.dumps(result)

    def handler(self, timestamp, message):
        """Collect messages, detect object and send to kafka endpoint."""
        records = message.collect()
        #print('******Records******')
        #print(records)
        # For performance reasons, we only want to process the newest message
        # for every camera_id
        to_process = {}
        self.logger.info( '\033[3' + str(randint(1, 7)) + ';1m' +  # Color
                          '-' * 25 +
                          '[ NEW MESSAGES: ' + str(len(records)) + ' ]'
                          + '-' * 25 +
                          '\033[0m' # End color
                          )
        dt_now = dt.datetime.now()
        for record in records:
            event = json.loads(record[1])
            self.logger.info('Received Message: ' +
                             event['filename'] + ' - ' + event['timestamp'])
            dt_event = dt.datetime.strptime(
                event['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
            delta = dt_now - dt_event
            if delta.seconds > 5:
                continue
            to_process[event['filename']] = event

        if len(to_process) == 0:
            self.logger.info('Skipping processing...')

        for key, event in to_process.items():
            self.logger.info('Processing Message: ' +
                             event['filename'] + ' - ' + event['timestamp'])
            start = timer()
            detection_result = self.detect_objects(event)
            end = timer()
            delta = end - start
            self.logger.info('Done after ' + str(delta) + ' seconds.')
            print('******Detection result******')
            print(detection_result)
            print("Type of detection_result")
            print(type(detection_result))
            self.producer.send(self.topic_for_produce, detection_result, key=b'sftp-recognized-images-from-files')
            self.producer.flush()


if __name__ == '__main__':
    sod = SparkObjectDetector(
        interval=3,
        model_file='C:\\Users\\singhgo\\Documents\\work\\dev\\ssd_mobilenet_v1_coco_2017_11_17\\'
                   'frozen_inference_graph.pb',
        labels_file='C:\\Users\\singhgo\\venv3.6\\Lib\\site-packages\\models\\research\\object_detection\\data\\'
                    'mscoco_label_map.pbtxt',
        number_classes=90,
        detect_treshold=.1,  # .5 This is default treshold for annotations in image
        topic_to_consume='sftp-topic',
        topic_for_produce='sftp-object-detection-topic',
        kafka_endpoint='localhost:29092')
    sod.start_processing()
