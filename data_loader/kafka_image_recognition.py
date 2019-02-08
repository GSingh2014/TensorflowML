import os
import json
import numpy as np
from io import StringIO, BytesIO
from timeit import default_timer as timer
from PIL import Image
import datetime as dt
from random import randint

# Streaming imports
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark.sql.functions import *
from kafka import KafkaProducer

# # Object detection imports
import tensorflow as tf
from object_detection import protos
from object_detection.utils import ops as utils_ops
from object_detection.utils import label_map_util
from object_detection.utils import visualization_utils as vis_util

"""
Clone the repo (https://github.com/tensorflow/models.git) at path of the virtual environment of python 3.7 i.e.
    C:\\Users\\singhgo\\venv\\Scripts
to be able to use object_detection.utils
"""

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 pyspark-shell'


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
                 detect_threshold=.5,
                 topic_to_consume='sftp-topic',
                 topic_for_produce='sftp-augmented-topic',
                 kafka_endpoint='localhost:29092'):
        """Initialize Spark & TensorFlow environment."""
        self.topic_to_consume = topic_to_consume
        self.topic_for_produce = topic_for_produce
        self.kafka_endpoint = kafka_endpoint
        self.threshold = detect_threshold
        self.v_sectors = ['top', 'middle', 'bottom']
        self.h_sectors = ['left', 'center', 'right']

        # Create Kafka Producer for sending results
        self.producer = KafkaProducer(bootstrap_servers=kafka_endpoint)

        # Load Labels & Categories
        label_map = label_map_util.load_labelmap(labels_file)
        categories = label_map_util.convert_label_map_to_categories(label_map,
                                                                    max_num_classes=number_classes,
                                                                    use_display_name=True
                                                                    )
        self.category_index = label_map_util.create_category_index(categories)

        #print(self.category_index)

        # Load Spark Context
        spark = SparkSession.builder \
            .master("local") \
            .appName("Detecting Streaming images and Videos") \
            .getOrCreate()

        self.spark = spark
        self.schema = StructType().add("image", StringType()) \
            .add("filename", StringType()) \
            .add("timestamp", StringType())

        # Make Spark logging less extensive
        log4jLogger = spark.sparkContext._jvm.org.apache.log4j
        log_level = log4jLogger.Level.ERROR
        log4jLogger.LogManager.getLogger('org').setLevel(log_level)
        log4jLogger.LogManager.getLogger('akka').setLevel(log_level)
        log4jLogger.LogManager.getLogger('kafka').setLevel(log_level)
        self.logger = log4jLogger.LogManager.getLogger(__name__)

        # Load Frozen Network Model & Broadcast to Worker Nodes
        with tf.gfile.FastGFile(model_file, 'rb') as f:
            model_data = f.read()
            #print(model_data)
        self.model_data_bc = spark.sparkContext.broadcast(model_data)
        #self.model_data_bc = model_data

        # # Load Graph Definition
        # self.graph_def = tf.GraphDef()
        # self.graph_def.ParseFromString(self.model_data_bc.value)

    @staticmethod
    def load_image_into_numpy_array(image):
        """Convert PIL image to numpy array."""
        (im_width, im_height) = image.size
        return np.array(image.getdata()).reshape(
            (im_height, im_width, 3)).astype(np.uint8)

    @staticmethod
    def detect_objects(model_data_bc, input_image, filename, timestamp ):
        """Use TensorFlow Model to detect objects."""
        """ if self, is added to the parameter list to this function , then the following error occurs
         TypeError: detect_objects() missing 1 required positional argument: 'input_image' """

        import base64

        # Load the image data from the json into PIL image & numpy array
        print("***Inside***")
        #print(input_image)

        decoded = base64.b64decode(input_image)
        # print(decoded)
        stream = BytesIO(decoded)
        #print(stream)
        image = Image.open(stream)
        #print(image)
        image_np = SparkObjectDetector.load_image_into_numpy_array(image)
        #print(image_np)
        stream.close()

        graph_def = tf.GraphDef()
        graph_def.ParseFromString(model_data_bc)

        # Load Network Graph
        try:
            tf.import_graph_def(graph_def, name='')  # ~ 2.7 sec
            print("Imported graph def")
        except Exception as e:
            print(e.args)

        # Runs a tensor flow session
        # with tf.Session() as sess:
        #     # Get handles to input and output tensors
        #     ops = tf.get_default_graph().get_operations()
        #     all_tensor_names = {
        #         output.name for op in ops for output in op.outputs
        #     }
        #     tensor_dict = {}
        #     for key in ['num_detections',
        #                 'detection_boxes',
        #                 'detection_scores',
        #                 'detection_classes',
        #                 'detection_masks'
        #                 ]:
        #         tensor_name = key + ':0'
        #         if tensor_name in all_tensor_names:
        #             tensor_dict[key] = (tf.get_default_graph()
        #                                 .get_tensor_by_name(tensor_name)
        #                                 )
        # #     if 'detection_masks' in tensor_dict:
        #         # The following processing is only for single image
        #         detection_boxes = tf.squeeze(
        #             tensor_dict['detection_boxes'], [0]
        #         )
        #         detection_masks = tf.squeeze(
        #             tensor_dict['detection_masks'], [0]
        #         )
        #         # Reframe is required to translate mask from box coordinates to image coordinates and fit the image size.
        #         real_num_detection = tf.cast(
        #             tensor_dict['num_detections'][0], tf.int32
        #         )
        #         detection_boxes = tf.slice(
        #             detection_boxes,
        #             [0, 0],
        #             [real_num_detection, -1]
        #         )
        #         detection_masks = tf.slice(
        #             detection_masks,
        #             [0, 0, 0],
        #             [real_num_detection, -1, -1]
        #         )
        #         detection_masks_reframed = utils_ops.reframe_box_masks_to_image_masks(
        #             detection_masks,
        #             detection_boxes,
        #             image.shape[0],
        #             image.shape[1]
        #         )
        #         detection_masks_reframed = tf.cast(
        #             tf.greater(detection_masks_reframed, 0.5),
        #             tf.uint8
        #         )
        #         # Follow the convention by adding back the batch dimension
        #         tensor_dict['detection_masks'] = tf.expand_dims(
        #             detection_masks_reframed,
        #             0
        #         )
        #
        #     image_tensor = (tf.get_default_graph()
        #                     .get_tensor_by_name('image_tensor:0')
        #                     )
        #
        #     # Run inference
        #     output = sess.run(
        #         tensor_dict,
        #         feed_dict={image_tensor: np.expand_dims(image, 0)}
        #     )  # ~4.7 sec
        #
        #     # all outputs are float32 numpy arrays, so convert types as appropriate
        #     output['num_detections'] = int(output['num_detections'][0])
        #     output['detection_classes'] = output['detection_classes'][0].astype(
        #         np.uint8)
        #     output['detection_boxes'] = output['detection_boxes'][0]
        #     output['detection_scores'] = output['detection_scores'][0]
        # # END tf.session
        #
        # # Prevent Memory Leaking
        # tf.reset_default_graph()
        #
        # # Prepare object for sending to endpoint
        # result = {'timestamp': timestamp,
        #           'filename': filename,
        #           'objects': SparkObjectDetector.format_object_desc(output),
        #           'image': SparkObjectDetector.get_annotated_image_as_text(image_np, output)
        #           }
        # return json.dumps(result)
        return "done"

    def start_processing(self):
        """Start consuming from Kafka endpoint and detect objects."""

        sftp_df = self.spark.readStream.format('kafka') \
            .option("kafka.bootstrap.servers", self.kafka_endpoint) \
            .option("subscribe", self.topic_to_consume) \
            .option("startingOffsets", "latest") \
            .option("kafka.max.partition.fetch.bytes", "104857600") \
            .load()

        exploded_json_df = sftp_df.withColumn("json_values", from_json(col("value").cast("string"), self.schema)) \
                                  .select("json_values.*")

        exploded_json_df.printSchema()
        #image_to_number = udf(self.image_to_number, ArrayType(IntegerType()))

        #udf_image_to_number = udf(SparkObjectDetector.image_to_number, ArrayType(IntegerType()))

        udf_detect_objects = udf(lambda model_data_bc: SparkObjectDetector.detect_objects(model_data_bc), StringType())

        print(type(self.model_data_bc.value))

        newdf = exploded_json_df.withColumn('image_to_numbers', udf_detect_objects(lit(self.model_data_bc.value), col('image'), col('filename'),
                                                                                   col('timestamp')))

        newdf.printSchema()

        #.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        console_query = newdf \
            .writeStream \
            .format("console") \
            .start()

        console_query.awaitTermination()


        # kvs = KafkaUtils.createDirectStream(self.ssc,
        #                                     [self.topic_to_consume],
        #                                     {'metadata.broker.list': self.kafka_endpoint}
        #                                     )
        # kvs.foreachRDD(self.handler)
        # self.ssc.start()
        # self.ssc.awaitTermination()

    def box_to_sector(self, box):
        """Transform object box in image into sector description."""
        width = box[3] - box[1]
        h_pos = box[1] + width / 2.0
        height = box[2] - box[0]
        v_pos = box[0] + height / 2.0
        h_sector = min(int(h_pos * 3), 2)  # 0: left, 1: center, 2: right
        v_sector = min(int(v_pos * 3), 2)  # 0: top, 1: middle, 2: bottom
        return self.v_sectors[v_sector], self.h_sectors[h_sector]

    def get_annotated_image_as_text(self, image_np, output):
        """Paint the annotations on the image and serialize it into text."""
        import base64
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
        text_stream = StringIO()
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
            if score > self.threshold:  # Only keep objects over threshold
                # Get label for category id
                cat_id = output['detection_classes'][i]
                label = self.category_index[cat_id]['name']

                # Get position of object box as [ymin, xmin, ymax, xmax]
                box = output['detection_boxes'][i]

                objs.append({
                    'label': label,
                    'score': score,
                    'sector': self.box_to_sector(box)
                })
        return objs


if __name__ == '__main__':
    sod = SparkObjectDetector(
        interval=3,
        model_file='C:\\Users\\singhgo\\Documents\\work\\dev\\ssd_mobilenet_v1_coco_2017_11_17\\frozen_inference_graph.pb',
        labels_file='C:\\Users\\singhgo\\venv3.6\\Lib\\site-packages\\models\\research\\object_detection\\data\\mscoco_label_map.pbtxt',
        number_classes=90,
        detect_threshold=.5
        )
sod.start_processing()

