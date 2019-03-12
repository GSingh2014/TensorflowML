import json
import pysftp
import cv2
import base64
import datetime as dt
from kafka import KafkaProducer
import logging
from time import sleep

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.springml:spark-sftp_2.11:1.1.3,' \
#                                    'org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 pyspark-shell'


logging.basicConfig(format='%(levelname)s - %(asctime)s: %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

sftp_host = "localhost"
sftp_port = 2233
sftp_user = "gsingh"
sftp_pass = "tc@5f^p"
sftp_working_dir = "upload/video_images/"
pem_file = 'C:\\Users\\singhgo\\.ssh\\id_rsa'
kafka_brokers = "localhost:29092"
kafka_topic = "sftp-topic-video-images"


class SftpVideoKafka:
    """
        Ensure that Kafka Producer is able to accept large size of the message. Thus, add the following config to producer
        max_request_size = 104857600
        This is in bytes
        (optional) - buffer_memory=104857600
        Also, add a similar property to Kafka container running in Docker for the topic to which message is being sent
        docker run --net=confluent_default --rm confluentinc/cp-kafka:5.0.0 kafka-configs --zookeeper zookeeper:2181
        --entity-type topics --entity-name sftp-topic --alter --add-config max.message.bytes=104857600
    """
    def __init__(self, sftphost, sftpport, sftpuser, sftppass, sftpworkingdir, kafka_brokers, kafka_topic):
        self.SFTPHOST = sftphost
        self.SFTPPORT = sftpport
        self.SFTPUSER = sftpuser
        self.SFTPPASS = sftppass
        self.SFTPWORKINGDIR = sftpworkingdir
        self.SFTPLISTOFFILES = []
        self.kafka_brokers = kafka_brokers
        self.kafka_topic = kafka_topic
        self.img_file = './input.jpg'
        # Connection to Kafka Enpoint
        try:
            self.producer = KafkaProducer(bootstrap_servers=self.kafka_brokers,
                                          value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                                          max_request_size=104857600)
        except Exception as e:
            logger.error(e)

    def get_sftp_list_of_files(self, sftp):
        sftp.cwd(self.SFTPWORKINGDIR)

        directory_structure = sftp.listdir_attr()

        for attr in directory_structure:
            self.SFTPLISTOFFILES.append(attr.filename)

        return self.SFTPLISTOFFILES

    def connect_to_kafka(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        with pysftp.Connection(host=self.SFTPHOST, port=self.SFTPPORT, username=self.SFTPUSER, password=self.SFTPPASS,
                               cnopts=cnopts) as sftp:
            print('Connection established')
            print('********')
            print(sftp.listdir(self.SFTPWORKINGDIR))
            print('********')

            sftp_file_list = self.get_sftp_list_of_files(sftp)

            for file in sftp_file_list:
                print('---------------')
                print(file)
                print('---------------')

                doc_file_ext = str(file).split('.')[1]
                print(doc_file_ext)
                doc_file = file if (doc_file_ext in ['mp4', 'gif', 'jpg', 'jpeg']) else ''
                print(doc_file)

                if file != 'archive' and doc_file != '':
                    sftp.get(file, "../ui/DataLake_Search/static/" + file)
                    vidcap = cv2.VideoCapture("../ui/DataLake_Search/static/" + file)
                    success, image = vidcap.read()
                    if success is True:
                        jpg = cv2.imencode('.jpg', image)[1]
                        #print(jpg)
                        jpg_as_text = base64.b64encode(jpg).decode('utf-8')

                        timestamp = dt.datetime.now().isoformat()

                        result = {
                            'image': jpg_as_text,
                            'filename': file,
                            'timestamp': timestamp
                        }
                        print(result)
                        self.send_to_kafka(result)
                        # self.save_image(file, image)
                        self.delete_file_from_sftp(sftp, folder="/upload/video_images/", filename=file)
                        vidcap.release()
                        sleep(10.0)
                        """
                        Archive the file to upload/archive/ folder
                        """
                        # sftp.rename(file, 'archive/' + file)
                    else:
                        print('Could not read image from source')

    def send_to_kafka(self, data):
        """Send JSON payload to topic in Kafka."""
        try:
            self.producer.send(self.kafka_topic, value=data, key=b'sftp-files')
            self.producer.flush()
            logger.info('Sent image to Kafka endpoint.')
        except Exception as e:
            logger.error('Could not send the file to Kafka endpoint')
            logger.error(str(e))

    @staticmethod
    def delete_file_from_sftp(sftp, folder, filename):
        sftp.remove(folder + filename)

    def save_image(self, filename, image):
        """Save frame as JPEG file, for debugging purpose only."""
        cv2.imwrite("ui/DataLake_Search/static/" + filename, image)
        logger.info(f'Saved image file to {self.img_file}.')


obj_sftp_video_kafka = SftpVideoKafka(sftp_host, sftp_port, sftp_user, sftp_pass, sftp_working_dir, kafka_brokers,
                                      kafka_topic)
obj_sftp_video_kafka.connect_to_kafka()

