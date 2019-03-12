import PyPDF2
import json
import pysftp
import datetime as dt
from kafka import KafkaProducer
import logging
from time import sleep

logging.basicConfig(format='%(levelname)s - %(asctime)s: %(message)s',
                    level=logging.WARN)
logger = logging.getLogger(__name__)

sftp_host = "localhost"
sftp_port = 2233
sftp_user = "gsingh"
sftp_pass = "tc@5f^p"
sftp_working_dir = "upload/documents/"
pem_file = 'C:\\Users\\singhgo\\.ssh\\id_rsa'
kafka_brokers = "localhost:29092"
kafka_topic = "sftp-topic-documents"


class SftpDocumentKafka:
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
                doc_file = file if (doc_file_ext in ['pdf', 'txt']) else ''
                print(doc_file)
                if doc_file != '':
                    sftp.get(file, "../ui/DataLake_Search/static/" + file)
                    if doc_file_ext == 'pdf':
                        with open("../ui/DataLake_Search/static/" + file, 'rb') as fh:
                            pdfReader = PyPDF2.PdfFileReader(fh)
                            num_pages = pdfReader.numPages
                            count = 0
                            text = ""
                            timestamp = dt.datetime.now().isoformat()
                            print('**Num of pages***')
                            print(num_pages)

                            while count < num_pages:
                                pageObj = pdfReader.getPage(count)
                                print('Page type: {}'.format(str(type(pageObj))))
                                count +=1
                                text += pageObj.extractText()
                            print(text)
                            result = {
                                'image': text,
                                'filename': file,
                                'timestamp': timestamp
                            }
                            self.send_to_kafka(result)
                            self.delete_file_from_sftp(sftp, folder="/upload/documents/", filename=file)
                else:
                    print('This is not a pdf or txt file')

    @staticmethod
    def delete_file_from_sftp(sftp, folder, filename):
        sftp.remove(folder + filename)

    def send_to_kafka(self, data):
        """Send JSON payload to topic in Kafka."""
        try:
            self.producer.send(self.kafka_topic, value=data, key=b'sftp-files')
            self.producer.flush()
            logger.info('Sent image to Kafka endpoint.')
        except Exception as e:
            logger.error('Could not send the file to Kafka endpoint')
            logger.error(str(e))


obj_sftp_video_kafka = SftpDocumentKafka(sftp_host, sftp_port, sftp_user, sftp_pass, sftp_working_dir, kafka_brokers,
                                      kafka_topic)
obj_sftp_video_kafka.connect_to_kafka()