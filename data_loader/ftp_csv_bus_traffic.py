import json
import pysftp
import logging
from pyspark.sql import SparkSession
import requests
import os
from pyspark.sql.types import *
import pyspark.sql.functions as sp_fn
from json import JSONEncoder
import datetime

from itertools import chain

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-avro_2.11:4.0.0 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages ' \
                                    'org.elasticsearch:elasticsearch-hadoop:6.4.3 pyspark-shell'


class MyEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


logging.basicConfig(format='%(levelname)s - %(asctime)s: %(message)s',
                    level=logging.WARN)
logger = logging.getLogger(__name__)

sftp_host = "localhost"
sftp_port = 2233
sftp_user = "gsingh"
sftp_pass = "tc@5f^p"
sftp_working_dir = "upload/bus_traffic/"
pem_file = 'C:\\Users\\singhgo\\.ssh\\id_rsa'
kafka_brokers = "localhost:29092"
kafka_topic = "bus-topic"


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
        spark = SparkSession.builder \
            .master("local") \
            .appName("Detecting Streaming images and Videos") \
            .getOrCreate()

        self.spark = spark
        self.schema_registry_url = 'http://localhost:8081'
        self.es_username = 'elastic'
        self.es_host = '127.0.0.1'
        self.es_port = '9200'
        self.es_data_source = 'org.elasticsearch.spark.sql'
        self.es_checkpoint_location = 'src/main/resources/checkpoint-sftp-object-detection-topic-elasticsearch1'
        self.es_index = 'sftp-bus-files-' + datetime.datetime.today().strftime('%Y-%m-%d')
        self.es_doc_type = '/files'

    def getschema(self, schema_registry_url):
        response = requests.get(schema_registry_url)
        return response.json()

    def get_sftp_list_of_files(self, sftp):
        sftp.cwd(self.SFTPWORKINGDIR)

        directory_structure = sftp.listdir_attr()

        for attr in directory_structure:
            self.SFTPLISTOFFILES.append(attr.filename)

        return self.SFTPLISTOFFILES

    # def toJSON(self, obj):
    #     return obj.__dict__
    #     #return json.dumps(obj, default=lambda o: o.__dict__,
    #     #                  sort_keys=True, indent=4)
    @staticmethod
    def concat(type):
        def concat_(*args):
            returnlist = []
            for arg in args:
                returnlist.append(str(arg))
            #return list(chain(*args))
            #print(returnlist)
            return returnlist
        return sp_fn.udf(concat_, ArrayType(type))

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
                doc_file = file if (doc_file_ext in ['csv']) else ''
                print(doc_file)
                schema_response = self.getschema('http://localhost:8081/subjects/bus-topic/versions/1')
                # print(schema_response)
                # print(type(schema_response['schema']))

                # value_schema = avro.loads(schema_response['schema'])

                # print(value_schema)
                # print(type(value_schema))

                map_datatypes = {'int': 'integer', 'string': 'string', 'long': 'long', 'null': 'nullable'}

                #{'string': <class 'pyspark.sql.types.StringType'>, 'binary': <class 'pyspark.sql.types.BinaryType'>, 'boolean': <class 'pyspark.sql.types.BooleanType'>, 'decimal': <class 'pyspark.sql.types.DecimalType'>,
                # 'float': <class 'pyspark.sql.types.FloatType'>, 'double': <class 'pyspark.sql.types.DoubleType'>, 'byte': <class 'pyspark.sql.types.ByteType'>, 'short': <class 'pyspark.sql.types.ShortType'>,
                # 'integer': <class 'pyspark.sql.types.IntegerType'>, 'long': <class 'pyspark.sql.types.LongType'>, 'date': <class 'pyspark.sql.types.DateType'>, 'timestamp': <class 'pyspark.sql.types.TimestampType'>,
                # 'null': <class 'pyspark.sql.types.NullType'>}

                str_schema = schema_response['schema']
                # print(str_schema)

                for k, v in map_datatypes.items():
                    str_schema = str(str_schema).replace(k, v)

                json_schema = json.loads(str_schema)

                for field in json_schema['fields']:
                    field['nullable'] = True
                    field['metadata'] = {}

                print(json_schema)

                # schema = avro.schema.Parse(json.dumps(json_schema0))
                # print(schema)
                # print(type(schema))
                # print("***json schema***")
                # print(MyEncoder().encode(schema))
                # json_schema = self.toJSON(schema)
                # print(json_schema['_props'])
                # print(type(json_schema))

                struct_schema = StructType.fromJson(json_schema)
                print(struct_schema)

                if doc_file != '':
                    sftp.get(file, "../ui/DataLake_Search/static/" + file)
                    if doc_file_ext == 'csv':
                        data = self.spark.read.format('csv').schema(struct_schema).load("../ui/DataLake_Search/static/" + file)
                        concat_string_arrays = self.concat(StringType())

                        data_str_df = data.select([sp_fn.col(c).cast("string") for c in data.columns])

                        cols = [c for c in data_str_df.columns]

                        print(cols)
                        data_str_df.printSchema()

                        data_metadata = data_str_df \
                                            .withColumn("schema", sp_fn.lit(json.dumps(json_schema))) \
                                            .withColumn("timestamp", sp_fn.lit(datetime.datetime.today().strftime('%Y-%m-%d'))) \
                                            .withColumn("topics", sp_fn.lit('Documents')) \
                                            .withColumn("filename", sp_fn.lit(file)) \
                                            .withColumn("actualtext", concat_string_arrays(*cols))

                        data_metadata.printSchema()

                        data_metadata.show(10)

                        data_metadata \
                            .write \
                            .format("org.elasticsearch.spark.sql") \
                            .option("es.nodes", self.es_host) \
                            .option("es.port", self.es_port) \
                            .option("es.nodes.wan.only", "true") \
                            .option("es.nodes.discovery", "false") \
                            .option("es.nodes.data.only", "false") \
                            .option("es.index.auto.create", "true") \
                            .option("es.write.operation", "index") \
                            .option("es.resource", self.es_index + self.es_doc_type) \
                            .save()

                        #self.delete_file_from_sftp(sftp, folder="/upload/documents/", filename=file)
                else:
                    print('This is not a csv file')

    @staticmethod
    def delete_file_from_sftp(sftp, folder, filename):
        sftp.remove(folder + filename)


obj_sftp_video_kafka = SftpDocumentKafka(sftp_host, sftp_port, sftp_user, sftp_pass, sftp_working_dir, kafka_brokers,
                                         kafka_topic)
obj_sftp_video_kafka.connect_to_kafka()