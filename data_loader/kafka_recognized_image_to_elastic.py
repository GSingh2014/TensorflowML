import os

# Streaming imports
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
import pandas as pd
import datetime

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2,' \
                                    'org.elasticsearch:elasticsearch-hadoop:6.4.3 pyspark-shell'

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.springml:spark-sftp_2.11:1.1.3,' \
#                                    'org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 pyspark-shell'


class KafkaRecognizedImageToElastic:
    def __init__(self, topic_to_consume='sftp-object-detection-topic',
                 kafka_endpoint='localhost:29092',
                 es_username='elastic',
                 es_host='127.0.0.1',
                 es_port='9200',
                 es_data_source='org.elasticsearch.spark.sql',
                 es_checkpoint_location='src/main/resources/checkpoint-sftp-object-detection-topic-elasticsearch2',
                 es_index='sftp-files-' + datetime.datetime.today().strftime('%Y-%m-%d'),
                 es_doc_type='/files'):
        self.topic_to_consume = topic_to_consume
        self.kafka_endpoint = kafka_endpoint
        self.schema = StructType() \
            .add("timestamp", StringType()) \
            .add("filename", StringType()) \
            .add("objects", ArrayType(StructType()
                                      .add("label", StringType())
                                      .add("score", StringType())
                                      .add("sector", ArrayType(StringType())))) \
            .add("image", StringType()) \
            .add('topics', StringType())

        # self.schema = StructType() \
        #     .add("ts", StringType()) \
        #     .add("filename", StringType())

        #StructType([StructField("label", StringType()),StructField("score", StringType()),StructField("sector", ArrayType(StringType()))])

        self.result = []

        self.es_username = es_username
        self.es_host = es_host
        self.es_port = es_port
        self.es_data_source=es_data_source
        self.es_checkpoint_location=es_checkpoint_location
        self.es_index=es_index
        self.es_doc_type=es_doc_type

        spark = SparkSession.builder \
            .master("local") \
            .appName("Detecting Streaming images and Videos") \
            .getOrCreate()

        self.spark = spark

    @staticmethod
    def get_unique_objects_with_max_score(objects):
        pd_df = pd.DataFrame(objects, columns=["label", "score", "sector"])
        max_score_df = pd_df.groupby('label').apply(lambda x: x.loc[x.score == x.score.max(), ['label', 'score', 'sector']])

        #print(max_score_df)

        non_dup_max_score_df = max_score_df.loc[max_score_df.astype(str).drop_duplicates().index]

        #print(non_dup_max_score_df)

        # @pandas_udf(self.schema, functionType=PandasUDFType.GROUPED_MAP)
        # def g(df):
        #     result = pd.DataFrame(df.groupby(df.label).apply(
        #                 lambda x: x.loc["score"].max(axis=1)
        #                 )
        #                           )
        #     result.reset_index(inplace=True, drop=False)
        #     self.result = result
        #     print("***result****")
        #     print(self.result)

        return non_dup_max_score_df.to_dict(orient='records')

    @staticmethod
    def replace_backslash(jsonstring):
        print(jsonstring.replace('\\', '').replace('"', '\''))
        return jsonstring.replace('\\', '')

    def start_processing(self):
        sftp_recognized_image_df = self.spark.readStream.format('kafka') \
            .option("kafka.bootstrap.servers", self.kafka_endpoint) \
            .option("subscribe", self.topic_to_consume) \
            .option("startingOffsets", "latest") \
            .option("kafka.max.partition.fetch.bytes", "204857600") \
            .load()

        udf_replace_backslash = udf(self.replace_backslash, StringType())

        object_schema = StructType().add("label", StringType()) \
                                    .add("score", StringType()) \
                                    .add("sector", ArrayType(StringType()))

        #json_df = sftp_recognized_image_df.select(col("key").cast("string"), explode(udf_replace_backslash(col("value").cast("string"))).alias("jsoncol"))
        json_df = sftp_recognized_image_df.select(col("key").cast("string"),
                                                  from_json(sftp_recognized_image_df.value.cast("string"), self.schema).alias("jsoncol")
                                                  )

        #json_df.printSchema()

        parsed_json_df = json_df.select('key', "jsoncol.*")

        udf_get_unique_objects_with_max_score = udf(self.get_unique_objects_with_max_score, ArrayType(StringType()))

        exploded_df = parsed_json_df.withColumn("object", explode(col("objects"))) \
                                    #.withColumn("ts", col("timestamp").cast('timestamp'))

        #json_object_df = exploded_df.withColumn("jsonobject", to_json(col("object")))

        json_object_df = exploded_df.select("*", from_json(to_json(col("object")), object_schema)
                                            .alias("object_value")) \
                                            .drop("object") \
                                            .selectExpr("*", "object_value.*")

        json_object_df.printSchema()

        #This is the error when aggregating using dataframe 'Append output mode not supported when there are streaming
        # aggregations on streaming DataFrames/DataSets without watermark
        # Using .withWatermark("ts", '5 seconds') takes longer to process
        # unique_object_max_score_df = json_object_df.drop("objects", "object", "object_value") \
        #                                            .withWatermark("ts", '5 seconds') \
        #                                            .groupBy("label",
        #                                                     window("ts", "5 minutes", "1 minutes")) \
        #                                            .agg({"score": "max"})

        unique_object_max_score_df = parsed_json_df.withColumn("uniqueObjects",
                                                               udf_get_unique_objects_with_max_score(col("objects"))) \
                                                   .drop("objects")
                                                   #.withColumn("topics", lit("Video/Image"))

        # unique_object_max_score_df = exploded_df.withColumn('maxScore',
        #                                                     exploded_df.groupby('objects.label').
        #                                                     agg({'objects.score': 'max'}))

        # console_query = unique_object_max_score_df \
        #     .writeStream \
        #     .format("console") \
        #     .option('truncate', 'false') \
        #     .start()
        #
        # console_query.awaitTermination()

        es_query = unique_object_max_score_df \
            .writeStream \
            .format("org.elasticsearch.spark.sql") \
            .option("es.mapping.id", "filename") \
            .option("es.nodes", self.es_host) \
            .option("es.port", self.es_port) \
            .option("es.nodes.wan.only", "true") \
            .option("es.nodes.discovery", "false") \
            .option("es.nodes.data.only", "false") \
            .option("es.index.auto.create", "true") \
            .option("checkpointLocation", self.es_checkpoint_location) \
            .option("es.write.operation", "upsert") \
            .start(self.es_index + self.es_doc_type)

        es_query.awaitTermination()


if __name__ == '__main__':
    krie = KafkaRecognizedImageToElastic()

    krie.start_processing()