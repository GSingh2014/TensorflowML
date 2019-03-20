import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.ml.clustering import LDA, LDAModel
import re as re
from pyspark.ml.feature import CountVectorizer , IDF

import pandas as pd

from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import RegexpTokenizer
import gensim
import datetime

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2,' \
#                                    'org.elasticsearch:elasticsearch-hadoop:6.4.3 pyspark-shell'


class SftpDocumentClustering:
    def __init__(self,
                 topic_to_consume='sftp-topic-documents',
                 topic_for_produce='sftp-augmented-topic',
                 kafka_endpoint='localhost:29092',
                 es_username='elastic',
                 es_host='127.0.0.1',
                 es_port='9200',
                 es_data_source='org.elasticsearch.spark.sql',
                 es_checkpoint_location='src/main/resources/checkpoint-sftp-topic-documents-elasticsearch1',
                 es_index='sftp-files-' + datetime.datetime.today().strftime('%Y-%m-%d'),
                 es_doc_type='/files'):

        # Load Spark Context
        spark = SparkSession.builder \
            .master("local") \
            .appName("Detecting unstructured documents") \
            .getOrCreate()

        StopWords = stopwords.words("english")

        self.spark = spark
        self.topic_to_consume = topic_to_consume
        self.topic_for_produce = topic_for_produce
        self.kafka_endpoint = kafka_endpoint
        self.schema = StructType().add("image", StringType()) \
            .add("filename", StringType()) \
            .add("timestamp", StringType())
        self.es_username = es_username
        self.es_host = es_host
        self.es_port = es_port
        self.es_data_source=es_data_source
        self.es_checkpoint_location=es_checkpoint_location
        self.es_index=es_index
        self.es_doc_type=es_doc_type

        self.stopwords = StopWords

        print(self.stopwords)

        self.cv = CountVectorizer(inputCol="releventtext", outputCol="raw_features", vocabSize=5000, minDF=10.0)
        self.idf = IDF(inputCol="raw_features", outputCol="features")
        num_topics = 10
        max_iterations = 100
        self.lda = LDA(featuresCol="features", k=num_topics, maxIter=max_iterations)

        # Make Spark logging less extensive
        log4jLogger = spark.sparkContext._jvm.org.apache.log4j
        log_level = log4jLogger.Level.ERROR
        log4jLogger.LogManager.getLogger('org').setLevel(log_level)
        log4jLogger.LogManager.getLogger('akka').setLevel(log_level)
        log4jLogger.LogManager.getLogger('kafka').setLevel(log_level)
        self.logger = log4jLogger.LogManager.getLogger(__name__)

    @staticmethod
    def get_relevant_text(actualtext, stopwords_str):
        print('***actualtext****')
        print(actualtext)
        space_removed_text = str(actualtext).replace('  ', '')
        #print(space_removed_text)
        tokenizer = RegexpTokenizer('[a-zA-Z]\w+')
        tokenized_word_list = tokenizer.tokenize(space_removed_text)
        stopwordslist = str(stopwords_str).split(',')
        stopwordslist.extend(['from', 'subject', 're', 'edu', 'use'])
        relevent_word_list = [word for word in tokenized_word_list if (len(word) >= 3) & (word not in stopwordslist)]
        print('***relevent_word_list****')
        print(relevent_word_list)
        return relevent_word_list

    @staticmethod
    def get_topics_list(relevant_text):
        # relevant_text = cls.get_relevant_text(actualtext, stopwords_str)
        relevant_text_list = str(relevant_text).split(',')
        #stemmed_tokens = SftpDocumentClustering.get_stemmed_tokens(relevant_text_list)
        p_stemmer = PorterStemmer()
        stemmed_tokens = [p_stemmer.stem(i) for i in relevant_text_list]
        #print('***stemmed_tokens****')
        #print(stemmed_tokens)
        #print(type(stemmed_tokens))
        stemmed_tokens_list_of_list = [[i] for i in stemmed_tokens]
        #print(stemmed_tokens_list_of_list)
        dictionary = gensim.corpora.Dictionary(stemmed_tokens_list_of_list)
        # get bag-of-words as list of vectors.In each document vector is a series of tuples. E.g.,print(bow[0])
        # The tuples are (term ID, term frequency)
        bow = [dictionary.doc2bow(text) for text in stemmed_tokens_list_of_list]
        lda_model = gensim.models.ldamodel.LdaModel(bow, num_topics=4, id2word=dictionary, passes=50)
        topic_list = lda_model.print_topics(num_topics=4, num_words=3)
        print(topic_list)
        return topic_list

    @staticmethod
    def get_stemmed_tokens(relevant_word_list):
        #print('***INSIDE STEM TOKEN***')
        p_stemmer = PorterStemmer()
        texts = [p_stemmer.stem(i) for i in relevant_word_list]
        # print(texts)
        # print(type(texts))
        return texts

    @staticmethod
    def get_unique_objects_with_max_score(topics):
        #["label", "score", "sector"]
        pd_df = pd.DataFrame(topics, columns=["topicnumber", "topics"])
        pd_df = pd_df.drop("topicnumber", axis=1)
        pd_df = pd_df.rename(index=str, columns={'topics':'label'})
        return pd_df.to_dict(orient="records")

    def start_processing(self):
        sftp_df = self.spark.readStream.format('kafka') \
            .option("kafka.bootstrap.servers", self.kafka_endpoint) \
            .option("subscribe", self.topic_to_consume) \
            .option("startingOffsets", "latest") \
            .option("kafka.max.partition.fetch.bytes", "104857600") \
            .load()

        exploded_json_df = sftp_df.withColumn("json_values", from_json(col("value").cast("string"), self.schema)) \
            .select("json_values.*")
        # .replace('\n', '') .replace('[^a-zA-Z]','') .encode('ascii', 'ignore') ^a-zA-Z(\s\s+)(\r\n)
        actual_text_df = exploded_json_df.withColumn("actualtext", regexp_replace(col("image"), "[\r\n]", " "))

        actual_text_df.printSchema()

        udf_get_relevant_text = udf(SftpDocumentClustering.get_relevant_text, ArrayType(StringType()))

        relevant_text_df = actual_text_df.withColumn('releventtext', udf_get_relevant_text(regexp_replace(
                                                                        col("actualtext"), "[^a-zA-Z(\s)]", ""),
                                                    lit(','.join(self.stopwords))))

        udf_get_topics_list = udf(lambda relevant_text: SftpDocumentClustering.get_topics_list(relevant_text),
                                  ArrayType(StructType([
                                      StructField("topicnumber", IntegerType(), False),
                                      StructField("topics", StringType(), False)
                                  ])))

        join_udf = udf(lambda x: ",".join(x))
        topics_relevant_text_df = relevant_text_df.withColumn("topics", udf_get_topics_list(join_udf(col('releventtext'))))

        udf_get_unique_objects_with_max_score = udf(self.get_unique_objects_with_max_score, ArrayType(StringType()))

        es_sftp_files_df = topics_relevant_text_df.withColumn("uniqueObjects", udf_get_unique_objects_with_max_score(col("topics"))) \
                                                  .drop(col('topics')) \
                                                  .withColumn("topics", lit("Documents"))

        # console_query = topics_relevant_text_df \
        #     .writeStream \
        #     .format("console") \
        #     .option("truncate", "false") \
        #     .start()
        #
        # console_query.awaitTermination()

        #.outputMode("append") \
        es_query = es_sftp_files_df.writeStream \
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
    sdc = SftpDocumentClustering()
    sdc.start_processing()