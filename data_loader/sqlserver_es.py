from pyspark.sql.session import SparkSession
import pyspark.sql.functions as sp_fn
from pyspark.sql.types import ArrayType, StringType
from elasticsearch import Elasticsearch
import datetime
import json
import requests
import os


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages ' \
                                    'org.elasticsearch:elasticsearch-hadoop:6.4.3 pyspark-shell'


def concat(dtype):
    def concat_(*args):
        returnlist = []
        for arg in args:
            returnlist.append(str(arg))
        #return list(chain(*args))
        #print(returnlist)
        return returnlist
    return sp_fn.udf(concat_, ArrayType(dtype))

# @staticmethod
# def getschema(schema_registry_url):
#     response = requests.get(schema_registry_url)
#     return response.json()


spark = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath",
            "C:\\Users\\singhgo\\Documents\\work\\BI Analytics\\POC\\sql-server\\mssql-jdbc-6.2.2.jre8.jar") \
    .appName("Python Spark SQL data source example") \
    .getOrCreate()

query = """
(select TOP (10) t.*,
st.arrival_time,st.departure_time,st.stop_id,st.stop_sequence,st.pickup_type,st.drop_off_type,
rt.route_short_name,rt.route_long_name,rt.route_desc,rt.route_type,rt.route_url,
c.monday,c.tuesday, c.wednesday, c.thursday,c.friday,c.saturday,c.sunday,c.[start_date],c.end_date,
cd.[date],cd.exception_type,
s.stop_code,s.stop_name,s.stop_desc,s.stop_lat,s.stop_lon,s.zone_id,s.stop_url,s.location_type
from dbo.trips t join [dbo].[stop_times] st
on t.trip_id = st.trip_id
join [dbo].[routes] rt
on t.route_id = rt.route_id
join [dbo].[calendar] c
on t.service_id = c.service_id
join [dbo].[calendar_dates] cd
on c.service_id = cd.service_id
join [dbo].[stops] s
on st.stop_id = s.stop_id) octranspo
"""

# jdbcDF = spark.read.format("jdbc") \
#     .option("url", "jdbc:sqlserver://localhost:1433;databaseName=octranspoDB") \
#     .option("dbtable", "TABLE_NAME") \
#     .option("user", "DB_USER") \
#     .option("password", "DB_PASSWORD")\
#     .load()

octranspo_df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlserver://localhost:1433;databaseName=octranspoDB") \
    .option("dbtable", query) \
    .option("user", "sa") \
    .option("password", "P@ssw0rd") \
    .load()

#octranspo_df.printSchema()
#octranspo_df.show(10)


# schema_response = getschema('http://localhost:8081/subjects/bus-topic/versions/1')
#
# map_datatypes = {'int': 'integer', 'string': 'string', 'long': 'long', 'null': 'nullable'}
#
# str_schema = schema_response['schema']
# # print(str_schema)
#
# for k, v in map_datatypes.items():
#     str_schema = str(str_schema).replace(k, v)
#
# json_schema = json.loads(str_schema)

json_schema = octranspo_df.schema.json()

print(json_schema)

concat_string_arrays = concat(StringType())

cols = [c for c in octranspo_df.columns]

es_df = octranspo_df.withColumn("schema", sp_fn.lit(json_schema)) \
                    .withColumn("timestamp", sp_fn.lit(datetime.datetime.today().strftime('%Y-%m-%d'))) \
                    .withColumn("topics", sp_fn.lit("Databases")) \
                    .withColumn("filename", sp_fn.lit("OCTranspo Bus Database in SQL Server")) \
                    .withColumn("actualtext", concat_string_arrays(*cols))


#es_df.printSchema()
es_df.show(10, False)

# es_df \
#     .write \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.nodes", '127.0.0.1') \
#     .option("es.port", '9200') \
#     .option("es.nodes.wan.only", "true") \
#     .option("es.nodes.discovery", "false") \
#     .option("es.nodes.data.only", "false") \
#     .option("es.index.auto.create", "true") \
#     .option("es.write.operation", "index") \
#     .option("es.resource", "databases-sqlserver-octranspo-" + datetime.datetime.today().strftime('%Y-%m-%d') + "/records") \
#     .save()
