from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
from pyspark.ml import Pipeline

from pyspark.ml.feature import VectorAssembler, VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .master("local") \
    .appName("Anomaly_Detection_Vehicle_Data") \
    .getOrCreate()


vehicle_raw_df = spark.read.csv('..\data_loader\data_dump.csv', header=True, inferSchema=True)

vehicle_raw_df.printSchema()

vehicle_raw_df_stats = vehicle_raw_df.describe("vehicle_speed", "engine_speed", "tire_pressure")

vehicle_raw_df_stats.show()

vecAssembler = VectorAssembler(inputCols=["vehicle_speed", "engine_speed", "tire_pressure"], outputCol="features")
vehicle_features_vector_df = vecAssembler.transform(vehicle_raw_df)

vehicle_features_vector_df.show()

kmeans = KMeans(k=2, seed=1)

model = kmeans.fit(vehicle_features_vector_df.select('features'))

print(model)

prediction_df = model.transform(vehicle_features_vector_df)

prediction_df.printSchema()

prediction_df = prediction_df.withColumnRenamed('prediction', 'label')

prediction_df.printSchema()

prediction_df.groupBy("label").agg(fn.count("label").alias("Num of records")).orderBy("label").show()


vehicle_features_vector_indexer = VectorIndexer(inputCol="features", outputCol="IndexedFeatures", maxCategories=3)\
    .fit(vehicle_features_vector_df)

rf = RandomForestRegressor(featuresCol="IndexedFeatures")

# Chain indexer and forest in a Pipeline
pipeline = Pipeline(stages=[vehicle_features_vector_indexer, rf])

(trainingData, testData) = prediction_df.randomSplit([0.7, 0.3])

# Train model.  This also runs the indexer.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

#Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="vehicle_id", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

rfModel = model.stages[1]
print(rfModel)  # summary only

