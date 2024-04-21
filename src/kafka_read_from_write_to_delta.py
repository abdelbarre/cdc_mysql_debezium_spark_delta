from pyspark.sql import SparkSession
import pyspark
from delta import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *


scala_version = '2.12'  # TODO: Ensure this is correct
spark_version = '3.5.0'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.0', 'io.delta:delta-spark_2.12:3.1.0'
]
spark = SparkSession.builder\
   .master("local")\
   .appName("kafka-example")\
   .config("spark.jars.packages", ",".join(packages))\
   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
   .getOrCreate()
spark

# Create Hello word df to be send after to the Kafka topic 
data = ['Hello', 'World']
df = spark.createDataFrame([{'value': v} for v in data])

## Write to Kafka topic 
topic_name = 'dbserver1.cdc.demo'
df.write.format("kafka")\
  .option("kafka.bootstrap.servers", "kafka:9092")\
  .option("topic", topic_name)\
  .save()

## Read from Kafka topic 
# Define Kafka source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver1.cdc.demo") \
    .option("startingOffsets", "earliest") \
    .load()

# Decode the key and value columns from bytes to strings
df = df.withColumn("key", col("key").cast("string"))
df = df.withColumn("value", col("value").cast("string"))

# Start processing the stream and write to Delta format

## Save into Delta format 
query = (df.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", "./data/_checkpoints/")
   .start("./data/events")
)
query.awaitTermination()