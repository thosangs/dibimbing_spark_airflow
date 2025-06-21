"""
# Objectives
- Handle late-arriving events from unsynchronized devices or data sources.
- Create accurate real-time metrics despite out-of-order data by using a `watermark`.

# How to Run
1. Run the producer:
   `make run-practice-5-producer`
2. Run this consumer:
   `make run-practice-5-consumer`
3. Observe the output in the console. Notice how the watermark discards data that is too late,
   while windowing groups events based on their occurrence time, not their processing time.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os

# Get KAFKA_HOST from environment variables
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("WindowingWatermarkPractice")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schema for page event data
schema = StructType(
    [
        StructField("page_url", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_timestamp", LongType(), True),  # The time when the event occurred
    ]
)

# Read stream from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")
    .option("subscribe", "page_events")
    .option("startingOffsets", "latest")
    .load()
)

# Deserialize and transform data
json_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("event_timestamp").cast("timestamp"))
)

# Count page views every 1 minute with a 1-minute watermark
# `withWatermark("event_time", "1 minute")` means Spark will wait up to 1 minute
# for late data. Any data arriving more than 1 minute after the maximum time
# ever seen will be ignored.
page_view_counts = (
    json_df.withWatermark("event_time", "1 minute")
    .groupBy(window(col("event_time"), "1 minute"), col("page_url"))  # 1-minute tumbling window
    .agg(count("*").alias("view_count"))
)

# Display results to the console
query = page_view_counts.writeStream.outputMode("update").format("console").option("truncate", "false").start()

query.awaitTermination()
