"""
# Objectives
- Understand the latency control differences between trigger types (processingTime, once, continuous).
- Use the appropriate output mode (append, complete, update) based on latency and idempotency needs.

# How to Run
1. Run the producer:
   In one terminal, run: `make run-practice-1-producer`
2. Run this consumer:
   In a new terminal, run: `make run-practice-1-consumer`
3. Observe the output in the console.
4. Experiment: Stop the consumer, change the `trigger` and `outputMode` below, then run it again to see the difference.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os

# Get KAFKA_HOST from environment variables, with 'localhost' as a fallback
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("TriggerAndOutputModePractice")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schema for the data from Kafka
schema = StructType(
    [
        StructField("ip", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("path", StringType(), True),
        StructField("method", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("timestamp", DoubleType(), True),
    ]
)

# Read a stream from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")
    .option("subscribe", "web_logs")
    .option("startingOffsets", "latest")
    .load()
)

# Deserialize the value from JSON
json_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_timestamp", col("timestamp").cast(TimestampType()))
)

# Simple transformation: filter for slow response logs
slow_responses_df = json_df.filter("response_time_ms > 1000")

# --- EXPERIMENT AREA ---
# Write the results to the console with various triggers and output modes.
# Only one `writeStream` can be active at a time. Comment out the others.

# 1. `processingTime` trigger with `append` mode
# Processes data in micro-batches every 10 seconds. `append` mode only shows new rows.
query = (
    slow_responses_df.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

# 2. `once` trigger with `append` mode
# Processes all available data in a single batch then stops.
# query = (slow_responses_df
#     .writeStream
#     .outputMode("append")
#     .format("console")
#     .option("truncate", "false")
#     .trigger(once=True)
#     .start())

# 3. `continuous` trigger with `append` mode (Requires more resources, low latency)
# Processes data with a fixed checkpoint interval (e.g., 1 second). Use with caution.
# query = (slow_responses_df
#     .writeStream
#     .outputMode("append")
#     .format("console")
#     .option("truncate", "false")
#     .trigger(continuous='1 second')
#     .start())


query.awaitTermination()
