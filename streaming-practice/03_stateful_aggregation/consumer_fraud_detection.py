"""
# Objectives
- Understand how to use stateful aggregation to accumulate data over time.
- Implement simple time-based anomaly detection for use cases like fraud detection.

# How to Run
1. Run the producer:
   `make run-practice-3-producer`
2. Run this consumer:
   `make run-practice-3-consumer`
3. Observe the output in the console. A warning will appear if a user makes more than 5 transactions
   within a 30-second window.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
import os

# Get KAFKA_HOST from environment variables
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("StatefulAggregationPractice")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schema for payment data
schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

# Read stream from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")
    .option("subscribe", "payments")
    .option("startingOffsets", "latest")
    .load()
)

# Deserialize and transform data
json_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
)

# Stateful aggregation: count transactions per user in a 30-second window.
# This is stateful because Spark must maintain the state (the count) for each user
# in each time window.
transaction_counts = json_df.groupBy(window(col("event_time"), "30 seconds"), col("user_id")).agg(
    count("transaction_id").alias("transaction_count")
)

# Fraud detection: filter for users with more than 5 transactions
fraud_alerts = transaction_counts.filter("transaction_count > 5")

# Display fraud alerts to the console
# Using "update" outputMode because of the aggregation.
# "update" will show the updated rows in each batch.
query = fraud_alerts.writeStream.outputMode("update").format("console").option("truncate", "false").start()

query.awaitTermination()
