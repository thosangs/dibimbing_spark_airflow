"""
# Objectives
- Apply stateless filters and transformations to streaming data.
- Simulate real-time alerts for a SIEM (Security Information and Event Management) system.

# How to Run
1. Run the producer:
   `make run-practice-2-producer`
2. Run this consumer:
   `make run-practice-2-consumer`
3. Observe the output in the console. Only logs from blacklisted IPs will appear.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
import os


# Function to read the list of blocked IPs
def get_blacklist_ips(file_path):
    with open(file_path, "r") as f:
        return [line.strip() for line in f.readlines()]


# Get KAFKA_HOST from environment variables
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("StatelessFilteringPractice")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read the IP list from the file
# In a distributed environment, this file must be accessible to all workers,
# or a broadcast variable should be used.
script_dir = os.path.dirname(os.path.abspath(__file__))
blacklist_path = os.path.join(script_dir, "blacklist_ips.txt")
blacklist_ips = get_blacklist_ips(blacklist_path)
print(f"Blacklisted IPs: {blacklist_ips}")

# Schema for security log data
schema = StructType(
    [
        StructField("source_ip", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user", StringType(), True),
        StructField("event_timestamp", LongType(), True),
    ]
)

# Read stream from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")
    .option("subscribe", "security_logs")
    .option("startingOffsets", "latest")
    .load()
)

# Deserialize and transform data
json_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", col("event_timestamp").cast(TimestampType()))
)

# Stateless filter: check if the source_ip is in the blacklist.
# This is a stateless operation because each record is evaluated independently.
suspicious_logs_df = json_df.filter(col("source_ip").isin(blacklist_ips))

# Display the suspicious logs to the console
query = suspicious_logs_df.writeStream.outputMode("append").format("console").option("truncate", "false").start()

query.awaitTermination()
