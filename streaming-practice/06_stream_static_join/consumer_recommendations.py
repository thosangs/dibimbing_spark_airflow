"""
# Objectives
- Understand how to enrich streaming data with data from a static table.
- Build basic logic for real-time personalization or recommendations.

# How to Run
1. Run the producer:
   `make run-practice-6-producer`
2. Run this consumer:
   `make run-practice-6-consumer`
3. Observe the output in the console. Each click event will be joined with user demographic data
   from the `users.csv` file.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import os

# Get KAFKA_HOST from environment variables
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("StreamStaticJoinPractice")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Read static data (Static DataFrame)
script_dir = os.path.dirname(os.path.abspath(__file__))
users_csv_path = os.path.join(script_dir, "users.csv")

users_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(users_csv_path)

print("Static User Data:")
users_df.show()

# 2. Read streaming data (Streaming DataFrame)
clickstream_schema = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")
    .option("subscribe", "clickstream")
    .option("startingOffsets", "latest")
    .load()
)

clickstream_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), clickstream_schema).alias("data"))
    .select("data.*")
)

# 3. Perform the Stream-Static Join
enriched_stream = clickstream_df.join(users_df, "user_id", "inner")

# Display the join result to the console
query = enriched_stream.writeStream.outputMode("append").format("console").option("truncate", "false").start()

query.awaitTermination()
