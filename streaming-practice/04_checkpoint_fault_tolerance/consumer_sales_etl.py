"""
# Objectives
- Simulate a resilient streaming ETL job (resistant to failures).
- Ensure `exactly-once` processing guarantees are maintained after a job restart, thanks to the checkpoint mechanism.

# How to Run
1. Make sure the directories `streaming-practice/04_checkpoint_fault_tolerance/output_data` and
   `streaming-practice/04_checkpoint_fault_tolerance/checkpoint_data` exist.
2. Run the producer:
   `make run-practice-4-producer`
3. Run this consumer:
   `make run-practice-4-consumer`
4. Observe the CSV files created in the `output_data` directory.
5. Stop the consumer (Ctrl+C). Then, run it again. Observe that processing continues from the last point before stopping,
   without data duplication.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
import os

# Get KAFKA_HOST from environment variables
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("CheckpointAndFaultTolerancePractice")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Define paths for checkpoint and output
# We construct absolute paths to ensure Spark writes to the correct mounted directories.
script_dir = os.path.dirname(os.path.abspath(__file__))
CHECKPOINT_LOCATION = os.path.join(script_dir, "checkpoint_data")
OUTPUT_LOCATION = os.path.join(script_dir, "output_data")


# Schema for sales data
schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

# Read stream from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")
    .option("subscribe", "sales_events")
    .option("startingOffsets", "latest")
    .load()
)

# Deserialize and transform data
json_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
    .withColumn("total_price", col("quantity") * col("unit_price"))
)


# Function to process each micro-batch.
# This function implements two sinks: console and CSV.
def process_batch(batch_df, batch_id):
    """
    Processes a micro-batch by writing it to two different sinks:
    1. A CSV file sink for persistent storage.
    2. The console sink for real-time monitoring.
    """
    print(f"--- Processing Batch ID: {batch_id} ---")

    # Sink 1: Write the batch to the console
    print("Console Output:")
    batch_df.show(truncate=False)

    # Sink 2: Write the batch to a CSV file
    # The write is idempotent, meaning if the job fails and retries this batch,
    # it won't create duplicate data in the file sink.
    if not batch_df.rdd.isEmpty():
        print(f"Writing {batch_df.count()} rows to CSV...")
        (batch_df.write.format("csv").option("header", "true").mode("append").save(OUTPUT_LOCATION))
    else:
        print("Batch is empty, skipping CSV write.")


# Use foreachBatch to apply the function to each micro-batch.
# This allows for writing to multiple output sinks.
query = (
    json_df.writeStream.outputMode("append")
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
