"""
# Objectives
- Detect orders that have not been paid or payments that do not have a corresponding order.
- Simulate the correlation between two different real-time systems (e.g., an order system and a payment system).

# How to Run
1. Run both producers in separate terminals:
   `make run-practice-7-producer-orders`
   `make run-practice-7-producer-payments`
2. Run this consumer:
   `make run-practice-7-consumer`
3. Observe the output in the console. You will see the result of the join between orders and payments.
   Use an 'outer' join to see orders without payments or vice versa.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType
import os

# Get KAFKA_HOST from environment variables
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("StreamStreamJoinPractice")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Schema for the orders stream
orders_schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_timestamp", LongType(), True),
    ]
)

# Schema for the payments stream
payments_schema = StructType(
    [
        StructField("payment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_timestamp", LongType(), True),
    ]
)

# Read the orders stream from Kafka
orders_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")
    .option("subscribe", "orders")
    .load()
    .select(from_json(col("value").cast("string"), orders_schema).alias("order"))
    .select("order.*")
    .withColumnRenamed("order_id", "o_order_id")
    .withColumn("order_time", col("order_timestamp").cast("timestamp"))
    .withWatermark("order_time", "2 minutes")
)

# Read the payments stream from Kafka
payments_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")
    .option("subscribe", "payments_join")
    .load()
    .select(from_json(col("value").cast("string"), payments_schema).alias("payment"))
    .select("payment.*")
    .withColumn("payment_time", col("payment_timestamp").cast("timestamp"))
    .withWatermark("payment_time", "2 minutes")
)

# Perform the Stream-Stream Join
# Using an 'inner' join to find orders that match their payments.
# Join condition: order_id is the same AND the event times are close (within a 2-minute range).
joined_df = orders_stream.join(
    payments_stream,
    expr(
        """
        o_order_id = order_id AND 
        payment_time >= order_time AND 
        payment_time <= order_time + interval 2 minute
    """
    ),
    "inner",  # Try changing to "leftOuter" or "rightOuter" to see unmatched orders/payments
)


# Display the join result to the console
query = joined_df.writeStream.outputMode("append").format("console").option("truncate", "false").start()

query.awaitTermination()
