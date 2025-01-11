import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path

from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

# spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

# spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"
)

spark = (
    pyspark.sql.SparkSession.builder.appName("DibimbingStreaming")
    # .master(spark_host)
    .master("local")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.sql.shuffle.partitions", 4)
    # .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("furniture", StringType(), True),
        StructField("color", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("ts", LongType(), True),
    ]
)

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = stream_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

avg_price_df = parsed_df.groupBy("furniture").agg(avg("price").alias("avg_price"))

# Write the result to the console
query = avg_price_df.writeStream.outputMode("update").format("console").trigger(processingTime="10 seconds").start()

query.awaitTermination()
