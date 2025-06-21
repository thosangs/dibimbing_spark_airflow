"""
# Objectives
- Understand how to integrate a pre-trained Machine Learning model into a Spark Structured Streaming pipeline.
- Apply the model for inference (prediction) on real-time data.
- Build an end-to-end streaming application that combines data processing and ML.

# How to Run
1. Train and save the model first (only needs to be done once):
   `make run-practice-8-train-model`
2. Run the producer in one terminal:
   `make run-practice-8-producer`
3. Run this consumer in another terminal:
   `make run-practice-8-consumer`
4. Observe the output in the console. Only transactions predicted as fraud (prediction=1.0) will be displayed.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
from pyspark.ml import PipelineModel
import os

# Get KAFKA_HOST from environment variables
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("StreamingFraudPrediction")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Load the pre-trained ML Model
script_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.join(script_dir, "fraud_detection_model")
loaded_model = PipelineModel.load(model_path)
print("Fraud detection model loaded successfully.")

# 2. Read the Transaction Stream from Kafka
schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("is_foreign_transaction", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:9092")
    .option("subscribe", "live_transactions")
    .option("startingOffsets", "latest")
    .load()
)

json_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# 3. Apply the Model to the Streaming Data
# The model (pipeline) we loaded will automatically create the 'features' and 'prediction' columns
predictions_df = loaded_model.transform(json_df)

# 4. Filter and Display the Prediction Results
# Display only transactions detected as fraud (prediction = 1.0)
fraudulent_transactions = predictions_df.filter(col("prediction") == 1.0).select(
    "transaction_id", "amount", "hour_of_day", "is_foreign_transaction", "prediction"
)

query = fraudulent_transactions.writeStream.outputMode("append").format("console").option("truncate", "false").start()

query.awaitTermination()
