from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("TrainFraudDetectionModel").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define paths based on the script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(script_dir, "training_data.csv")
model_path = os.path.join(script_dir, "fraud_detection_model")

# Load training data
df = spark.read.csv(data_path, header=True, inferSchema=True)

print("Training Data:")
df.show()

# Define features and label
feature_cols = ["amount", "hour_of_day", "is_foreign_transaction"]
label_col = "is_fraud"

# Create an assembler to combine features into a single vector
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Create a Logistic Regression model
lr = LogisticRegression(featuresCol="features", labelCol=label_col)

# Create the ML pipeline
pipeline = Pipeline(stages=[assembler, lr])

# Train the model
model = pipeline.fit(df)

# Define the path to save the model
print(f"Saving model to: {model_path}")

# Remove the old model if it exists and save the new model
model.write().overwrite().save(model_path)

print("Model saved successfully.")
spark.stop()
