import time
import json
from faker import Faker
from kafka import KafkaProducer
import random
import os

# Get Kafka address from environment variable, default to 'localhost'
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = "9092"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"

print(f"KAFKA_HOST: {KAFKA_HOST}")

# Initialize Faker to generate fake data
fake = Faker()


# Function to generate a single web log record
def get_web_log():
    return {
        "ip": fake.ipv4(),
        "user_agent": fake.user_agent(),
        "path": fake.uri_path(),
        "method": random.choice(["GET", "POST", "PUT", "DELETE"]),
        "status_code": random.choice([200, 201, 400, 404, 500]),
        "response_time_ms": random.randint(50, 2000),  # Simulate response delay
        "timestamp": time.time(),
    }


# Serializer to convert a Python dictionary to JSON bytes
def json_serializer(data):
    return json.dumps(data).encode("utf-8")


# Initialize Kafka Producer
# Make sure the Kafka container is running
producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], value_serializer=json_serializer)

# The Kafka topic to be used
KAFKA_TOPIC = "web_logs"

if __name__ == "__main__":
    print(f"Starting to send data to Kafka topic: {KAFKA_TOPIC}...")
    try:
        while True:
            # Generate a log
            web_log_data = get_web_log()

            # Send to Kafka
            print(f"Sending: {web_log_data}")
            producer.send(KAFKA_TOPIC, web_log_data)

            # Wait a bit before sending the next record
            time.sleep(random.uniform(0.5, 2))
    except KeyboardInterrupt:
        print("Sending stopped.")
    finally:
        producer.close()
