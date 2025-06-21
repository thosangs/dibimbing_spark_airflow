import time
import json
from kafka import KafkaProducer
import random
import os

# Get Kafka address from environment variable, default to 'localhost'
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = "9092"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"

# User IDs that exist in the users.csv file
USER_IDS = list(range(1, 11))
PRODUCT_IDS = [f"prod_{100+i}" for i in range(20)]


def get_click_event():
    return {"user_id": random.choice(USER_IDS), "product_id": random.choice(PRODUCT_IDS), "timestamp": int(time.time())}


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], value_serializer=json_serializer)

KAFKA_TOPIC = "clickstream"

if __name__ == "__main__":
    print(f"Starting to send data to Kafka topic: {KAFKA_TOPIC}...")
    try:
        while True:
            click_event_data = get_click_event()
            print(f"Sending: {click_event_data}")
            producer.send(KAFKA_TOPIC, click_event_data)
            time.sleep(random.uniform(0.5, 3))
    except KeyboardInterrupt:
        print("Sending stopped.")
    finally:
        producer.close()
