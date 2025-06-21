import time
import json
from kafka import KafkaProducer
import random
import uuid
import os

# Get Kafka address from environment variable, default to 'localhost'
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = "9092"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"


def get_sales_event():
    return {
        "order_id": str(uuid.uuid4()),
        "product_id": random.randint(100, 200),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10.0, 100.0), 2),
        "timestamp": int(time.time()),
    }


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], value_serializer=json_serializer)

KAFKA_TOPIC = "sales_events"

if __name__ == "__main__":
    print(f"Starting to send data to Kafka topic: {KAFKA_TOPIC}...")
    try:
        while True:
            sales_event_data = get_sales_event()
            print(f"Sending: {sales_event_data}")
            producer.send(KAFKA_TOPIC, sales_event_data)
            time.sleep(random.uniform(1, 3))
    except KeyboardInterrupt:
        print("Sending stopped.")
    finally:
        producer.close()
