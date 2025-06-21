import time
import json
from kafka import KafkaProducer
import random
import uuid
import os

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = "9092"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"


def get_live_transaction():
    # 5% chance of a large transaction in vulnerable hours (midnight)
    if random.random() < 0.05:
        amount = round(random.uniform(1000, 5000), 2)
        hour = random.randint(0, 5)
    else:
        amount = round(random.uniform(5, 500), 2)
        hour = random.randint(6, 23)

    return {
        "transaction_id": str(uuid.uuid4()),
        "amount": amount,
        "hour_of_day": hour,
        "is_foreign_transaction": random.choice([0, 1]),
        "timestamp": int(time.time()),
    }


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], value_serializer=json_serializer)

KAFKA_TOPIC = "live_transactions"

if __name__ == "__main__":
    print(f"Starting to send data to Kafka topic: {KAFKA_TOPIC}...")
    try:
        while True:
            transaction_data = get_live_transaction()
            print(f"Sending: {transaction_data}")
            producer.send(KAFKA_TOPIC, transaction_data)
            time.sleep(random.uniform(1, 4))
    except KeyboardInterrupt:
        print("Sending stopped.")
    finally:
        producer.close()
