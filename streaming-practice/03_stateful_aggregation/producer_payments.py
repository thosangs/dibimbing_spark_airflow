import time
import json
from faker import Faker
from kafka import KafkaProducer
import random
import uuid
import os

# Get Kafka address from environment variable, default to 'localhost'
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = "9092"
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"

fake = Faker()

USER_IDS = list(range(1, 11))  # 10 users


def get_payment_data():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "amount": round(random.uniform(1.0, 500.0), 2),
        "timestamp": int(time.time()),
    }


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], value_serializer=json_serializer)

KAFKA_TOPIC = "payments"


def produce_burst_of_transactions(user_id, num_transactions):
    """Send several transactions quickly for a single user."""
    print(f"--- Starting burst for user_id: {user_id} ---")
    for _ in range(num_transactions):
        payment_data = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "amount": round(random.uniform(1.0, 50.0), 2),
            "timestamp": int(time.time()),
        }
        print(f"Sending (burst): {payment_data}")
        producer.send(KAFKA_TOPIC, payment_data)
        time.sleep(random.uniform(0.1, 0.5))
    print(f"--- Finished burst for user_id: {user_id} ---")


if __name__ == "__main__":
    print(f"Starting to send data to Kafka topic: {KAFKA_TOPIC}...")
    try:
        while True:
            # 5% chance to generate a burst of transactions
            if random.random() < 0.05:
                user_to_burst = random.choice(USER_IDS)
                produce_burst_of_transactions(user_to_burst, random.randint(6, 10))
            else:
                # Send a normal transaction
                payment_data = get_payment_data()
                print(f"Sending (normal): {payment_data}")
                producer.send(KAFKA_TOPIC, payment_data)

            time.sleep(random.uniform(1, 4))

    except KeyboardInterrupt:
        print("Sending stopped.")
    finally:
        producer.close()
