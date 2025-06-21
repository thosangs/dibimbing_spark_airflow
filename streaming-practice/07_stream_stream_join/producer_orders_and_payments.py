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

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

KAFKA_TOPIC_ORDERS = "orders"
KAFKA_TOPIC_PAYMENTS = "payments_join"

# Buffer to store newly created order IDs, so the payment producer can use them
order_id_buffer = []


def generate_order():
    order_id = str(uuid.uuid4())
    order_data = {
        "order_id": order_id,
        "user_id": random.randint(1, 100),
        "total_amount": round(random.uniform(20.0, 1000.0), 2),
        "order_timestamp": int(time.time()),
    }
    # Store the order_id so it can be used by the payment
    if len(order_id_buffer) < 10:
        order_id_buffer.append(order_id)

    print(f"Sending ORDER: {order_data}")
    producer.send(KAFKA_TOPIC_ORDERS, order_data)


def generate_payment():
    # 70% chance the payment corresponds to an existing order in the buffer
    if order_id_buffer and random.random() < 0.7:
        order_id = order_id_buffer.pop(0)
    else:
        # 30% chance of a payment without a matching order (or empty buffer)
        order_id = str(uuid.uuid4())

    payment_data = {
        "payment_id": str(uuid.uuid4()),
        "order_id": order_id,
        "payment_method": random.choice(["credit_card", "jenius", "bank_transfer"]),
        "payment_timestamp": int(time.time()) + random.randint(1, 10),  # payment slightly after the order
    }
    print(f"Sending PAYMENT: {payment_data}")
    producer.send(KAFKA_TOPIC_PAYMENTS, payment_data)


if __name__ == "__main__":
    print(f"Starting to send data to topics {KAFKA_TOPIC_ORDERS} and {KAFKA_TOPIC_PAYMENTS}...")
    try:
        while True:
            # Generate orders and payments alternately
            generate_order()
            time.sleep(random.uniform(1, 3))
            generate_payment()
            time.sleep(random.uniform(1, 3))

    except KeyboardInterrupt:
        print("Sending stopped.")
    finally:
        producer.close()
