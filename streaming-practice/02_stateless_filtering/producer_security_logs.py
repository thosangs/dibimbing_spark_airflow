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

fake = Faker()

# The same IP list as in blacklist_ips.txt to ensure some are filtered
BLACKLIST_IPS = ["127.0.0.1", "192.168.1.10", "10.0.0.5", "203.0.113.42", "198.51.100.99"]
EVENT_TYPES = ["login_success", "login_failure", "file_access", "firewall_block", "sql_injection_attempt"]
USERS = ["admin", "guest", "root", "testuser", fake.user_name()]


def get_security_log():
    # 30% chance of using an IP from the blacklist
    if random.random() < 0.3:
        source_ip = random.choice(BLACKLIST_IPS)
    else:
        source_ip = fake.ipv4()

    return {
        "source_ip": source_ip,
        "event_type": random.choice(EVENT_TYPES),
        "user": random.choice(USERS),
        "event_timestamp": int(time.time()),
    }


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], value_serializer=json_serializer)

KAFKA_TOPIC = "security_logs"

if __name__ == "__main__":
    print(f"Starting to send data to Kafka topic: {KAFKA_TOPIC}...")
    try:
        while True:
            security_log_data = get_security_log()
            print(f"Sending: {security_log_data}")
            producer.send(KAFKA_TOPIC, security_log_data)
            time.sleep(random.uniform(0.2, 1.5))
    except KeyboardInterrupt:
        print("Sending stopped.")
    finally:
        producer.close()
