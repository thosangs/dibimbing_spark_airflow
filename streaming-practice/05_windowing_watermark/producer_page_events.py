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

# Example list of web pages
PAGES = [f"/page/{i}.html" for i in range(1, 10)]
PAGES.extend(["/home", "/profile", "/settings", "/products/search"])

# Buffer to store events that will be sent late
late_events_buffer = []


def get_page_event(is_late=False):
    event_time = int(time.time())

    # If this event is intentionally made late, reduce its timestamp
    # by 65 to 120 seconds (more than the 1-minute watermark)
    if is_late:
        delay = random.randint(65, 120)
        event_time -= delay
        print(f"*** Preparing a late event (delay: {delay}s) ***")

    return {"page_url": random.choice(PAGES), "user_id": str(uuid.uuid4()), "event_timestamp": event_time}


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], value_serializer=json_serializer)

KAFKA_TOPIC = "page_events"

if __name__ == "__main__":
    print(f"Starting to send data to Kafka topic: {KAFKA_TOPIC}...")
    try:
        while True:
            # 5% chance to create a late event and store it in the buffer
            if random.random() < 0.05 and len(late_events_buffer) < 5:
                late_event = get_page_event(is_late=True)
                late_events_buffer.append(late_event)

            # Send a normal event
            page_event_data = get_page_event()
            print(f"Sending (normal): {page_event_data}")
            producer.send(KAFKA_TOPIC, page_event_data)

            # 10% chance to send an event from the late buffer
            if random.random() < 0.1 and late_events_buffer:
                event_to_send = late_events_buffer.pop(0)
                print(f"Sending (LATE): {event_to_send}")
                producer.send(KAFKA_TOPIC, event_to_send)

            time.sleep(random.uniform(1, 4))

    except KeyboardInterrupt:
        print("Sending stopped.")
    finally:
        producer.close()
