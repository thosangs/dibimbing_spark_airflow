import json
import uuid
import os
import json
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from time import sleep
from datetime import datetime, timedelta

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
faker = Faker()


class DataGenerator(object):
    @staticmethod
    def get_data():
        now = datetime.now()
        return [
            uuid.uuid4().__str__(),
            faker.random_int(min=1, max=100),
            faker.random_element(elements=("Chair", "Table", "Desk", "Sofa", "Bed")),
            faker.safe_color_name(),
            faker.random_int(min=100, max=150000),
            faker.unix_time(
                start_datetime=now - timedelta(minutes=60), end_datetime=now
            ),
        ]


while True:
    columns = [
        "order_id",
        "customer_id",
        "furniture",
        "color",
        "price",
        "ts",
    ]
    data_list = DataGenerator.get_data()
    json_data = dict(zip(columns, data_list))
    _payload = json.dumps(json_data).encode("utf-8")
    print(_payload, flush=True)
    print("=-" * 5, flush=True)
    response = producer.send(topic=kafka_topic, value=_payload)
    print(response.get())
    print("=-" * 20, flush=True)
    sleep(6)
