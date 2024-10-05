import employee_pb2
import uuid
from faker import Faker
import json

faker = Faker()


class DataGenerator(object):
    @staticmethod
    def get_data():
        return [
            uuid.uuid4().__str__(),
            faker.name(),
            faker.random_element(elements=("IT", "HR", "Sales", "Marketing")),
            faker.random_element(elements=("CA", "NY", "TX", "FL", "IL", "RJ")),
            faker.random_int(min=10000, max=150000),
            faker.random_int(min=18, max=60),
            faker.random_int(min=0, max=100000),
            int(faker.unix_time()),
        ]


data_list = DataGenerator.get_data()

# Create Protobuf message
employee = employee_pb2.Employee(
    emp_id=data_list[0],
    employee_name=data_list[1],
    department=data_list[2],
    state=data_list[3],
    salary=data_list[4],
    age=data_list[5],
    bonus=data_list[6],
    ts=data_list[7],
    new=True,
)
protobuf_data = employee.SerializeToString()
with open("kafka/employee_data.protobuf", "wb") as f:
    f.write(protobuf_data)


# Create JSON data
json_data = {
    "emp_id": data_list[0],
    "employee_name": data_list[1],
    "department": data_list[2],
    "state": data_list[3],
    "salary": data_list[4],
    "age": data_list[5],
    "bonus": data_list[6],
    "ts": data_list[7],
    "new": True,
}

json_str = json.dumps(json_data)
with open("kafka/employee_data.json", "w") as f:
    f.write(json_str)
