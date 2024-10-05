import os

protobuf_file_size = os.path.getsize("./kafka/employee_data.protobuf")
json_file_size = os.path.getsize("./kafka/employee_data.json")

print(f"Protobuf file size: {protobuf_file_size} bytes")
print(f"JSON file size: {json_file_size} bytes")
