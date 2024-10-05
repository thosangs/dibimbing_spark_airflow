import employee_pb2

# Read the Protobuf data from the file
with open("./kafka/employee_data.protobuf", "rb") as f:
    protobuf_data = f.read()

# Deserialize the data into an Employee object
employee = employee_pb2.Employee()
employee.ParseFromString(protobuf_data)

print(employee)
