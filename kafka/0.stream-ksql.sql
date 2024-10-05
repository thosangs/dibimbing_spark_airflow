CREATE STREAM employee_stream (
    emp_id VARCHAR,
    employee_name VARCHAR,
    department VARCHAR,
    state VARCHAR,
    salary INT,
    age INT,
    bonus INT,
    ts BIGINT,
    new BOOLEAN
) WITH (
    KAFKA_TOPIC = 'employee_data',
    VALUE_FORMAT = 'JSON'
);