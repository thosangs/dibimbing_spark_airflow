-- This stream will hold metadata about each department, like its physical location.
-- The department name is the KEY of the stream.
CREATE STREAM department_metadata_stream (
    department VARCHAR KEY,
    location VARCHAR
) WITH (
    KAFKA_TOPIC = 'department_metadata',
    VALUE_FORMAT = 'JSON'
);

-- This stream enriches the employee data with the department location
-- by joining the two streams on the department field.
CREATE STREAM employee_enriched_stream AS
SELECT
    e.emp_id,
    e.employee_name,
    e.department,
    d.location,
    e.state,
    e.salary,
    e.age,
    e.bonus
FROM
    employee_stream e
INNER JOIN
    department_metadata_stream d
ON
    e.department = d.department
EMIT CHANGES; 