CREATE TABLE avg_salary_per_minute AS
SELECT
    department,
    AVG(salary) AS avg_salary
FROM employee_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY department
EMIT CHANGES; 