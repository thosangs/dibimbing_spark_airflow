CREATE TABLE avg_salary_table AS
SELECT
    department,
    AVG(salary) AS avg_salary
FROM employee_stream
GROUP BY department
EMIT CHANGES;