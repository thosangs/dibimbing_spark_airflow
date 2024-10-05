CREATE TABLE employee_table AS
SELECT emp_id,
       LATEST_BY_OFFSET(employee_name) AS employee_name,
       LATEST_BY_OFFSET(department) AS department,
       LATEST_BY_OFFSET(state) AS state,
       LATEST_BY_OFFSET(salary) AS salary,
       LATEST_BY_OFFSET(age) AS age,
       LATEST_BY_OFFSET(bonus) AS bonus,
       LATEST_BY_OFFSET(ts) AS ts,
       LATEST_BY_OFFSET(new) AS new
FROM employee_stream
GROUP BY emp_id;
