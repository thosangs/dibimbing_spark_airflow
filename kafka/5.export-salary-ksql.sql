CREATE TABLE avg_salary_exported WITH (
    KAFKA_TOPIC='avg_salary_topic',
    VALUE_FORMAT='JSON'
) AS
SELECT
    department,
    avg_salary
FROM avg_salary_table;
