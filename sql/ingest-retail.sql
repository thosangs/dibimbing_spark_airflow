COPY retail FROM '/data/online-retail-dataset.csv' DELIMITER AS ',' CSV HEADER;
SELECT * FROM retail LIMIT 5;