include .env

help:
	@echo "## docker-build			- Build Docker Images (amd64) including its inter-container network."
	@echo "## docker-build-arm		- Build Docker Images (arm64) including its inter-container network."
	@echo "## postgres			- Run a Postgres container  "
	@echo "## spark			- Run a Spark cluster, rebuild the postgres container, then create the destination tables "
	@echo "## jupyter			- Spinup jupyter notebook for testing and validation purposes."
	@echo "## airflow			- Spinup airflow scheduler and webserver."
	@echo "## kafka			- Spinup kafka cluster (Kafka+Zookeeper)."
	@echo "## datahub			- Spinup datahub instances."
	@echo "## metabase			- Spinup metabase instance."
	@echo "## clean			- Cleanup all running containers related to the challenge."

docker-build-slim:
	@chmod 777 logs/
	@chmod 777 notebooks/
	@docker network inspect dataeng-network >/dev/null 2>&1 || docker network create dataeng-network
	@docker build -t dataeng-dibimbing/jupyter -f ./docker/Dockerfile.jupyter .

docker-build-slim-windows:
	@docker build -t dataeng-dibimbing/jupyter -f ./docker/Dockerfile.jupyter .

docker-build:
	@echo '__________________________________________________________'
	@echo 'Building Docker Images ...'
	@echo '__________________________________________________________'
	@chmod 777 logs/
	@chmod 777 notebooks/
	@docker network inspect dataeng-network >/dev/null 2>&1 || docker network create dataeng-network
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/spark -f ./docker/Dockerfile.spark .
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/airflow -f ./docker/Dockerfile.airflow .
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/jupyter -f ./docker/Dockerfile.jupyter .
	@echo '==========================================================='

docker-build-windows:
	@echo '__________________________________________________________'
	@echo 'Building Docker Images ...'
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/spark -f ./docker/Dockerfile.spark .
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/airflow -f ./docker/Dockerfile.airflow .
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/jupyter -f ./docker/Dockerfile.jupyter .
	@echo '==========================================================='


docker-build-arm:
	@echo '__________________________________________________________'
	@echo 'Building Docker Images ...'
	@echo '__________________________________________________________'
	@docker network inspect dataeng-network >/dev/null 2>&1 || docker network create dataeng-network
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/spark -f ./docker/Dockerfile.spark .
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/airflow -f ./docker/Dockerfile.airflow-arm .
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/jupyter -f ./docker/Dockerfile.jupyter .
	@echo '==========================================================='

jupyter:
	@echo '__________________________________________________________'
	@echo 'Creating Jupyter Notebook Cluster at http://localhost:${JUPYTER_PORT} ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-jupyter.yml --env-file .env up -d
	@echo 'Created...'
	@echo 'Processing token...'
	@sleep 20
	@docker logs ${JUPYTER_CONTAINER_NAME} 2>&1 | grep '\?token\=' -m 1 | cut -d '=' -f2
	@echo '==========================================================='

spark:
	@echo '__________________________________________________________'
	@echo 'Creating Spark Cluster ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-spark.yml --env-file .env up -d
	@echo '==========================================================='

spark-submit-test:
	@docker exec ${SPARK_WORKER_CONTAINER_NAME}-1 \
		spark-submit \
		--master spark://${SPARK_MASTER_HOST_NAME}:${SPARK_MASTER_PORT} \
		/spark-scripts/spark-example.py

spark-submit-airflow-test:
	@docker exec ${AIRFLOW_WEBSERVER_CONTAINER_NAME} \
		spark-submit \
		--master spark://${SPARK_MASTER_HOST_NAME}:${SPARK_MASTER_PORT} \
		--conf "spark.standalone.submit.waitAppCompletion=false" \
		--conf "spark.ui.enabled=false" \
		/spark-scripts/spark-example.py

airflow:
	@echo '__________________________________________________________'
	@echo 'Creating Airflow Instance ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-airflow.yml --env-file .env up
	@echo '==========================================================='

postgres: postgres-create postgres-create-warehouse postgres-create-table postgres-ingest-csv

postgres-create:
	@docker compose -f ./docker/docker-compose-postgres.yml --env-file .env up -d
	@echo '__________________________________________________________'
	@echo 'Postgres container created at port ${POSTGRES_PORT}...'
	@echo '__________________________________________________________'
	@echo 'Postgres Docker Host	: ${POSTGRES_CONTAINER_NAME}' &&\
		echo 'Postgres Account	: ${POSTGRES_USER}' &&\
		echo 'Postgres password	: ${POSTGRES_PASSWORD}' &&\
		echo 'Postgres Db		: ${POSTGRES_DW_DB}'
	@sleep 5
	@echo '==========================================================='

postgres-create-table:
	@echo '__________________________________________________________'
	@echo 'Creating tables...'
	@echo '_________________________________________'
	@docker exec -it ${POSTGRES_CONTAINER_NAME} psql -U ${POSTGRES_USER} -d ${POSTGRES_DW_DB} -f sql/ddl-retail.sql
	@echo '==========================================================='

postgres-ingest-csv:
	@echo '__________________________________________________________'
	@echo 'Ingesting CSV...'
	@echo '_________________________________________'
	@docker exec -it ${POSTGRES_CONTAINER_NAME} psql -U ${POSTGRES_USER} -d ${POSTGRES_DW_DB} -f sql/ingest-retail.sql
	@echo '==========================================================='

postgres-create-warehouse:
	@echo '__________________________________________________________'
	@echo 'Creating Warehouse DB...'
	@echo '_________________________________________'
	@docker exec -it ${POSTGRES_CONTAINER_NAME} psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -f sql/warehouse-ddl.sql
	@echo '==========================================================='

kafka: kafka-create

kafka-create:
	@echo '__________________________________________________________'
	@echo 'Creating Kafka Cluster ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-kafka.yml --env-file .env up -d
	@echo 'Waiting for uptime on http://localhost:8083 ...'
	@sleep 20
	@echo '==========================================================='

kafka-create-test-topic:
	@docker exec ${KAFKA_CONTAINER_NAME} \
		kafka-topics.sh --create \
		--partitions 3 \
		--replication-factor ${KAFKA_REPLICATION} \
		--bootstrap-server localhost:9092 \
		--topic ${KAFKA_TOPIC_NAME}

kafka-create-topic:
	@docker exec ${KAFKA_CONTAINER_NAME} \
		kafka-topics.sh --create \
		--partitions ${partition} \
		--replication-factor ${KAFKA_REPLICATION} \
		--bootstrap-server localhost:9092 \
		--topic ${topic}

spark-produce:
	@echo '__________________________________________________________'
	@echo 'Producing fake events ...'
	@echo '__________________________________________________________'
	@docker exec ${SPARK_WORKER_CONTAINER_NAME}-1 \
		python \
		/scripts/event_producer.py

spark-consume:
	@echo '__________________________________________________________'
	@echo 'Consuming fake events ...'
	@echo '__________________________________________________________'
	@docker exec ${SPARK_WORKER_CONTAINER_NAME}-1 \
		spark-submit \
		/spark-scripts/spark-event-consumer.py

datahub-create:
	@echo '__________________________________________________________'
	@echo 'Creating Datahub Instance ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-datahub.yml --env-file .env up
	@echo '==========================================================='

datahub-ingest:
	@echo '__________________________________________________________'
	@echo 'Ingesting Data to Datahub ...'
	@echo '__________________________________________________________'
	@datahub ingest -c ./datahub/sample.yaml --dry-run
	@echo '==========================================================='

metabase: postgres-create-warehouse
	@echo '__________________________________________________________'
	@echo 'Creating Metabase Instance ...'
	@echo '__________________________________________________________'
	@docker compose -f ./docker/docker-compose-metabase.yml --env-file .env up
	@echo '==========================================================='

clean:
	@bash ./scripts/goodnight.sh


postgres-bash:
	@docker exec -it dataeng-postgres bash

## =================================================================================================
## SPARK STREAMING PRACTICE
## =================================================================================================
#
# Usage:
#   make run-producer P=<practice_number>
#   make run-consumer P=<practice_number>
#
# Example:
#   make run-producer P=1
#   make run-consumer P=1
#
# To run the model training for practice 8:
#   make run-practice-8-train
#
# -------------------------------------------------------------------------------------------------

# Default to Practice 1 if P is not set
P ?= 1

# --- Practice Configurations ---
# For each practice, define the directory, producer script, consumer script, and any extra packages.
# Note: For P1 producer, we use a special command.
P_1_DIR := 01_trigger_output_mode
P_1_PRODUCER_CMD := @echo "Running producer for Practice 1: Trigger and Output Mode..."; docker exec -it ${SPARK_MASTER_CONTAINER_NAME} python /streaming-practice/$(P_1_DIR)/producer_web_logs.py
P_1_CONSUMER_SCRIPT := consumer_trigger_output.py
P_1_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

P_2_DIR := 02_stateless_filtering
P_2_PRODUCER_SCRIPT := producer_security_logs.py
P_2_CONSUMER_SCRIPT := consumer_stateless_filter.py
P_2_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

P_3_DIR := 03_stateful_aggregation
P_3_PRODUCER_SCRIPT := producer_payments.py
P_3_CONSUMER_SCRIPT := consumer_fraud_detection.py
P_3_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

P_4_DIR := 04_checkpoint_fault_tolerance
P_4_PRODUCER_SCRIPT := producer_sales.py
P_4_CONSUMER_SCRIPT := consumer_sales_etl.py
P_4_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

P_5_DIR := 05_windowing_watermark
P_5_PRODUCER_SCRIPT := producer_page_events.py
P_5_CONSUMER_SCRIPT := consumer_page_views.py
P_5_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

P_6_DIR := 06_stream_static_join
P_6_PRODUCER_SCRIPT := producer_clickstream.py
P_6_CONSUMER_SCRIPT := consumer_recommendations.py
P_6_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

P_7_DIR := 07_stream_stream_join
P_7_PRODUCER_SCRIPT := producer_orders_and_payments.py
P_7_CONSUMER_SCRIPT := consumer_order_fulfillment.py
P_7_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

P_8_DIR := 08_streaming_ml_fraud
P_8_PRODUCER_SCRIPT := producer_live_transactions.py
P_8_CONSUMER_SCRIPT := consumer_fraud_prediction.py
P_8_TRAIN_SCRIPT := train_model.py
P_8_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-mllib_2.12:3.5.0

# --- Generic Targets ---
# These targets use variable indirection to get the configuration for the selected practice (P).
P_DIR := $(P_$(P)_DIR)
P_PRODUCER_SCRIPT := $(P_$(P)_PRODUCER_SCRIPT)
P_CONSUMER_SCRIPT := $(P_$(P)_CONSUMER_SCRIPT)
P_PACKAGES := $(P_$(P)_PACKAGES)

run-producer:
	@if [ "$(P)" = "1" ]; then \
		$(P_1_PRODUCER_CMD); \
	else \
		echo "Running producer for Practice $(P)..."; \
		docker exec -it ${SPARK_MASTER_CONTAINER_NAME} python streaming-practice/$(P_DIR)/$(P_PRODUCER_SCRIPT); \
	fi

run-consumer:
	@echo "Running consumer for Practice $(P)..."
	docker exec -it ${SPARK_MASTER_CONTAINER_NAME} spark-submit \
		--packages $(P_PACKAGES) \
		streaming-practice/$(P_DIR)/$(P_CONSUMER_SCRIPT)

# --- Special Targets ---
run-practice-8-train:
	@echo "Training ML model for Practice 8..."
	docker exec -it ${SPARK_MASTER_CONTAINER_NAME} spark-submit streaming-practice/$(P_8_DIR)/$(P_8_TRAIN_SCRIPT)
