#!/bin/bash
airflow db init
echo "AUTH_ROLE_PUBLIC = 'Admin'" >> webserver_config.py
airflow connections add 'postgres_main' \
--conn-type 'postgres' \
--conn-login $POSTGRES_USER \
--conn-password $POSTGRES_PASSWORD \
--conn-host $POSTGRES_CONTAINER_NAME \
--conn-port $POSTGRES_PORT \
--conn-schema $POSTGRES_DB
airflow connections add 'postgres_dw' \
--conn-type 'postgres' \
--conn-login $POSTGRES_USER \
--conn-password $POSTGRES_PASSWORD \
--conn-host $POSTGRES_CONTAINER_NAME \
--conn-port $POSTGRES_PORT \
--conn-schema $POSTGRES_DW_DB
export SPARK_FULL_HOST_NAME="spark://$SPARK_MASTER_HOST_NAME"
airflow connections add 'spark_main' \
--conn-type 'spark' \
--conn-host $SPARK_FULL_HOST_NAME \
--conn-port $SPARK_MASTER_PORT
airflow webserver
