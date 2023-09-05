docker-build:
	@echo '__________________________________________________________'
	@echo 'Building Docker Images ...'
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/spark -f ./docker/spark.dockerfile .
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/airflow -f ./docker/airflow.dockerfile .
	@echo '==========================================================='

docker-up:
	@echo '__________________________________________________________'
	@echo 'Spin Up Docker Containers ...'
	@echo '__________________________________________________________'
	@docker-compose --env-file .env up
	@echo '==========================================================='

