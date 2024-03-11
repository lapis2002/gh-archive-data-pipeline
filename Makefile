build:
	docker compose -f monitoring/prom-graf-docker-compose.yaml up -d
	docker compose -f docker-compose.yaml -f dockers/kafka-flink-docker-compose.yaml -f dockers/airflow-docker-compose.yaml up -d
tear-down:
	docker stop $$(docker ps -q)
	docker rm $$(docker ps -qa)