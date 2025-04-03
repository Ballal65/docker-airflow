start:
	docker compose up --build -d
stop:
	docker compose down
restart:
	docker restart airflow-scheduler airflow-webserver
clean:
	docker rmi airflow-init airflow-scheduler airflow-webserver