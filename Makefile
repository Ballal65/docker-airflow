start:
	docker compose up --build -d

restart:
	docker restart airflow-scheduler airflow-webserver