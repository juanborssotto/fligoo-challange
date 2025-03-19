# fligoo-challange

## airflow
docker compose up airflow-init -d
docker compose up -d

http://localhost:8080/home
user pass airflow airflow

dag
http://localhost:8080/dags/insert-flights-dag/grid?search=insert-flights-dag

run it


jupyter
http://localhost:8889/lab?token=my-token

define .env
AIRFLOW_IMAGE_NAME="apache/airflow:2.10.5"
AIRFLOW_UID=50000
POSTGRES_PASSWORD="mypass"