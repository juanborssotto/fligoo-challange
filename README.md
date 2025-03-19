# Fligoo Challange

## Set up steps

### Settings up directories
```
mkdir apache-airflow
mkdir apache-airflow/config
mkdir apache-airflow/dags
mkdir apache-airflow/logs
mkdir apache-airflow/plugins
cp insert-flights-dag.py apache-airflow/dags/
mkdir jupyter-volume
cp read_database.ipynb jupyter-volume/
```

### Setting up environment variables
#### Create .env file in the root directory with the following attributes
```
AIRFLOW_IMAGE_NAME="apache/airflow:2.10.5"
AIRFLOW_UID=50000
POSTGRES_PASSWORD="my_pass"
AVIATIONSTACK_API_KEY="my_api_key"
```

### Running the docker services
```
docker compose up airflow-init -d
docker compose up -d
```

### Running the tasks
#### Airflow
```
1) Enter: http://localhost:8080/dags/insert-flights-dag/grid?search=insert-flights-dag
2) Login with username airflow password airflow
3) Run the Dag
```

#### Jupyter
```
1) Enter: http://localhost:8889/lab?token=my-token
2) Move to file work/read_database.ipynb
3) Run it
```
