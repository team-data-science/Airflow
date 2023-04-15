# Airflow
 Airflow Course

## Setting the right Airflow user
As outlined in the [Apache Airflow Documentation](URL "https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#initializing-environment"), to prevent 
permission-related problems **when executing on a Linux distribution**
execute the following command prior to using docker compose: 

echo -e "AIRFLOW_UID"=$(id -u) > .env

## Python packages in WSL
pip install apache-airflow

## Portainer
Port:9000
Username/Password: admin/password

## Adminer
connection: postgres:5432
Username/Password: airflow/airflow

## Postgres
- create WeatherData database
- create Temperature table without defining schema


