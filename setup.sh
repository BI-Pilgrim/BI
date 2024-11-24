#!/bin/bash
airflow db migrate
airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

# airflow webserver --port 8080 & airflow scheduler && fg
airflow standalone