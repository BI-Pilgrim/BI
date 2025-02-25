

# Import Functions 
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator


# Define the start date in UTC 
START_DATE = timezone.datetime(2025, 2, 25, 7, 55, 0, tzinfo=timezone.utc)  # Corresponds to 1.15 PM IST on 2025-01-02
default_args = {
    'owner': 'shafiq@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': START_DATE,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Goonj_sync',
    schedule_interval='30 23 7 * *',  # Cron expression for 5 AM IST (11:30 PM UTC)
    default_args=default_args,
    catchup=False
) as dag:
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )
# Abandoned Checkout Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Project_Goonj/Monhtly_dashboard/Master_Query_all_in_one.sql', 'r') as file:
        sql_query_1 = file.read()
    master_query = BigQueryInsertJobOperator(
        task_id='Project_GOONJ_DAG',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

start_pipeline >> master_query
