
# Import Functions
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator

# Define the start date in UTC 
START_DATE = timezone.datetime(2024, 11, 7, 5, 30, 0, tzinfo=timezone.utc)  # Corresponds to 11 AM IST on 2024-11-7 

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "Pre_Analytics"
LOCATION = "US"  # Ensure this matches your dataset location
DATASET_STAGING = "Shopify_staging"


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
    dag_id='Google_Ads_retargetting_exclusion_Customers_1',
    schedule_interval='30 5 * * *',  # Cron expression for 11 AM IST (11:30 PM UTC)
    default_args=default_args,
    catchup=False
) as dag:
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )


    # Load SQL query from file
    with open('gcs/dags/D2C/Adhoc/Google_Ads_Retargeting/SQL/Google_Ads_exlusion_customers.sql', 'r') as file:
        sql_query_1 = file.read()

    Customers_list = BigQueryInsertJobOperator(
        task_id='latest_customers_list',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    ) 




    finish_pipeline = DummyOperator(
        task_id='finish_pipeline',
        dag=dag
    )

# Task Orchestration

start_pipeline >> Customers_list >> finish_pipeline
