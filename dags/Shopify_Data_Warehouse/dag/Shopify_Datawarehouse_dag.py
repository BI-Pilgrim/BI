# Import Functions
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator

# Define the start date in UTC 
START_DATE = timezone.datetime(2025, 1, 2, 7, 55, 0, tzinfo=timezone.utc)  # Corresponds to 1.15 PM IST on 2025-01-02

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "Data_Warehouse_Shopify_Staging"
LOCATION = "asia-south1"  # Ensure this matches your dataset location

DATASET_STAGING = "Data_Warehouse_Shopify_Staging"


default_args = {
    'owner': 'kajal.ray@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': START_DATE,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Shopify_Warehouse',
    schedule_interval='30 23 * * *',  # Cron expression for 5 AM IST (11:30 PM UTC)
    default_args=default_args,
    catchup=False
) as dag:
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

# Abandoned Checkout Staging Table Refresh - Append

    # Load SQL query from file
    with open('gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Abandoned_chekout/Abandoned_checkout_create.sql', 'r') as file:
        sql_query_1 = file.read()

    append_abandoned_checkout = BigQueryInsertJobOperator(
        task_id='append_abandoned_checkout',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

append_abandoned_checkout
