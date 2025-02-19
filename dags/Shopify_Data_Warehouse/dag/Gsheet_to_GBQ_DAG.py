# Import Functions
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import os
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


# Define default arguments for the DAG
default_args = {
    'owner': 'omkar.sadawarte@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define the DAG
with DAG(
    dag_id='gsheet_to_gbq_testing',
    default_args=default_args,
    description='A DAG to execute those SQL while which are referring to table connected with gsheet',
    schedule_interval='30 3 * * *',  # 3:30 AM UTC is 9:00 AM IST
    start_date=timezone.datetime(2025, 2, 18),
    catchup=False,
) as dag:

    # Start of the pipeline
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    
    # Test
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Order_item_master/Order_item_master_append.sql', 'r') as file:
        sql_query_10 = file.read()
        bigquery_task = BigQueryExecuteQueryOperator(
        task_id="bq_query",
        sql=sql_query_10,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",  # Ensure this is set correctly
        location="asia-south1",  # Change based on your dataset location
        impersonation_chain=["composer-bi-scheduling@shopify-pubsub-project.iam.gserviceaccount.com"]
    )


    # End of the pipeline
    finish_pipeline = DummyOperator(
        task_id='finish_pipeline'
    )


    start_pipeline >> bigquery_task >> finish_pipeline
