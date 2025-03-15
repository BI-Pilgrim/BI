# Import Functions
from datetime import timedelta
from airflow import DAG
import subprocess
import sys
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator,BigQueryExecuteQueryOperator
import os
from fb_ads_warehouse.python.FB_DW_Sanity_Check_mail import send_sanity_check_email  # Import the function from the script
LOCATION = "US"

# SQL_DIR = "../dags/FB_ADS_PERF_REPORT/SQL"
SQL_DIR = "/home/airflow/gcs/dags/FB_ADS_PERF_REPORT/SQL"

# Define the start date in UTC 
START_DATE = timezone.datetime(2025, 3, 4, 23, 30, 0, tzinfo=timezone.utc)  # Corresponds to 5:00 AM IST on 2025-03-06

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "shopify-pubsub-project.Facebook_Ads_Performance_Report"
LOCATION = "asia-south1"  # Ensure this matches your dataset location

default_args = {
    'owner': 'omkar.sadawarte@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': START_DATE,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='FB_ADS_PERF_REPORT',
    schedule_interval='30 23 * * *',  # Cron expression for 5 AM IST (11:30 PM UTC)
    default_args=default_args,
    catchup=False
) as dag:
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

    # Final_Report2 Table Refresh - CREATE OR REPLACE
    Final_Report2_sql_path = os.path.join(SQL_DIR, "Final_Report2_with_days2death.sql")
    with open(Final_Report2_sql_path, 'r') as file:
        sql_query_1 = file.read()

    CREATE_REPLACE_Final_Report2 = BigQueryExecuteQueryOperator(
        task_id='CREATE_REPLACE_Final_Report2',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # facebook_ads_daily_tier_count_log Table Refresh - APPEND / UPDATE
    facebook_ads_daily_tier_count_log_sql_path = os.path.join(SQL_DIR, "facebook_ads_daily_tier_count_log.sql")
    with open(facebook_ads_daily_tier_count_log_sql_path, 'r') as file:
        sql_query_1 = file.read()

    APPEND_facebook_ads_daily_tier_count_log = BigQueryExecuteQueryOperator(
        task_id='APPEND_facebook_ads_daily_tier_count_log',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # End of the pipeline
    finish_pipeline = DummyOperator(
        task_id='finish_pipeline'
    )

    # Task Orchestration
    start_pipeline >> CREATE_REPLACE_Final_Report2
    CREATE_REPLACE_Final_Report2 >> APPEND_facebook_ads_daily_tier_count_log
    APPEND_facebook_ads_daily_tier_count_log >> finish_pipeline
