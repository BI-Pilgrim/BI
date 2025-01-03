# Import Functions
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'rwitapa.mitra@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define constants
LOCATION = "US"  # Replace with your BigQuery dataset location (e.g., "US", "EU")
SQL_DIR = "gcs/dags/Google_Ads_Warehouse/sql/GoogleAds_to_bq"  # Adjust this path if necessary

# Define the DAG
with DAG(
    dag_id='google_ads_warehouse',
    default_args=default_args,
    description='A DAG to copy invoices from EasyEcom to S3 and update Google Ads tables.',
    schedule_interval='30 3 * * *',  # 3:30 AM UTC is 9:00 AM IST
    start_date=timezone.datetime(2025, 1, 3),
    catchup=False,
) as dag:

    # Start of the pipeline
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    # account_performance_report Table Refresh - Append
    account_performance_report_sql_path = os.path.join(SQL_DIR, "account_performance_report/account_performance_report_append.sql")
    with open(account_performance_report_sql_path, 'r') as file:
        sql_query_1 = file.read()

    append_account_performance_report = BigQueryInsertJobOperator(
        task_id='append_account_performance_report',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group Staging Table Refresh - Append
    ad_group_sql_path = os.path.join(SQL_DIR, "ad_group/ad_group_append.sql")
    with open(ad_group_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group = BigQueryInsertJobOperator(
        task_id='append_ad_group',
        configuration={
            "query": {
                "query": sql_query_2,
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
    start_pipeline >> [append_account_performance_report, append_ad_group]
    [append_account_performance_report, append_ad_group] >> finish_pipeline
