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


# Define constants
LOCATION = "US"  # Replace with your BigQuery dataset location (e.g., "US", "EU")
SQL_DIR = "/home/airflow/gcs/dags/Facebook_Ads_Performance_Report/SQL"  # Adjust this path if necessary
# SQL_DIR = "../dags/Facebook_Ads_Performance_Report/SQL"

# Define the DAG
with DAG(
    dag_id='Facebook_Ads_Performance_Report_DAG',
    default_args=default_args,
    description='A DAG to update the tables used in Facebook Ads Performance Report',
    schedule_interval='30 3 * * *',  # 3:30 AM UTC is 9:00 AM IST
    start_date=timezone.datetime(2025, 1, 3),
    catchup=False,
) as dag:

    # Start of the pipeline
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    # daily_ad_spend_and_revenue Table Refresh - Append
    daily_ad_spend_and_revenue_sql_path = os.path.join(SQL_DIR, "daily_ad_spend_and_revenue/daily_ad_spend_and_revenue_append.sql")
    with open(daily_ad_spend_and_revenue_sql_path, 'r') as file:
        sql_query_1 = file.read()

    Append_daily_ad_spend_and_revenue = BigQueryInsertJobOperator(
        task_id='Append_daily_ad_spend_and_revenue',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # FACEBOOK_ADS_SPEND_TIERS_NEW Table Refresh - Append
    FACEBOOK_ADS_SPEND_TIERS_NEW_sql_path = os.path.join(SQL_DIR, "FACEBOOK_ADS_SPEND_TIERS_NEW/FACEBOOK_ADS_SPEND_TIERS_NEW_APPEND.sql")
    with open(FACEBOOK_ADS_SPEND_TIERS_NEW_sql_path, 'r') as file:
        sql_query_2 = file.read()

    Append_FACEBOOK_ADS_SPEND_TIERS_NEW = BigQueryInsertJobOperator(
        task_id='Append_FACEBOOK_ADS_SPEND_TIERS_NEW',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )
    
    # Test
    test_sql_path = os.path.join(SQL_DIR, "test.sql")
    with open(test_sql_path, 'r') as file:
        sql_query_10 = file.read()
        
    #     test_new = BigQueryInsertJobOperator(
    #     task_id='test',
    #     configuration={
    #         "query": {
    #             "query": sql_query_10,
    #             "useLegacySql": False,
    #         },
    #         "location": LOCATION,
    #     }
    # )

        bigquery_task = BigQueryExecuteQueryOperator(
        task_id="bq_query",
        sql=sql_query_10,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",  # Ensure this is set correctly
        location="asia-south1"  # Change based on your dataset location
    )

################################################################################################################################################################

    # daily_ads_count Table Refresh - Append
    daily_ads_count_sql_path = os.path.join(SQL_DIR, "daily_ads_count/daily_ads_count_append.sql")
    with open(daily_ads_count_sql_path, 'r') as file:
        sql_query_3 = file.read()

    Append_daily_ads_count = BigQueryInsertJobOperator(
        task_id='Append_daily_ads_count',
        configuration={
            "query": {
                "query": sql_query_3,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # facebook_ads_daily_log Table Refresh - Append
    facebook_ads_daily_log_sql_path = os.path.join(SQL_DIR, "facebook_ads_daily_log/facebook_ads_daily_log_append.sql")
    with open(facebook_ads_daily_log_sql_path, 'r') as file:
        sql_query_4 = file.read()

    Append_facebook_ads_daily_log = BigQueryInsertJobOperator(
        task_id='Append_facebook_ads_daily_log',
        configuration={
            "query": {
                "query": sql_query_4,
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
   # start_pipeline >> Append_daily_ad_spend_and_revenue
   # Append_daily_ad_spend_and_revenue >> Append_FACEBOOK_ADS_SPEND_TIERS_NEW
   # Append_FACEBOOK_ADS_SPEND_TIERS_NEW >> [ Append_daily_ads_count, Append_facebook_ads_daily_log]
   # [ Append_daily_ads_count, Append_facebook_ads_daily_log] >> finish_pipeline



    start_pipeline >> bigquery_task >> finish_pipeline
