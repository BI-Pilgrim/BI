from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "Amazon_Ads_Data_Warehouse_Staging"
LOCATION = "asia-south1"

default_args = {
    'owner': 'aryan.kumar@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to create BigQuery insert job tasks
def create_query_task(query, task_id):
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

# DAG definition
with DAG(
    dag_id='Amazon_Ads_DW',
    default_args=default_args,
    description='DAG to append Amazon Ads data to BigQuery',
    schedule_interval='30 8 * * *',  # 2:00 PM IST (08:30 AM UTC)
    start_date=datetime(2025, 2, 5),  # Update this with the desired start date
    catchup=False,
) as dag:

    # List of table names
    tables = [
        "product_targetings", "product_ads", "profiles", "display_ad_groups", "brands_campaigns",
        "product_negative_keywords", "brands_keywords", "portfolios", "display_campaigns", "products_report_stream",
        "product_keywords", "display_budget_rules", "product_ad_groups", "brands_ad_groups", "brands_video_report_stream",
        "display_report_stream", "brands_v3_report_stream", "product_campaign", "brands_report_stream",
        "display_product_ads", "product_campaign_negative_keywords"
    ]

    # Create tasks for each table
    tasks = {}
    for table in tables:
        file_path = f'/home/airflow/gcs/dags/Amazon_Ads_Data_Warehouse/sql/amz_ads_to_bq/{table}/{table}_APPEND.sql'
        with open(file_path, 'r') as file:
            sql_query = file.read()

        task_id = f'{table}_Append'
        task = create_query_task(sql_query, task_id)
        tasks[task_id] = task

    # Example of adding a final sanity check task (using a predefined SQL query)
    #with open('../dags/Amazon_Ads_Data_Warehouse/sql/Amz_Ads_DW_sanity_check.sql', 'r') as file:
    #    sql_query_sanity_check = file.read()
#
    #task_sanity_check = create_query_task(sql_query_sanity_check, 'sanity_check')

    # Create dependencies (example: running all append tasks first, then sanity check)
    append_tasks = list(tasks.values())
    append_tasks 
