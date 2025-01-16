
# Import Functions 
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
import subprocess

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

# Sanity check Table 

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Shopify_DW_Sanity_check.sql', 'r') as file:
        sql_query_32 = file.read()

    sanity_check = BigQueryInsertJobOperator(
        task_id='sanity_check',
        configuration={
            "query": {
                "query": sql_query_32,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )   

	

    def run_main_script():
        script_path = '/home/airflow/gcs/dags/Shopify_Data_Warehouse/python/Shopify_DW_Sanity_check_mail.py'
        try:
            # Use subprocess to run the Python script with the specified path
            result = subprocess.run(
                ['python', script_path],
                check=True,
                capture_output=True,
                text=True
            )
            # print("Script output:", result.stdout)
            # print("Script errors:", result.stderr)
        except subprocess.CalledProcessError as e:
            # print(f"Error occurred while running the script: {e}")
            # print(f"Command output: {e.stdout}")
            # print(f"Command errors: {e.stderr}")
            raise

# Define the PythonOperator to run the function
    run_python_task = PythonOperator(
        task_id='run_main_script',
        python_callable=run_main_script,
    )


start_pipeline >> sanity_check >> run_python_task
