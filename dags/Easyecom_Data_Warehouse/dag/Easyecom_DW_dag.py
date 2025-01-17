
# Import Functions 
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
import subprocess
from airflow.operators.python import PythonOperator

# Define the start date in UTC 
START_DATE = timezone.datetime(2025, 1, 10, 7, 55, 0, tzinfo=timezone.utc)  # Corresponds to 1.15 PM IST on 2025-01-02

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "easycom"
LOCATION = "asia-south1"  # Ensure this matches your dataset location

DATASET_STAGING = "Data_Warehouse_Easyecom_Staging"


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
    dag_id='Easyecom_DataWarehouse',
    schedule_interval='30 00 * * *',  # Cron expression for 6 AM IST (12:30 AM UTC)
    default_args=default_args,
    catchup=False
) as dag:
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

# Orders Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Easyecom_Data_Warehouse/sql/Orders/Order_Append.sql', 'r') as file:
        sql_query_1 = file.read()

    Orders_append = BigQueryInsertJobOperator(
        task_id='Orders_append',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

# Order item Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Easyecom_Data_Warehouse/sql/Order_items/Order_item_Append.sql', 'r') as file:
        sql_query_2 = file.read()

    Orderitem_append = BigQueryInsertJobOperator(
        task_id='Orderitem_append',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )    


# Sanity Check of all table
    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Easyecom_Data_Warehouse/sql/Easyecom_DW_sanity_check.sql', 'r') as file:
        sql_query_3 = file.read()

        DW_Sanity_check = BigQueryInsertJobOperator(
        task_id='DW_Sanity_check',
        configuration={
            "query": {
                "query": sql_query_3,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )  
        
    def run_main_script():
        script_path = '/home/airflow/gcs/dags/Easyecom_Data_Warehouse/dag/Easyecom_DW_dag.py'
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


#    finish_pipeline = DummyOperator(
#        task_id='finish_pipeline',
#        dag=dag
#    )

start_pipeline >> Orders_append >> Orderitem_append >> DW_Sanity_check >> run_python_task
