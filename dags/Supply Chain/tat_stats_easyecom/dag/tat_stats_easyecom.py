from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Define default arguments for the DAG
default_args = {
    'owner': 'akash.banger@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['akash.banger@discoverpilgrim.com'],
}

# Define the DAG
with DAG(
    dag_id='tat_stats_easyecom',
    default_args=default_args,
    description='Process TAT stats for orders and store in BigQuery',
    schedule_interval='0 9 * * *',  # 9:00 AM IST
    start_date=datetime(2024, 1, 19),
    catchup=False,
    max_active_runs=1,
    tags=['supply_chain', 'tat_analysis'],
) as dag:

    def run_create_tables():
        script_path = '/home/airflow/gcs/dags/Supply Chain/tat_stats_easyecom/python/create_tables.py'
        try:
            result = subprocess.run(
                ['python', script_path],
                check=True,
                capture_output=True,
                text=True
            )
            print("Script output:", result.stdout)
            print("Script errors:", result.stderr)
        except subprocess.CalledProcessError as e:
            print(f"Error occurred while running create_tables script: {e}")
            print(f"Command output: {e.stdout}")
            print(f"Command errors: {e.stderr}")
            raise

    def run_process_data():
        script_path = '/home/airflow/gcs/dags/Supply Chain/tat_stats_easyecom/python/process_data.py'
        try:
            result = subprocess.run(
                ['python', script_path],
                check=True,
                capture_output=True,
                text=True
            )
            print("Script output:", result.stdout)
            print("Script errors:", result.stderr)
        except subprocess.CalledProcessError as e:
            print(f"Error occurred while running process_data script: {e}")
            print(f"Command output: {e.stdout}")
            print(f"Command errors: {e.stderr}")
            raise

    # Task to create BigQuery tables
    create_tables_task = PythonOperator(
        task_id='create_bigquery_tables',
        python_callable=run_create_tables,
    )

    # Task to process and store data
    process_data_task = PythonOperator(
        task_id='process_and_store_data',
        python_callable=run_process_data,
    )

    # Set task dependencies
    create_tables_task >> process_data_task