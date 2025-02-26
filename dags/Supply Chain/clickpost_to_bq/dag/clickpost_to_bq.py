from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

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
    dag_id='clickpost_to_bq',
    default_args=default_args,
    description='Fetch Clickpost data and store in BigQuery',
    schedule_interval='0 5 * * *',  # 5:00 AM IST
    start_date=datetime(2024, 2, 17),
    catchup=False,
    max_active_runs=1,
    tags=['supply_chain', 'clickpost'],
) as dag:

    def run_fetch_data():
        script_path = 'gcs/dags/Supply Chain/clickpost_to_bq/python/fetch_data.py'
        try:
            result = subprocess.run(
                ['python', script_path],
                check=True,
                capture_output=True,
                text=True,
                env={**os.environ}  # Pass current environment variables
            )
            print("Script output:", result.stdout)
            print("Script errors:", result.stderr)
        except subprocess.CalledProcessError as e:
            print(f"Error occurred while running fetch_data script: {e}")
            print(f"Command output: {e.stdout}")  # Add this to see the actual error
            print(f"Command errors: {e.stderr}")  # Add this to see the actual error
            raise

    # Task to fetch and store data
    fetch_data_task = PythonOperator(
        task_id='fetch_and_store_data',
        python_callable=run_fetch_data,
    )

    fetch_data_task