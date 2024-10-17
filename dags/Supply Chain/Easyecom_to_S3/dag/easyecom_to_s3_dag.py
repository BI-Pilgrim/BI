from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Define default arguments for the DAG
default_args = {
    'owner': 'rwitapa.mitra@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='trigger_copy_easyecom_to_s3',
    default_args=default_args,
    description='A simple DAG to copy invoices from EasyEcom to S3 every day at 9 AM',
    schedule_interval='30 3 * * *',  # 3:30 AM UTC is 9:00 AM IST
    start_date=datetime(2024, 10, 16),  # Update this with the desired start date
    catchup=False,
) as dag:

    # Define a function to execute the main.py script
    def run_main_script():
        script_path = '/home/airflow/gcs/dags/python/main.py'
        try:
            # Use subprocess to run the Python script with the specified path
            result = subprocess.run(
                ['python', script_path],
                check=True,
                capture_output=True,
                text=True
            )
            print("Script output:", result.stdout)
            print("Script errors:", result.stderr)
        except subprocess.CalledProcessError as e:
            print(f"Error occurred while running the script: {e}")
            print(f"Command output: {e.stdout}")
            print(f"Command errors: {e.stderr}")
            raise

    # Define the PythonOperator to run the function
    run_main_task = PythonOperator(
        task_id='run_main_script',
        python_callable=run_main_script,
    )

    run_main_task
