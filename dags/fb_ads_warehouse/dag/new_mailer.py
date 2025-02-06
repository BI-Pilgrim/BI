from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Define default arguments for the DAG
default_args = {
    'owner': 'omkar.sadawarte@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['omkar.sadawarte@discoverpilgrim.com'],
}

# Define the DAG
with DAG(
    dag_id='new_mailer',
    default_args=default_args,
    description='Process TAT stats for orders and store in BigQuery',
    schedule_interval='0 9 * * *',  # 9:00 AM IST
    start_date=datetime(2024, 1, 19),
    catchup=False,
    max_active_runs=1,
    tags=['supply_chain', 'tat_analysis'],
) as dag:
    
    def run_fb_sanity_check():
        script_path = '"../dags/fb_ads_warehouse/python/fb_sanity_check.py"'
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

     # Task to run fb sanity check BigQuery tables
    sanity_check = PythonOperator(
        task_id='run_sanity_check',
        python_callable=run_fb_sanity_check,
    )

    run_fb_sanity_check