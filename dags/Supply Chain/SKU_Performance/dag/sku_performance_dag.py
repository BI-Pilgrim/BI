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
    dag_id='dag_test',
    default_args=default_args,
    description='Dag to append latest Sales data to bigquery',
    #schedule_interval='30 1 * * *',  # 3:30 AM UTC is 9:00 AM IST
    schedule_interval='15 11 * * *' , # 4:45 PM IST (11:15 AM UTC)

    start_date=datetime(2024, 11, 5),  # Update this with the desired start date
    catchup=False,
) as dag:

    
    def run_main_script():
        script_path = 'gcs/dags/Supply Chain/SKU_Performance/python/Extract_Sales.py' 
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

    run_test_task