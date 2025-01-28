"""from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
from airflow.decorators import task, dag
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
    dag_id='amazon_scraping',
    default_args=default_args,
    description='A simple DAG to run scraping of Amaozon',
    schedule_interval='30 1 * * *',  # 3:30 AM UTC is 9:00 AM IST
    start_date=datetime(2024, 10, 16),  # Update this with the desired start date
    catchup=False,
) as dag:

# Define a function to execute the AZ_MS_face_serum.py script 
@dag("Amazon_scraping", schedule='30 1 * * *', start_date=datetime(year=2025,month=1,day=24))
def Amazon_scraping():
    script_path = 'gcs/dags/Amazon_Web_Scraping/python/AMZ_Review_Scraping_Top_Products_PlayWright.py'

    @task.bash
    def install_playwright_firefox():
        return "playwright install-deps && playwright install firefox"
    
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
    run_scrape_task = PythonOperator(
        task_id='Amazon_scraping',
        python_callable=Amazon_scraping,
    )

    install_playwright_firefox() >> run_scrape_task
dag = Amazon_scraping() """
