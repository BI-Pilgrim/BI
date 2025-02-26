from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import subprocess
import os
from typing import List
import pendulum

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

def generate_date_ranges() -> List[dict]:
    """Generate list of date ranges for specific backfill period"""
    start_date = datetime(2024, 11, 27)
    end_date = datetime(2025, 1, 1)
    date_ranges = []
    
    current_date = start_date
    while current_date <= end_date:
        date_ranges.append({
            'start_time': f"{current_date.strftime('%Y-%m-%d')} 00:00:00",
            'end_time': f"{current_date.strftime('%Y-%m-%d')} 23:59:59",
            'date_str': current_date.strftime('%Y_%m_%d')
        })
        current_date += timedelta(days=1)
    
    return date_ranges

def run_fetch_data_for_date(start_time: str, end_time: str, **context):
    """Run the fetch data script for a specific date range"""
    script_path = 'gcs/dags/Supply Chain/clickpost_to_bq/python/fetch_data.py'
    
    # Add date range to environment variables
    env = {
        **os.environ,
        'CLICKPOST_START_TIME': start_time,
        'CLICKPOST_END_TIME': end_time
    }
    
    try:
        result = subprocess.run(
            ['python', script_path],
            check=True,
            capture_output=True,
            text=True,
            env=env
        )
        print(f"Processing data for period: {start_time} to {end_time}")
        print("Script output:", result.stdout)
        if result.stderr:
            print("Script errors:", result.stderr)
            
    except subprocess.CalledProcessError as e:
        print(f"Error processing data for period: {start_time} to {end_time}")
        print(f"Command output: {e.stdout}")
        print(f"Command errors: {e.stderr}")
        raise

# Define the DAG
with DAG(
    dag_id='clickpost_backfill',
    default_args=default_args,
    description='Backfill Clickpost data from Nov 27, 2024 to Jan 1, 2025',
    schedule_interval=None,  # Manual trigger only
    start_date=pendulum.datetime(2024, 11, 27, tz="UTC"),  # Set to one day before backfill start
    catchup=False,
    max_active_runs=1,
    concurrency=3,  # Process 3 days simultaneously
    tags=['supply_chain', 'clickpost', 'backfill'],
) as dag:
    
    date_ranges = generate_date_ranges()
    
    # Create tasks in groups of 10 days each for better organization
    for group_num in range(0, len(date_ranges), 10):
        group_dates = date_ranges[group_num:group_num + 10]
        
        with TaskGroup(group_id=f'process_days_{group_num + 1}_to_{group_num + len(group_dates)}') as task_group:
            for date_range in group_dates:
                PythonOperator(
                    task_id=f'fetch_data_{date_range["date_str"]}',
                    python_callable=run_fetch_data_for_date,
                    op_kwargs={
                        'start_time': date_range['start_time'],
                        'end_time': date_range['end_time']
                    }
                ) 