from datetime import datetime, timedelta
from airflow import DAG
import subprocess
import sys
from airflow.utils.dates import days_ago, timezone
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import  BigQueryInsertJobOperator
import os 
from Clickpost_Data_Warehouse.python.Clickpost_DW_sanity_check_mail import send_sanity_check_email

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "Data_Warehouse_ClickPost_Staging"
LOCATION = "US" 

default_args = {
    'owner': 'aryan.kumar@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
} 

with DAG(
    dag_id='Clickpost_dw',
    default_args=default_args,
    description='Dag to append retention data to bigquery',
    schedule_interval='30 8 * * *',  # 2:00 PM IST (08:30 AM UTC)
    start_date=datetime(2025, 2, 5),  # Update this with the desired start date
    catchup=False,
) as dag: 
    

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
    
    with open('../dags/Clickpost_Data_Warehouse/sql/clickpost_to_bq/orders/Clickpost_orders_Append.sql', 'r') as file: 
        sql_query_1 = file.read() 
    
    task_query_1 = create_query_task(sql_query_1, 'orders_Append') 

    with open('../dags/Clickpost_Data_Warehouse/sql/clickpost_to_bq/addresses/Clickpost_addresses_Append.sql', 'r') as file: 
        sql_query_2 = file.read() 

    task_query_2 = create_query_task(sql_query_2, 'addresses_Append') 

    with open('../dags/Clickpost_Data_Warehouse/sql/clickpost_to_bq/package/Clickpost_package_Append.sql', 'r') as file: 
        sql_query_3 = file.read() 

    task_query_3 = create_query_task(sql_query_3, 'package_Append') 

    with open('../dags/Clickpost_Data_Warehouse/sql/clickpost_to_bq/shipping/Clickpost_shipping_Append.sql', 'r') as file: 
        sql_query_4 = file.read() 

    task_query_4 = create_query_task(sql_query_4, 'shipping_Append') 

    with open('../dags/Clickpost_Data_Warehouse/sql/clickpost_to_bq/tracking/Clickpost_tracking_Append.sql', 'r') as file: 
        sql_query_5 = file.read() 

    task_query_5 = create_query_task(sql_query_5, 'tracking_Append') 

    with open('../dags/Clickpost_Data_Warehouse/sql/Clickpost_DW_sanity_check.sql', 'r') as file: 
        sql_query_6 = file.read() 

    task_query_6 = create_query_task(sql_query_6, 'sanity_check') 

    run_python_task = PythonOperator(
        task_id='run_main_script',
        python_callable=send_sanity_check_email,  # Call the function here
        ) 
    
    [task_query_1,task_query_2, task_query_3 ,task_query_4, task_query_5]>> task_query_6>>run_python_task




    
    