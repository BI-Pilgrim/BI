from airflow import DAG 
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator 
from airflow.models import Variable  


GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "Data_Warehouse_Shopify_Staging"
LOCATION = "asia-south1"


default_args = {
    'owner': 'shivam.kulshreshtha@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
} 

with DAG(
    dag_id='flipkart_Seller_emr_dag',
    default_args=default_args,
    description='Dag to append earn_more_report data to bigquery',
    #schedule_interval='30 1 * * *',  # 3:30 AM UTC is 9:00 AM IST
    schedule_interval='0 7 * * *',  # 8:00 AM IST (03:30 AM UTC)

    start_date=datetime(2025, 2, 5),  # Update this with the desired start date
    catchup=False,
) as dag: 
    
    with open('../dags/Flipkart_seller/sql/filpkart_Seller_earn_more_report_append.sql', 'r') as file: 
        sql_query_1 = file.read() 



    flipkart_seller_emr = BigQueryInsertJobOperator(
        task_id='flipkart_Seller_emr_dag',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    ) 


flipkart_seller_emr

        