from airflow import DAG 
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator,BigQueryExecuteQueryOperator
from airflow.models import Variable

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "Data_Warehouse_Shopify_Staging"
LOCATION = "asia-south1"

default_args = {
    'owner': 'aryan.kumar@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
} 

with DAG(
    dag_id='Shopify_Retention11',
    default_args=default_args,
    description='Dag to append retention data to bigquery',
    schedule_interval='30 8 * * *',  # 8:00 AM IST (03:30 AM UTC)
    start_date=datetime(2025, 2, 5),  # Update this with the desired start date
    catchup=False,
) as dag: 

    # Load SQL queries from files
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/Overall_Cohort.sql', 'r') as file: 
        sql_query_1 = file.read()  
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/Skuwise_Retention_Cohort.sql', 'r') as file: 
        sql_query_2 = file.read() 
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/Per_Product_Retention.sql', 'r') as file: 
        sql_query_3 = file.read() 
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/Retention_KPI.sql', 'r') as file: 
        sql_query_4 = file.read()
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/Exact_Order_Count_KPI.sql', 'r') as file: 
        sql_query_5 = file.read() 
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/Retention_KPI_Meta_Spend_PP.sql', 'r') as file: 
        sql_query_6 = file.read() 
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/KPI_Order_Count.sql', 'r') as file: 
        sql_query_7 = file.read()
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/Retention_KPI_M0Cont.sql', 'r') as file: 
        sql_query_8 = file.read() 
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/Retention_KPI_bogo_dis.sql', 'r') as file: 
        sql_query_9 = file.read() 
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/Retention_KPI_crm_metrics.sql', 'r') as file: 
        sql_query_10 = file.read()  
    with open('/home/airflow/gcs/dags/Shopify_retention/sql/Retention_KPI_MAIN.sql', 'r') as file: 
        sql_query_11 = file.read() 

    # Define the queries to run
    def create_query_task(query, task_id):
        return BigQueryExecuteQueryOperator(
        task_id=task_id,
        gcp_conn_id="google_cloud_default",  # Ensure this is set correctly
        location=LOCATION,  # Change based on your dataset location
        impersonation_chain=["composer-bi-scheduling@shopify-pubsub-project.iam.gserviceaccount.com"],
        sql=query,
        use_legacy_sql=False,
    )

    # Create tasks for queries 1 to 10
    task_query_1 = create_query_task(sql_query_1, 'Shopify_Overall_Cohort')
    task_query_2 = create_query_task(sql_query_2, 'Shopify_Skuwise_Retention_Cohort')
    task_query_3 = create_query_task(sql_query_3, 'Shopify_Per_Product_Retention')
    task_query_4 = create_query_task(sql_query_4, 'Shopify_Retention_KPI')
    task_query_5 = create_query_task(sql_query_5, 'Shopify_Exact_Order_Count_KPI')
    task_query_6 = create_query_task(sql_query_6, 'Shopify_Retention_KPI_Meta_Spend_PP')
    task_query_7 = create_query_task(sql_query_7, 'Shopify_KPI_Order_Count')
    task_query_8 = create_query_task(sql_query_8, 'Shopify_Retention_KPI_M0Cont')
    task_query_9 = create_query_task(sql_query_9, 'Shopify_Retention_KPI_bogo_dis')
    task_query_10 = create_query_task(sql_query_10, 'Shopify_Retention_KPI_crm_metrics')

    
    task_query_11 = create_query_task(sql_query_11, 'Shopify_Retention_KPI_MAIN')

    [task_query_1,task_query_2, task_query_3 ,task_query_4, task_query_5,task_query_6, task_query_7, task_query_8,task_query_9,task_query_10] >>task_query_11
    

