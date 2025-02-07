# Import Functions
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import timezone 

# Import the function from the external script correctly
import sys
sys.path.append('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/python')  # Add the correct path
from Amazon_Seller_DW_Sanity_check_mail import send_sanity_check_email  # Import the function

# Define DAG default arguments
default_args = {
    'owner': 'kajal.ray@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': timezone.datetime(2025, 1, 13, 7, 55, 0, tzinfo=timezone.utc),
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Amazon_Seller',
    schedule_interval='30 23 * * *',  # Cron expression for 11:30 PM UTC (5 AM IST)
    default_args=default_args,
    catchup=False
) as dag:
    
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    # Load and execute BigQuery queries as tasks
    def create_bigquery_task(task_id, file_path):
        with open(file_path, 'r') as file:
            query = file.read()
        return BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                    "location": "asia-south1",
                }
            }
        )

    append_all_orders_data_last_update_general = create_bigquery_task(
        "append_all_orders_data_last_update_general",
        "/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL/ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL_append.sql"
    )

    append_all_orders_data_order_date_general = create_bigquery_task(
        "append_all_orders_data_order_date_general",
        "/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL/ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_append.sql"
    )

    append_merchant_cancelled_listings_data = create_bigquery_task(
        "append_merchant_cancelled_listings_data",
        "/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/MERCHANT_CANCELLED_LISTINGS_DATA/MERCHANT_CANCELLED_LISTINGS_DATA_append.sql"
    )

    append_merchant_listings_all_data = create_bigquery_task(
        "append_merchant_listings_all_data",
        "/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/MERCHANT_LISTINGS_ALL_DATA/MERCHANT_LISTINGS_ALL_DATA_append.sql"
    )

    append_orders = create_bigquery_task(
        "append_orders",
        "/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/Orders/Orders_append.sql"
    )

    append_sales_and_traffic_report = create_bigquery_task(
        "append_SALES_AND_TRAFFIC_REPORT",
        "/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/Sales and Traffic Report/SALES_AND_TRAFFIC_REPORT_append.sql"
    )

    sanity_check = create_bigquery_task(
        "sanity_check",
        "/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/Amazon_Seller_DW_Sanity_check.sql"
    )

    # Define PythonOperator to run the function from external script
    run_python_task = PythonOperator(
        task_id='run_main_script',
        python_callable=send_sanity_check_email  # Only executes when DAG runs
    )

    # Set dependencies for DAG execution
    start_pipeline >> [
        append_all_orders_data_last_update_general,
        append_all_orders_data_order_date_general,
        append_merchant_cancelled_listings_data,
        append_merchant_listings_all_data,
        append_orders,
        append_sales_and_traffic_report
    ] >> sanity_check >> run_python_task
