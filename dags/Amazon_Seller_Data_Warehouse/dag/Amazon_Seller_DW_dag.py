# Import Functions
from datetime import timedelta
from airflow import DAG
import subprocess
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator

# Add the path where Amazon_Seller_DW_Sanity_check_mail.py is located
sys.path.append('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/python')

from Amazon_Seller_DW_Sanity_check_mail import send_sanity_check_email  # Import the function from the script


# Define the start date in UTC 
START_DATE = timezone.datetime(2025, 1, 13, 7, 55, 0, tzinfo=timezone.utc)  # Corresponds to 1.15 PM IST on 2025-01-02

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "Data_Warehouse_Amazon_Seller_Staging"
LOCATION = "asia-south1"  # Ensure this matches your dataset location

DATASET_STAGING = "Data_Warehouse_Amazon_Seller_Staging"


default_args = {
    'owner': 'kajal.ray@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': START_DATE,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Amazon_Seller',
    schedule_interval='30 23 * * *',  # Cron expression for 5 AM IST (11:30 PM UTC)
    default_args=default_args,
    catchup=False
) as dag:
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )


  
# ALL ORDERS DATA BY LAST UPDATE GENERAL Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL/ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL_append.sql', 'r') as file:
        sql_query_1 = file.read()

    append_all_orders_data_last_update_general = BigQueryInsertJobOperator(
        task_id='append_all_orders_data_last_update_general',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )




# ALL ORDERS DATA BY ORDER DATE GENERAL Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL/ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_append.sql', 'r') as file:
        sql_query_2 = file.read()

    append_all_orders_data_order_date_general = BigQueryInsertJobOperator(
        task_id='append_all_orders_data_order_date_general',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# MERCHANT CANCELLED LISTINGS DATA Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/MERCHANT_CANCELLED_LISTINGS_DATA/MERCHANT_CANCELLED_LISTINGS_DATA_append.sql', 'r') as file:
        sql_query_3 = file.read()

    append_merchant_cancelled_listings_data = BigQueryInsertJobOperator(
        task_id='append_merchant_cancelled_listings_data',
        configuration={
            "query": {
                "query": sql_query_3,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

# MERCHANT LISTINGS ALL DATA Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/MERCHANT_LISTINGS_ALL_DATA/MERCHANT_LISTINGS_ALL_DATA_append.sql', 'r') as file:
        sql_query_4 = file.read()

    append_merchant_listings_all_data = BigQueryInsertJobOperator(
        task_id='append_merchant_listings_all_data',
        configuration={
            "query": {
                "query": sql_query_4,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# MERCHANT LISTINGS DATA Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/MERCHANT_LISTINGS_DATA/MERCHANT_LISTINGS_DATA_append.sql', 'r') as file:
        sql_query_5 = file.read()

    append_merchant_listings_data = BigQueryInsertJobOperator(
        task_id='append_merchant_listings_data',
        configuration={
            "query": {
                "query": sql_query_5,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# MERCHANT LISTINGS DATA BACK COMPAT Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/MERCHANT_LISTINGS_DATA_BACK_COMPAT/MERCHANT_LISTINGS_DATA_BACK_COMPAT_append.sql', 'r') as file:
        sql_query_6 = file.read()

    append_merchant_listings_data_back_compat = BigQueryInsertJobOperator(
        task_id='append_merchant_listings_data_back_compat',
        configuration={
            "query": {
                "query": sql_query_6,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# MERCHANT LISTINGS INACTIVE DATA Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/MERCHANT_LISTINGS_INACTIVE_DATA/MERCHANT_LISTINGS_INACTIVE_DATA_append.sql', 'r') as file:
        sql_query_7 = file.read()

    append_merchant_listings_inactive_data = BigQueryInsertJobOperator(
        task_id='append_merchant_listings_inactive_data',
        configuration={
            "query": {
                "query": sql_query_7,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# OPEN LISTINGS DATA Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/OPEN_LISTINGS_DATA/OPEN_LISTINGS_DATA_append.sql', 'r') as file:
        sql_query_8 = file.read()

    append_open_listings_data = BigQueryInsertJobOperator(
        task_id='append_open_listings_data',
        configuration={
            "query": {
                "query": sql_query_8,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    
)

# Order Items Staging Table Refresh - Append

    # Load SQL query from file
   # with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/Order Items/Order_items_append.sql', 'r') as file:
    #    sql_query_9 = file.read()

    #append_order_items = BigQueryInsertJobOperator(
    #    task_id='append_order_items',
     #   configuration={
      #      "query": {
       #         "query": sql_query_9,
        #        "useLegacySql": False,
         #       "location": LOCATION,
          #  }
        #}
    #)


# Orders Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/Orders/Orders_append.sql', 'r') as file:
        sql_query_10 = file.read()

    append_orders = BigQueryInsertJobOperator(
        task_id='append_orders',
        configuration={
            "query": {
                "query": sql_query_10,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# XML_BROWSE_TREE_DATA Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/XML_BROWSE_TREE_DATA/XML_BROWSE_TREE_DATA_append.sql', 'r') as file:
        sql_query_11 = file.read()

    append_XML_BROWSE_TREE_DATA = BigQueryInsertJobOperator(
        task_id='append_XML_BROWSE_TREE_DATA',
        configuration={
            "query": {
                "query": sql_query_11,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

# SALES_AND_TRAFFIC_REPORT Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/Sales and Traffic Report/SALES_AND_TRAFFIC_REPORT_append.sql', 'r') as file:
        sql_query_12 = file.read()

    append_SALES_AND_TRAFFIC_REPORT = BigQueryInsertJobOperator(
        task_id='append_SALES_AND_TRAFFIC_REPORT',
        configuration={
            "query": {
                "query": sql_query_12,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Sanity check Table 

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Amazon_Seller_Data_Warehouse/sql/Amazon_Seller_DW_Sanity_check.sql', 'r') as file:
        sql_query_13 = file.read()

    sanity_check = BigQueryInsertJobOperator(
        task_id='sanity_check',
        configuration={
            "query": {
                "query": sql_query_13,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )   

    

run_python_task = PythonOperator(
    task_id='run_main_script',
    python_callable=send_sanity_check_email,  # Call the function here
)

start_pipeline >> [append_all_orders_data_last_update_general,append_all_orders_data_order_date_general,append_merchant_cancelled_listings_data,append_merchant_listings_all_data,append_merchant_listings_data,append_merchant_listings_data_back_compat,append_merchant_listings_inactive_data,append_open_listings_data,append_orders,append_XML_BROWSE_TREE_DATA,append_SALES_AND_TRAFFIC_REPORT] >> sanity_check >> run_python_task 
