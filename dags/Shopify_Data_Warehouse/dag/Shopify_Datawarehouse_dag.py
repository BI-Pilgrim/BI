
# Import Functions
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator

# Define the start date in UTC 
START_DATE = timezone.datetime(2025, 01, 02, 07, 45, 0, tzinfo=timezone.utc)  # Corresponds to 1.15 PM IST on 2025-01-02

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "Data_Warehouse_Shopify_Staging"
LOCATION = "asia-south1"  # Ensure this matches your dataset location

DATASET_STAGING = "Data_Warehouse_Shopify_Staging"


default_args = {
    'owner': 'rwitapa.mitra@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': START_DATE,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Shopify_Warehouse',
    schedule_interval='30 23 * * *',  # Cron expression for 5 AM IST (11:30 PM UTC)
    default_args=default_args,
    catchup=False
) as dag:
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

# Abandoned Checkout Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Abandoned_chekout/Abandoned_checkout_create.sql', 'r') as file:
        sql_query_1 = file.read()

    append_abandoned_checkout = BigQueryInsertJobOperator(
        task_id='append_abandoned_checkout',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

 


# Metafield Customer Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Metafield_Customer/Metafield_customer_append.sql', 'r') as file:
        sql_query_2 = file.read()

    append_metafield_customers = BigQueryInsertJobOperator(
        task_id='append_metafield_customers',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

    

# Discount Code Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Discount_code/Discount_code_append.sql', 'r') as file:
        sql_query_3 = file.read()

    append_discount_code = BigQueryInsertJobOperator(
        task_id='append_discount_code',
        configuration={
            "query": {
                "query": sql_query_3,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

    

# Customer Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Customers/Customer_append.sql', 'r') as file:
        sql_query_4 = file.read()

    append_customer = BigQueryInsertJobOperator(
        task_id='append_customer',
        configuration={
            "query": {
                "query": sql_query_4,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

    



# Orders Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Orders/Orders_append.sql', 'r') as file:
        sql_query_5 = file.read()

    append_order = BigQueryInsertJobOperator(
        task_id='append_order',
        configuration={
            "query": {
                "query": sql_query_5,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

    

# Transaction Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Transaction/Transaction_append.sql', 'r') as file:
        sql_query_6 = file.read()

    append_transaction = BigQueryInsertJobOperator(
        task_id='append_transaction',
        configuration={
            "query": {
                "query": sql_query_6,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

    


# Draft Item Staging Table Refresh - Append

    # Load SQL query from file
   # with open('/home/airflow/gcs/dags/sql/Draft_order_item_append.sql', 'r') as file:
 #       sql_query_7 = file.read()

 #   append_draft_order_item = BigQueryInsertJobOperator(
  #      task_id='append_draft_order_item',
   #     configuration={
    #        "query": {
     #           "query": sql_query_7,
      #         "location": LOCATION,
       #     }
       # }
    #)

    


# Draft Order Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Draft_orders/Draft_order_append.sql', 'r') as file:
        sql_query_8 = file.read()

    append_draft_order = BigQueryInsertJobOperator(
        task_id='append_draft_order',
        configuration={
            "query": {
                "query": sql_query_8,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

    
# Fulfilment Staging Table Refresh - Append

    # Load SQL query from file
#    with open('/home/airflow/gcs/dags/sql/Fulfillment_append.sql', 'r') as file:
#        sql_query_9 = file.read()
#
 #   append_fulfilment = BigQueryInsertJobOperator(
  #      task_id='append_fulfilment',
  #      configuration={
  #          "query": {
  #             "query": sql_query_9,
  #              "useLegacySql": False,
  #              "location": LOCATION,
  #          }
  #      }
  #  )

    



    

# Refund Order Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Refund_Orders/Refund_Orders_append.sql', 'r') as file:
        sql_query_11 = file.read()

    append_refund_order_item = BigQueryInsertJobOperator(
        task_id='append_refund_order_item',
        configuration={
            "query": {
                "query": sql_query_11,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

    



    finish_pipeline = DummyOperator(
        task_id='finish_pipeline',
        dag=dag
    )

# Task Orchestration

start_pipeline >> [append_order, append_transaction,append_abandoned_checkout,append_discount_code,append_customer,append_metafield_customers,append_draft_order,append_refund_order_item]



#append_refund_order_item >> check_refund_order_item_dataset
#append_order_item >> check_order_item_dataset
#append_fulfilment >> check_fulfilment_dataset
#append_draft_order_item >> check_draft_order_item_dataset
#append_draft_order >> check_draft_order_dataset
#append_metafield_customers >> check_metafield_customers_dataset
#append_discount_code >> check_discount_code_dataset
#append_abandoned_checkout >> check_abandoned_checkout_dataset
#append_customer >> check_customers_dataset
#append_order >> check_order_dataset
#append_transaction >> check_transaction_dataset




append_order >> finish_pipeline
append_transaction >> finish_pipeline
append_abandoned_checkout >> finish_pipeline
append_discount_code >> finish_pipeline
append_customer >> finish_pipeline
append_metafield_customers >> finish_pipeline
append_draft_order >> finish_pipeline
append_refund_order_item >> finish_pipeline

