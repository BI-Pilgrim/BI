

# Import Functions 
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
import subprocess

# Define the start date in UTC 
START_DATE = timezone.datetime(2025, 1, 2, 7, 55, 0, tzinfo=timezone.utc)  # Corresponds to 1.15 PM IST on 2025-01-02

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "Data_Warehouse_Shopify_Staging"
LOCATION = "asia-south1"  # Ensure this matches your dataset location

DATASET_STAGING = "Data_Warehouse_Shopify_Staging"


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
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Abandoned_chekout/Abandoned_checkout_append.sql', 'r') as file:
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


# Draft Order Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Draft_orders/Draft_order_append.sql', 'r') as file:
        sql_query_7 = file.read()

    append_draft_order = BigQueryInsertJobOperator(
        task_id='append_draft_order',
        configuration={
            "query": {
                "query": sql_query_7,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

# Draft Order Items Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Draft_Order_items/Draft_order_item_append.sql', 'r') as file:
        sql_query_31 = file.read()

    append_draft_order_items = BigQueryInsertJobOperator(
        task_id='append_draft_order_items',
        configuration={
            "query": {
                "query": sql_query_31,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

# Refund Order Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Refund_Orders/Refund_Orders_append.sql', 'r') as file:
        sql_query_8 = file.read()

    append_refund_order = BigQueryInsertJobOperator(
        task_id='append_refund_order',
        configuration={
            "query": {
                "query": sql_query_8,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


#Metafield Order Staging Table Refresh - Append

   # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Metafield_orders/Metafield_order_append.sql', 'r') as file:
        sql_query_9 = file.read()

    append_metafield_order = BigQueryInsertJobOperator(
        task_id='append_metafield_order',
        configuration={
            "query": {
                "query": sql_query_9,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

# Customer Address Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Customer_Address/Customer_Address_append.sql', 'r') as file:
        sql_query_10 = file.read()

    append_customer_address = BigQueryInsertJobOperator(
        task_id='append_customer_address',
        configuration={
            "query": {
                "query": sql_query_10,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Collections Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Collections/Collections_append.sql', 'r') as file:
        sql_query_11 = file.read()

    append_collections = BigQueryInsertJobOperator(
        task_id='append_collections',
        configuration={
            "query": {
                "query": sql_query_11,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Metafield Collections Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Metafield_Collections/Metafield_collections_append.sql', 'r') as file:
        sql_query_12 = file.read()

    append_metafield_collections = BigQueryInsertJobOperator(
        task_id='append_metafield_collections',
        configuration={
            "query": {
                "query": sql_query_12,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Inventory Level Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Inventory_level/Inventory_level_append.sql', 'r') as file:
        sql_query_13 = file.read()

    append_inventory_level = BigQueryInsertJobOperator(
        task_id='append_inventory_level',
        configuration={
            "query": {
                "query": sql_query_13,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Pages Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Pages/Pages_append.sql', 'r') as file:
        sql_query_14 = file.read()

    append_pages = BigQueryInsertJobOperator(
        task_id='append_pages',
        configuration={
            "query": {
                "query": sql_query_14,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Metafield Pages Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Metafield_pages/Metafield_pages_append.sql', 'r') as file:
        sql_query_15 = file.read()

    append_metafield_pages = BigQueryInsertJobOperator(
        task_id='append_metafield_pages',
        configuration={
            "query": {
                "query": sql_query_15,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Locations Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Locations/Locations_append.sql', 'r') as file:
        sql_query_16 = file.read()

    append_locations = BigQueryInsertJobOperator(
        task_id='append_locations',
        configuration={
            "query": {
                "query": sql_query_16,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Articles Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Articles/Articles_append.sql', 'r') as file:
        sql_query_17 = file.read()

    append_articles = BigQueryInsertJobOperator(
        task_id='append_articles',
        configuration={
            "query": {
                "query": sql_query_17,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Product Variants Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Product_variants/Product_variants_append.sql', 'r') as file:
        sql_query_18 = file.read()

    append_product_variants = BigQueryInsertJobOperator(
        task_id='append_product_variants',
        configuration={
            "query": {
                "query": sql_query_18,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

# Metafield Articles Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Metafield_Articles/Metafield_articles_append.sql', 'r') as file:
        sql_query_19 = file.read()

    append_metafield_articles = BigQueryInsertJobOperator(
        task_id='append_metafield_articles',
        configuration={
            "query": {
                "query": sql_query_19,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Inventory Items Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Inventory_items/Inventory_items_append.sql', 'r') as file:
        sql_query_20 = file.read()

    append_inventory_items = BigQueryInsertJobOperator(
        task_id='append_inventory_items',
        configuration={
            "query": {
                "query": sql_query_20,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )



# Products Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Products/Products_append.sql', 'r') as file:
        sql_query_21 = file.read()

    append_products = BigQueryInsertJobOperator(
        task_id='append_products',
        configuration={
            "query": {
                "query": sql_query_21,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Customer Journey Summary Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Customer_journey_summary/Customer_journey_summary_append.sql', 'r') as file:
        sql_query_22 = file.read()

    append_customer_journey_summary = BigQueryInsertJobOperator(
        task_id='append_customer_journey_summary',
        configuration={
            "query": {
                "query": sql_query_22,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )



# Metafield Product Variants Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Metafield_Product_Variants/Metafield_product_variants_append.sql', 'r') as file:
        sql_query_23 = file.read()

    append_Metafield_Product_Variants = BigQueryInsertJobOperator(
        task_id='append_Metafield_Product_Variants',
        configuration={
            "query": {
                "query": sql_query_23,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )



# Metafield Product Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Metafield_products/Metafield_products_append.sql', 'r') as file:
        sql_query_24 = file.read()

    append_Metafield_products = BigQueryInsertJobOperator(
        task_id='append_Metafield_products',
        configuration={
            "query": {
                "query": sql_query_24,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )



# Order Risks Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Order_risks/Order_risks_append.sql', 'r') as file:
        sql_query_25 = file.read()

    append_Order_risks = BigQueryInsertJobOperator(
        task_id='append_Order_risks',
        configuration={
            "query": {
                "query": sql_query_25,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )




# Tender Transactions Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Tender_Transactions/tender_transactions_append.sql', 'r') as file:
        sql_query_26 = file.read()

    append_tender_transactions = BigQueryInsertJobOperator(
        task_id='append_tender_transactions',
        configuration={
            "query": {
                "query": sql_query_26,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )




# Fulfillments Staging Table Refresh - Append

    # # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Fulfillments/Fulfillments_append.sql', 'r') as file:
        sql_query_27 = file.read()

    append_Fulfillments = BigQueryInsertJobOperator(
        task_id='append_Fulfillments',
        configuration={
            "query": {
                "query": sql_query_27,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )

# Fulfillment Orders Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Fulfillment_Orders/Fulfillment_Orders_append.sql', 'r') as file:
        sql_query_28 = file.read()

    append_Fulfillment_Orders = BigQueryInsertJobOperator(
        task_id='append_Fulfillment_Orders',
        configuration={
            "query": {
                "query": sql_query_28,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )


# Smart Collections Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Smart_collections/Smart_collections_append.sql', 'r') as file:
        sql_query_29 = file.read()

    append_Smart_collections = BigQueryInsertJobOperator(
        task_id='append_Smart_collections',
        configuration={
            "query": {
                "query": sql_query_29,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )   

# Order Items Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Order Items/Order_items_append.sql', 'r') as file:
        sql_query_30 = file.read()

    append_order_items = BigQueryInsertJobOperator(
        task_id='append_order_items',
        configuration={
            "query": {
                "query": sql_query_30,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )  


# Orderitem master Staging Table Refresh - Append

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Order_item_master/Order_item_master_append.sql', 'r') as file:
        sql_query_40 = file.read()

    append_order_item_master = BigQueryInsertJobOperator(
        task_id='append_order_item_master',
        configuration={
            "query": {
                "query": sql_query_40,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )
    
# Sanity check Table 

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Shopify_Data_Warehouse/sql/shopify_to_bq/Shopify_DW_Sanity_check.sql', 'r') as file:
        sql_query_32 = file.read()

    sanity_check = BigQueryInsertJobOperator(
        task_id='sanity_check',
        configuration={
            "query": {
                "query": sql_query_32,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )   

    

    def run_main_script():
        script_path = '/home/airflow/gcs/dags/Shopify_Data_Warehouse/python/Shopify_DW_Sanity_check_mail.py'
        try:
            # Use subprocess to run the Python script with the specified path
            result = subprocess.run(
                ['python', script_path],
                check=True,
                capture_output=True,
                text=True
            )
            # print("Script output:", result.stdout)
            # print("Script errors:", result.stderr)
        except subprocess.CalledProcessError as e:
            # print(f"Error occurred while running the script: {e}")
            # print(f"Command output: {e.stdout}")
            # print(f"Command errors: {e.stderr}")
            raise

# Define the PythonOperator to run the function
    run_python_task = PythonOperator(
        task_id='run_main_script',
        python_callable=run_main_script,
    )

    def product_mapping():
        script_path = '/home/airflow/gcs/dags/Shopify_Data_Warehouse/python/Gsheet_to_Static_Table.py'
        try:
            # Use subprocess to run the Python script with the specified path
            result = subprocess.run(
                ['python', script_path],
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Error occurred while running the script: {e}")
            raise

# Define the PythonOperator to run the function
    run_product_mapping = PythonOperator(
        task_id='product_mapping',
        python_callable=product_mapping,
    )

start_pipeline >> append_order >> append_order_items >> run_product_mapping >> append_order_item_master
# start_pipeline >> [append_abandoned_checkout, append_metafield_customers, append_discount_code, append_customer, append_order >> append_order_items >> run_product_mapping >> append_order_item_master , append_draft_order >> append_draft_order_items, append_transaction, append_refund_order, append_customer_address, append_metafield_order, append_collections, append_metafield_collections, append_pages, append_metafield_pages, append_locations, append_inventory_level, append_articles, append_product_variants, append_metafield_articles, append_products, append_inventory_items, append_customer_journey_summary, append_Metafield_Product_Variants,append_Metafield_products, append_Order_risks, append_tender_transactions, append_Fulfillment_Orders, append_Fulfillments, append_Smart_collections] >> sanity_check >> run_python_task
