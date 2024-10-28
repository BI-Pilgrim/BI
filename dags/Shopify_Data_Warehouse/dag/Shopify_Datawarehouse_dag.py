
# # Import Functions
# from datetime import timedelta
# from airflow import DAG
# from airflow.utils.dates import days_ago, timezone
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator

# # Define the start date in UTC
# START_DATE = timezone.datetime(2024, 9, 15, 23, 30, 0, tzinfo=timezone.utc)  # Corresponds to 5 AM IST on 2024-09-16

# GOOGLE_CONN_ID = "google_cloud_default"
# PROJECT_ID = "shopify-pubsub-project"
# DATASET = "Shopify_Production"
# LOCATION = "US"  # Ensure this matches your dataset location

# DATASET_STAGING = "Shopify_staging"


# default_args = {
#     'owner': 'rwitapa.mitra@discoverpilgrim.com',
#     'depends_on_past': False,
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 1,
#     'start_date': START_DATE,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     dag_id='Shopify_Warehouse',
#     schedule_interval='30 23 * * *',  # Cron expression for 5 AM IST (11:30 PM UTC)
#     default_args=default_args,
#     catchup=False
# ) as dag:
#     start_pipeline = DummyOperator(
#         task_id='start_pipeline',
#         dag=dag
#     )

# # Abandoned Checkout Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Abandoned_checkout_append.sql', 'r') as file:
#         sql_query_1 = file.read()

#     append_abandoned_checkout = BigQueryInsertJobOperator(
#         task_id='append_abandoned_checkout',
#         configuration={
#             "query": {
#                 "query": sql_query_1,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_abandoned_checkout_dataset = BigQueryCheckOperator(
#         task_id='check_abandoned_checkout_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Abandoned_checkout`'
#     )


# # Metafield Customer Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Metafield_customer_append.sql', 'r') as file:
#         sql_query_2 = file.read()

#     append_metafield_customers = BigQueryInsertJobOperator(
#         task_id='append_metafield_customers',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_metafield_customers_dataset = BigQueryCheckOperator(
#         task_id='check_metafield_customers_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Metafield_customers`'
#     )

# # Discount Code Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Discount_code_append.sql', 'r') as file:
#         sql_query_3 = file.read()

#     append_discount_code = BigQueryInsertJobOperator(
#         task_id='append_discount_code',
#         configuration={
#             "query": {
#                 "query": sql_query_3,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_discount_code_dataset = BigQueryCheckOperator(
#         task_id='check_discount_code_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Discount_Code`'
#     )

# # Customer Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Customer_append.sql', 'r') as file:
#         sql_query_4 = file.read()

#     append_customer = BigQueryInsertJobOperator(
#         task_id='append_customer',
#         configuration={
#             "query": {
#                 "query": sql_query_4,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_customers_dataset = BigQueryCheckOperator(
#         task_id='check_customers_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Customers`'
#     )



# # Orders Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Orders_append.sql', 'r') as file:
#         sql_query_5 = file.read()

#     append_order = BigQueryInsertJobOperator(
#         task_id='append_order',
#         configuration={
#             "query": {
#                 "query": sql_query_5,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_order_dataset = BigQueryCheckOperator(
#         task_id='check_order_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Orders`'
#     )

# # Transaction Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Transaction_append.sql', 'r') as file:
#         sql_query_6 = file.read()

#     append_transaction = BigQueryInsertJobOperator(
#         task_id='append_transaction',
#         configuration={
#             "query": {
#                 "query": sql_query_6,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_transaction_dataset = BigQueryCheckOperator(
#         task_id='check_transaction_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Transactions`'
#     )


# # Draft Item Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Draft_order_item_append.sql', 'r') as file:
#         sql_query_7 = file.read()

#     append_draft_order_item = BigQueryInsertJobOperator(
#         task_id='append_draft_order_item',
#         configuration={
#             "query": {
#                 "query": sql_query_7,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_draft_order_item_dataset = BigQueryCheckOperator(
#         task_id='check_draft_order_item_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Draft_Order_items`'
#     )


# # Draft Order Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Draft_order_append.sql', 'r') as file:
#         sql_query_8 = file.read()

#     append_draft_order = BigQueryInsertJobOperator(
#         task_id='append_draft_order',
#         configuration={
#             "query": {
#                 "query": sql_query_8,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_draft_order_dataset = BigQueryCheckOperator(
#         task_id='check_draft_order_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Draft_Orders`'
#     )

# # Fulfilment Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Fulfillment_append.sql', 'r') as file:
#         sql_query_9 = file.read()

#     append_fulfilment = BigQueryInsertJobOperator(
#         task_id='append_fulfilment',
#         configuration={
#             "query": {
#                 "query": sql_query_9,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_fulfilment_dataset = BigQueryCheckOperator(
#         task_id='check_fulfilment_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Fulfillments`'
#     )

# # Order Item Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Order_item_append.sql', 'r') as file:
#         sql_query_10 = file.read()

#     append_order_item = BigQueryInsertJobOperator(
#         task_id='append_order_item',
#         configuration={
#             "query": {
#                 "query": sql_query_10,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_order_item_dataset = BigQueryCheckOperator(
#         task_id='check_order_item_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Order_items`'
#     )

# # Refund Order Item Staging Table Refresh - Append

#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Refund_Order_item_append.sql', 'r') as file:
#         sql_query_11 = file.read()

#     append_refund_order_item = BigQueryInsertJobOperator(
#         task_id='append_refund_order_item',
#         configuration={
#             "query": {
#                 "query": sql_query_11,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_refund_order_item_dataset = BigQueryCheckOperator(
#         task_id='check_refund_order_item_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET_STAGING}.Refund_Order_items`'
#     )


# # Order Master Table Check
#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Order_Master_Append.sql', 'r') as file:
#         sql_query_12 = file.read()

#     append_order_master = BigQueryInsertJobOperator(
#         task_id='append_order_master',
#         configuration={
#             "query": {
#                 "query": sql_query_12,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_order_master_dataset = BigQueryCheckOperator(
#         task_id='check_order_master_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Order_Master`'
#     )


# # Customer Master Table Check
#     # Load SQL query from file
#     with open('/home/airflow/gcs/dags/sql/Customer_Master_Append.sql', 'r') as file:
#         sql_query_13 = file.read()

#     append_customer_master = BigQueryInsertJobOperator(
#         task_id='append_customer_master',
#         configuration={
#             "query": {
#                 "query": sql_query_13,
#                 "useLegacySql": False,
#                 "location": LOCATION,
#             }
#         }
#     )

#     check_customer_master_dataset = BigQueryCheckOperator(
#         task_id='check_customer_master_dataset',
#         use_legacy_sql=False,
#         location=LOCATION,
#         sql=f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Customer_Master`'
#     )

#     finish_pipeline = DummyOperator(
#         task_id='finish_pipeline',
#         dag=dag
#     )

# # Task Orchestration

# start_pipeline >> [append_order, append_transaction,append_abandoned_checkout,append_discount_code,append_customer,append_metafield_customers,append_draft_order_item,append_draft_order,append_fulfilment,append_order_item,append_refund_order_item]



# append_refund_order_item >> check_refund_order_item_dataset
# append_order_item >> check_order_item_dataset
# append_fulfilment >> check_fulfilment_dataset
# append_draft_order_item >> check_draft_order_item_dataset
# append_draft_order >> check_draft_order_dataset
# append_metafield_customers >> check_metafield_customers_dataset
# append_discount_code >> check_discount_code_dataset
# append_abandoned_checkout >> check_abandoned_checkout_dataset
# append_customer >> check_customers_dataset
# append_order >> check_order_dataset
# append_transaction >> check_transaction_dataset


# # Order Master and Customer Master tasks

# check_order_dataset >> append_order_master
# check_transaction_dataset >> append_order_master

# check_customers_dataset >> append_customer_master
# check_metafield_customers_dataset >> append_customer_master


# append_order_master >> check_order_master_dataset >> finish_pipeline
# append_customer_master >> check_customer_master_dataset >> finish_pipeline


# check_discount_code_dataset >> finish_pipeline
# check_abandoned_checkout_dataset >> finish_pipeline
# check_draft_order_item_dataset >> finish_pipeline
# check_draft_order_dataset >> finish_pipeline
# check_fulfilment_dataset >> finish_pipeline
# check_order_item_dataset >> finish_pipeline
# check_refund_order_item_dataset >> finish_pipeline
