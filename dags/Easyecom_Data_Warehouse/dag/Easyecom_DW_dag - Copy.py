# # Import Functions
# from datetime import timedelta
# from airflow import DAG
# from airflow.utils.dates import timezone
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# import os


# # Define default arguments for the DAG
# default_args = {
#     'owner': 'omkar.sadawarte@discoverpilgrim.com',
#     'depends_on_past': False,
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }


# # Define constants
# LOCATION = "US"  # Replace with your BigQuery dataset location (e.g., "US", "EU")
# SQL_DIR = "..//dags//Easyecom_Data_Warehouse//sql//easyecom_to_bq"  # Adjust this path if necessary

# # Define the DAG
# with DAG(
#     dag_id='EasyEcom_DataWarehouse_DAG',
#     default_args=default_args,
#     description='A DAG to update the EasyEcom staging tables from EasyEcom raw tables',
#     schedule_interval='30 3 * * *',  # 3:30 AM UTC is 9:00 AM IST
#     start_date=timezone.datetime(2025, 1, 3),
#     catchup=False,
# ) as dag:

#     # Start of the pipeline
#     start_pipeline = DummyOperator(
#         task_id='start_pipeline'
#     )

#     # Easyecom_All_Return_Order Staging Table Refresh - Append
#     Easyecom_All_Return_Order_sql_path = os.path.join(SQL_DIR, "All_Return_Order//Easyecom_All_Return_Order_Append.sql")
#     with open(Easyecom_All_Return_Order_sql_path, 'r') as file:
#         sql_query_1 = file.read()

#     Append_Easyecom_All_Return_Order = BigQueryInsertJobOperator(
#         task_id='Append_Easyecom_All_Return_Order',
#         configuration={
#             "query": {
#                 "query": sql_query_1,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Countries Staging Table Refresh - Append
#     Countries_sql_path = os.path.join(SQL_DIR, "Countries//Easyecom_Countries_Append.sql")
#     with open(Countries_sql_path, 'r') as file:
#         sql_query_2 = file.read()

#     Append_Countries = BigQueryInsertJobOperator(
#         task_id='Append_Countries',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # # Customers Staging Table Refresh - Append
#     Customers_sql_path = os.path.join(SQL_DIR, "Customers//Easyecom_Customers_Append.sql")
#     with open(Customers_sql_path, 'r') as file:
#         sql_query_3 = file.read()

#     Append_Customers = BigQueryInsertJobOperator(
#         task_id='Append_Customers',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Easyecom_Order_Status_History Staging Table Refresh - Append
#     Easyecom_Order_Status_History_sql_path = os.path.join(SQL_DIR, "Easyecom_Order_Status_History//Easyecom_Order_Status_History_Append.sql")
#     with open(Easyecom_Order_Status_History_sql_path, 'r') as file:
#         sql_query_4 = file.read()

#     Append_Easyecom_Order_Status_History = BigQueryInsertJobOperator(
#         task_id='Append_Easyecom_Order_Status_History',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # GRN_detaill_Reports_sql_path Staging Table Refresh - Append
#     GRN_detaill_Reports_sql_path = os.path.join(SQL_DIR, "GRN_detaill_Reports//Easyecom_GRN_detaill_Reports_Append.sql")
#     with open(GRN_detaill_Reports_sql_path, 'r') as file:
#         sql_query_5 = file.read()

#     Append_GRN_detaill_Reports_sql_path = BigQueryInsertJobOperator(
#         task_id='Append_GRN_detaill_Reports_sql_path',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Inventory_Aging_report Staging Table Refresh - Append
#     Inventory_Aging_report_sql_path = os.path.join(SQL_DIR, "Inventory_Aging_report//Easyecom_Inventory_Aging_report_Append.sql")
#     with open(Inventory_Aging_report_sql_path, 'r') as file:
#         sql_query_6 = file.read()

#     Append_Inventory_Aging_report = BigQueryInsertJobOperator(
#         task_id='Append_Inventory_Aging_report',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Inventory_details Staging Table Refresh - Append
#     Inventory_details_sql_path = os.path.join(SQL_DIR, "Inventory_details//Easyecom_Inventory_details_Append.sql")
#     with open(Inventory_details_sql_path, 'r') as file:
#         sql_query_7 = file.read()

#     Append_Inventory_details = BigQueryInsertJobOperator(
#         task_id='Append_Inventory_details',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Inventory_Snapshot Staging Table Refresh - Append
#     Inventory_Snapshot_sql_path = os.path.join(SQL_DIR, "Inventory_Snapshot//Easyecom_Inventory_Snapshot_Append.sql")
#     with open(Inventory_Snapshot_sql_path, 'r') as file:
#         sql_query_8 = file.read()

#     Append_Inventory_Snapshot = BigQueryInsertJobOperator(
#         task_id='Append_Inventory_Snapshot',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Locations Staging Table Refresh - Append
#     Locations_sql_path = os.path.join(SQL_DIR, "Locations//Easyecom_Locations_Append.sql")
#     with open(Locations_sql_path, 'r') as file:
#         sql_query_9 = file.read()

#     Append_Locations = BigQueryInsertJobOperator(
#         task_id='Append_Locations',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Marketplace Staging Table Refresh - Append
#     Marketplace_sql_path = os.path.join(SQL_DIR, "Marketplace//Easyecom_Marketplace_Append.sql")
#     with open(Marketplace_sql_path, 'r') as file:
#         sql_query_10 = file.read()

#     Append_Marketplace = BigQueryInsertJobOperator(
#         task_id='Append_Marketplace',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Marketplace_listings Staging Table Refresh - Append
#     Marketplace_listings_sql_path = os.path.join(SQL_DIR, "Marketplace_listings//Easyecom_Marketplace_listings_Append.sql")
#     with open(Marketplace_listings_sql_path, 'r') as file:
#         sql_query_11 = file.read()

#     Append_Marketplace_listings = BigQueryInsertJobOperator(
#         task_id='Append_Marketplace_listings',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Master_Product Staging Table Refresh - Append
#     Master_Product_sql_path = os.path.join(SQL_DIR, "Master_Product//Easyecom_Master_Product_Append.sql")
#     with open(Master_Product_sql_path, 'r') as file:
#         sql_query_12 = file.read()

#     Append_Master_Product = BigQueryInsertJobOperator(
#         task_id='Append_Master_Product',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # # Mini_Sales_Report Staging Table Refresh - Append
#     # Customers_sql_path = os.path.join(SQL_DIR, "Customers//Easyecom_Customers_Append.sql")
#     # with open(Customers_sql_path, 'r') as file:
#     #     sql_query_13 = file.read()

#     # Append_Customers = BigQueryInsertJobOperator(
#     #     task_id='Append_Customers',
#     #     configuration={
#     #         "query": {
#     #             "query": sql_query_2,
#     #             "useLegacySql": False,
#     #         },
#     #         "location": LOCATION,
#     #     }
#     # )

#     # Order_items Staging Table Refresh - Append
#     Order_items_sql_path = os.path.join(SQL_DIR, "Order_items//Easyecom_Order_items_Append.sql")
#     with open(Order_items_sql_path, 'r') as file:
#         sql_query_14 = file.read()

#     Append_Order_items = BigQueryInsertJobOperator(
#         task_id='Append_Order_items',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Order_Status_History Staging Table Refresh - Append
#     Order_Status_History_sql_path = os.path.join(SQL_DIR, "Order_Status_History//Easyecom_Order_Status_History_Append.sql")
#     with open(Order_Status_History_sql_path, 'r') as file:
#         sql_query_15 = file.read()

#     Append_Order_Status_History = BigQueryInsertJobOperator(
#         task_id='Append_Order_Status_History',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Orders Staging Table Refresh - Append
#     Orders_sql_path = os.path.join(SQL_DIR, "Orders//Easyecom_Orders_Append.sql")
#     with open(Orders_sql_path, 'r') as file:
#         sql_query_16 = file.read()

#     Append_Orders = BigQueryInsertJobOperator(
#         task_id='Append_Orders',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Pending_Returns_report Staging Table Refresh - Append
#     Pending_Returns_report_sql_path = os.path.join(SQL_DIR, "Pending_Returns_report//Easyecom_Pending_Returns_report_Append.sql")
#     with open(Pending_Returns_report_sql_path, 'r') as file:
#         sql_query_17 = file.read()

#     Append_Pending_Returns_report = BigQueryInsertJobOperator(
#         task_id='Append_Pending_Returns_report',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Purchase_Orders Staging Table Refresh - Append
#     Purchase_Orders_sql_path = os.path.join(SQL_DIR, "Purchase_Orders//Easyecom_Purchase_Orders_Append.sql")
#     with open(Purchase_Orders_sql_path, 'r') as file:
#         sql_query_18 = file.read()

#     Append_Purchase_Orders = BigQueryInsertJobOperator(
#         task_id='Append_Purchase_Orders',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Reports Staging Table Refresh - Append
#     Reports_sql_path = os.path.join(SQL_DIR, "Reports//Easyecom_Reports_Append.sql")
#     with open(Reports_sql_path, 'r') as file:
#         sql_query_19 = file.read()

#     Append_Reports = BigQueryInsertJobOperator(
#         task_id='Append_Reports',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Return_report Staging Table Refresh - Append
#     Return_report_sql_path = os.path.join(SQL_DIR, "Return_report//Easyecom_Return_report_Append.sql")
#     with open(Return_report_sql_path, 'r') as file:
#         sql_query_20 = file.read()

#     Append_Return_report = BigQueryInsertJobOperator(
#         task_id='Append_Return_report',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # States Staging Table Refresh - Append
#     States_sql_path = os.path.join(SQL_DIR, "States//Easyecom_States_Append.sql")
#     with open(States_sql_path, 'r') as file:
#         sql_query_21 = file.read()

#     Append_States = BigQueryInsertJobOperator(
#         task_id='Append_States',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # # Tax_Report Staging Table Refresh - Append
#     # Customers_sql_path = os.path.join(SQL_DIR, "Customers//Easyecom_Customers_Append.sql")
#     # with open(Customers_sql_path, 'r') as file:
#     #     sql_query_22 = file.read()

#     # Append_Customers = BigQueryInsertJobOperator(
#     #     task_id='Append_Customers',
#     #     configuration={
#     #         "query": {
#     #             "query": sql_query_2,
#     #             "useLegacySql": False,
#     #         },
#     #         "location": LOCATION,
#     #     }
#     # )

#     # Vendors Staging Table Refresh - Append
#     Vendors_sql_path = os.path.join(SQL_DIR, "Vendors//Easyecom_Vendors_Append.sql")
#     with open(Vendors_sql_path, 'r') as file:
#         sql_query_23 = file.read()

#     Append_Vendors = BigQueryInsertJobOperator(
#         task_id='Append_Vendors',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )

#     # Status_Wise_Stock_report Staging Table Refresh - Append
#     Status_Wise_Stock_report_sql_path = os.path.join(SQL_DIR, "Wise_Stock_report//Easyecom_Status_Wise_Stock_report_Append.sql")
#     with open(Status_Wise_Stock_report_sql_path, 'r') as file:
#         sql_query_24 = file.read()

#     Append_Status_Wise_Stock_report = BigQueryInsertJobOperator(
#         task_id='Append_Status_Wise_Stock_report',
#         configuration={
#             "query": {
#                 "query": sql_query_2,
#                 "useLegacySql": False,
#             },
#             "location": LOCATION,
#         }
#     )


#     # End of the pipeline
#     finish_pipeline = DummyOperator(
#         task_id='finish_pipeline'
#     )


#     # # Task Orchestration
#     # start_pipeline >> [Append_Easyecom_All_Return_Order, Append_Countries, Append_Customers, Append_Easyecom_Order_Status_History, Append_GRN_detaill_Reports_sql_path,Append_Inventory_Aging_report, Append_Inventory_details, Append_Inventory_Snapshot, Append_Locations, Append_Marketplace, Append_Marketplace_listings, Append_Master_Product, Append_Order_Status_History, [Append_Orders >> Append_Order_items], Append_Pending_Returns_report, Append_Purchase_Orders, Append_Reports, Append_Return_report, Append_States, Append_Vendors, Append_Status_Wise_Stock_report]
#     # [Append_Easyecom_All_Return_Order, Append_Countries, Append_Customers, Append_Easyecom_Order_Status_History, Append_GRN_detaill_Reports_sql_path,Append_Inventory_Aging_report, Append_Inventory_details, Append_Inventory_Snapshot, Append_Locations, Append_Marketplace, Append_Marketplace_listings, Append_Master_Product, Append_Order_Status_History, Append_Orders, Append_Pending_Returns_report, Append_Purchase_Orders, Append_Reports, Append_Return_report, Append_States, Append_Vendors, Append_Status_Wise_Stock_report] >> finish_pipeline #Append_Order_items
#     # # Append_Order_items >> finish_pipeline

#     # Orchestrate tasks
#     start_pipeline >> [
#         Append_Easyecom_All_Return_Order,
#         Append_Countries,
#         Append_Customers,
#         Append_Easyecom_Order_Status_History,
#         Append_GRN_detaill_Reports_sql_path,
#         Append_Inventory_Aging_report,
#         Append_Inventory_details,
#         Append_Inventory_Snapshot,
#         Append_Locations,
#         Append_Marketplace,
#         Append_Marketplace_listings,
#         Append_Master_Product,
#         Append_Order_Status_History,
#         Append_Orders,
#         Append_Pending_Returns_report,
#         Append_Purchase_Orders,
#         Append_Reports,
#         Append_Return_report,
#         Append_States,
#         Append_Vendors,
#         Append_Status_Wise_Stock_report,
#     ]

#     # Ensure Append_Order_items runs after Append_Orders
#     Append_Orders >> Append_Order_items

#     # Ensure all tasks finish at the end
#     [
#         Append_Easyecom_All_Return_Order,
#         Append_Countries,
#         Append_Customers,
#         Append_Easyecom_Order_Status_History,
#         Append_GRN_detaill_Reports_sql_path,
#         Append_Inventory_Aging_report,
#         Append_Inventory_details,
#         Append_Inventory_Snapshot,
#         Append_Locations,
#         Append_Marketplace,
#         Append_Marketplace_listings,
#         Append_Master_Product,
#         Append_Order_Status_History,
#         Append_Order_items,
#         Append_Pending_Returns_report,
#         Append_Purchase_Orders,
#         Append_Reports,
#         Append_Return_report,
#         Append_States,
#         Append_Vendors,
#         Append_Status_Wise_Stock_report,
#     ] >> finish_pipeline



####################################################################################################################################
# # Import Functions 
# from datetime import timedelta
# from airflow import DAG
# from airflow.utils.dates import days_ago, timezone
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
# import subprocess
# from airflow.operators.python import PythonOperator

# # Define the start date in UTC 
# START_DATE = timezone.datetime(2025, 1, 10, 7, 55, 0, tzinfo=timezone.utc)  # Corresponds to 1.15 PM IST on 2025-01-02

# GOOGLE_CONN_ID = "google_cloud_default"
# PROJECT_ID = "shopify-pubsub-project"
# DATASET = "easycom"
# LOCATION = "asia-south1"  # Ensure this matches your dataset location

# DATASET_STAGING = "Data_Warehouse_Easyecom_Staging"


# default_args = {
#     'owner': 'shafiq@discoverpilgrim.com',
#     'depends_on_past': False,
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 1,
#     'start_date': START_DATE,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     dag_id='Easyecom_DataWarehouse',
#     schedule_interval='30 00 * * *',  # Cron expression for 6 AM IST (12:30 AM UTC)
#     default_args=default_args,
#     catchup=False
# ) as dag:
#     start_pipeline = DummyOperator(
#         task_id='start_pipeline',
#         dag=dag
#     )

    # # Orders Staging Table Refresh - Append

    # # Load SQL query from file
    # with open('/home/airflow/gcs/dags/Easyecom_Data_Warehouse/sql/Orders/Order_Append.sql', 'r') as file:
    #     sql_query_1 = file.read()

    # Orders_Append = BigQueryInsertJobOperator(
    #     task_id='Orders_Append',
    #     configuration={
    #         "query": {
    #             "query": sql_query_1,
    #             "useLegacySql": False,
    #             "location": LOCATION,
    #         }
    #     }
    # )

    # # Order item Staging Table Refresh - Append

    # # Load SQL query from file
    # with open('/home/airflow/gcs/dags/Easyecom_Data_Warehouse/sql/Order_items/Order_item_Append.sql', 'r') as file:
    #     sql_query_2 = file.read()

    # Orderitem_Append = BigQueryInsertJobOperator(
    #     task_id='Orderitem_Append',
    #     configuration={
    #         "query": {
    #             "query": sql_query_2,
    #             "useLegacySql": False,
    #             "location": LOCATION,
    #         }
    #     }
    # )    


    # # Sanity Check of all table
    # # Load SQL query from file
    # with open('/home/airflow/gcs/dags/Easyecom_Data_Warehouse/sql/Easyecom_DW_sanity_check.sql', 'r') as file:
    #     sql_query_33 = file.read()

    #     DW_Sanity_check = BigQueryInsertJobOperator(
    #     task_id='DW_Sanity_check',
    #     configuration={
    #         "query": {
    #             "query": sql_query_34,
    #             "useLegacySql": False,
    #             "location": LOCATION,
    #         }
    #     }
    # )  
        
    # def run_main_script():
    #     script_path = '/home/airflow/gcs/dags/Easyecom_Data_Warehouse/dag/Easyecom_DW_dag.py'
    #     try:
    #      # Use subprocess to run the Python script with the specified path
    #         result = subprocess.run(
    #             ['python', script_path],
    #             check=True,
    #             capture_output=True,
    #             text=True
    #         )
    #      # print("Script output:", result.stdout)
    #      # print("Script errors:", result.stderr)
    #     except subprocess.CalledProcessError as e:
    #      # print(f"Error occurred while running the script: {e}")
    #      # print(f"Command output: {e.stdout}")
    #      # print(f"Command errors: {e.stderr}")
    #         raise

    # # Define the PythonOperator to run the function
    # run_python_task = PythonOperator(
    #     task_id='run_main_script',
    #     python_callable=run_main_script,
    # )


#    finish_pipeline = DummyOperator(
#        task_id='finish_pipeline',
#        dag=dag
#    )

# start_pipeline >> Orders_Append >> Orderitem_Append >> DW_Sanity_check >> run_python_task
