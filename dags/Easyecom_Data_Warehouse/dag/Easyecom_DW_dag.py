# Import Functions
from datetime import timedelta
from airflow import DAG
import subprocess
import sys
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
import os
from Easyecom_Data_Warehouse.python.Easyecom_DW_Sanity_check_mail import send_sanity_check_email


# Define default arguments for the DAG
default_args = {
    'owner': 'omkar.sadawarte@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define constants
LOCATION = "US"  # Replace with your BigQuery dataset location (e.g., "US", "EU")
SQL_DIR = "/home/airflow/gcs/dags/Easyecom_Data_Warehouse/sql/easyecom_to_bq"  # Adjust this path if necessary
# SQL_DIR = "../dags/Easyecom_Data_Warehouse/sql/easyecom_to_bq/"  # Adjust this path if necessary

# Define the DAG
with DAG(
    dag_id='EasyEcom_DataWarehouse_DAG',
    default_args=default_args,
    description='A DAG to update the EasyEcom staging tables from EasyEcom raw tables',
    schedule_interval='30 3 * * *',  # 3:30 AM UTC is 9:00 AM IST
    start_date=timezone.datetime(2025, 1, 3),
    catchup=False,
) as dag:

    # Start of the pipeline
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    # Easyecom_All_Return_Order Staging Table Refresh - Append
    Easyecom_All_Return_Order_sql_path = os.path.join(SQL_DIR, "All_Return_Order/Easyecom_All_Return_Order_Append.sql")
    with open(Easyecom_All_Return_Order_sql_path, 'r') as file:
        sql_query_1 = file.read()

    Append_Easyecom_All_Return_Order = BigQueryInsertJobOperator(
        task_id='Append_Easyecom_All_Return_Order',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Countries Staging Table Refresh - Append
    Countries_sql_path = os.path.join(SQL_DIR, "Countries/Easyecom_Countries_Append.sql")
    with open(Countries_sql_path, 'r') as file:
        sql_query_2 = file.read()

    Append_Countries = BigQueryInsertJobOperator(
        task_id='Append_Countries',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # # Customers Staging Table Refresh - Append
    Customers_sql_path = os.path.join(SQL_DIR, "Customers/Easyecom_Customers_Append.sql")
    with open(Customers_sql_path, 'r') as file:
        sql_query_3 = file.read()

    Append_Customers = BigQueryInsertJobOperator(
        task_id='Append_Customers',
        configuration={
            "query": {
                "query": sql_query_3,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Easyecom_Order_Status_History Staging Table Refresh - Append
    Easyecom_Order_Status_History_sql_path = os.path.join(SQL_DIR, "Easyecom_Order_Status_History/Easyecom_Order_Status_History_Append.sql")
    with open(Easyecom_Order_Status_History_sql_path, 'r') as file:
        sql_query_4 = file.read()

    Append_Easyecom_Order_Status_History = BigQueryInsertJobOperator(
        task_id='Append_Easyecom_Order_Status_History',
        configuration={
            "query": {
                "query": sql_query_4,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # grn_details_report_sql_path Staging Table Refresh - Append
    grn_details_report_sql_path = os.path.join(SQL_DIR, "grn_details_report/grn_details_report_Append.sql")
    with open(grn_details_report_sql_path, 'r') as file:
        sql_query_5 = file.read()

    Append_grn_details_report_sql_path = BigQueryInsertJobOperator(
        task_id='Append_grn_details_report_sql_path',
        configuration={
            "query": {
                "query": sql_query_5,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Inventory_Aging_report Staging Table Refresh - Append
    Inventory_Aging_report_sql_path = os.path.join(SQL_DIR, "Inventory_Aging_report/Easyecom_Inventory_Aging_report_Append.sql")
    with open(Inventory_Aging_report_sql_path, 'r') as file:
        sql_query_6 = file.read()

    Append_Inventory_Aging_report = BigQueryInsertJobOperator(
        task_id='Append_Inventory_Aging_report',
        configuration={
            "query": {
                "query": sql_query_6,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Inventory_details Staging Table Refresh - Append
    Inventory_details_sql_path = os.path.join(SQL_DIR, "Inventory_details/Easyecom_Inventory_details_Append.sql")
    with open(Inventory_details_sql_path, 'r') as file:
        sql_query_7 = file.read()

    Append_Inventory_details = BigQueryInsertJobOperator(
        task_id='Append_Inventory_details',
        configuration={
            "query": {
                "query": sql_query_7,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Inventory_Snapshot Staging Table Refresh - Append
    Inventory_Snapshot_sql_path = os.path.join(SQL_DIR, "Inventory_Snapshot/Easyecom_Inventory_Snapshot_Append.sql")
    with open(Inventory_Snapshot_sql_path, 'r') as file:
        sql_query_8 = file.read()

    Append_Inventory_Snapshot = BigQueryInsertJobOperator(
        task_id='Append_Inventory_Snapshot',
        configuration={
            "query": {
                "query": sql_query_8,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Locations Staging Table Refresh - Append
    Locations_sql_path = os.path.join(SQL_DIR, "Locations/Easyecom_Locations_Append.sql")
    with open(Locations_sql_path, 'r') as file:
        sql_query_9 = file.read()

    Append_Locations = BigQueryInsertJobOperator(
        task_id='Append_Locations',
        configuration={
            "query": {
                "query": sql_query_9,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Marketplace Staging Table Refresh - Append
    Marketplace_sql_path = os.path.join(SQL_DIR, "Marketplace/Easyecom_Marketplace_Append.sql")
    with open(Marketplace_sql_path, 'r') as file:
        sql_query_10 = file.read()

    Append_Marketplace = BigQueryInsertJobOperator(
        task_id='Append_Marketplace',
        configuration={
            "query": {
                "query": sql_query_10,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Marketplace_listings Staging Table Refresh - Append
    Marketplace_listings_sql_path = os.path.join(SQL_DIR, "Marketplace_listings/Easyecom_Marketplace_listings_Append.sql")
    with open(Marketplace_listings_sql_path, 'r') as file:
        sql_query_11 = file.read()

    Append_Marketplace_listings = BigQueryInsertJobOperator(
        task_id='Append_Marketplace_listings',
        configuration={
            "query": {
                "query": sql_query_11,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Master_Product Staging Table Refresh - Append
    Master_Product_sql_path = os.path.join(SQL_DIR, "Master_Product/Easyecom_Master_Product_Append.sql")
    with open(Master_Product_sql_path, 'r') as file:
        sql_query_12 = file.read()

    Append_Master_Product = BigQueryInsertJobOperator(
        task_id='Append_Master_Product',
        configuration={
            "query": {
                "query": sql_query_12,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # # Mini_Sales_Report Staging Table Refresh - Append
    # Mini_Sales_Report_sql_path = os.path.join(SQL_DIR, "Mini_Sales_Report/Easyecom_Mini_Sales_Report_Append.sql")
    # with open(Mini_Sales_Report_sql_path, 'r') as file:
    #     sql_query_13 = file.read()

    # Append_Mini_Sales_Report = BigQueryInsertJobOperator(
    #     task_id='Append_Mini_Sales_Report',
    #     configuration={
    #         "query": {
    #             "query": sql_query_13,
    #             "useLegacySql": False,
    #         },
    #         "location": LOCATION,
    #     }
    # )

    # Order_items Staging Table Refresh - Append
    Order_items_sql_path = os.path.join(SQL_DIR, "Order_items/Easyecom_Order_items_Append.sql")
    with open(Order_items_sql_path, 'r') as file:
        sql_query_14 = file.read()

    Append_Order_items = BigQueryInsertJobOperator(
        task_id='Append_Order_items',
        configuration={
            "query": {
                "query": sql_query_14,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Order_Status_History Staging Table Refresh - Append
    Order_Status_History_sql_path = os.path.join(SQL_DIR, "Order_Status_History/Easyecom_Order_Status_History_Append.sql")
    with open(Order_Status_History_sql_path, 'r') as file:
        sql_query_15 = file.read()

    Append_Order_Status_History = BigQueryInsertJobOperator(
        task_id='Append_Order_Status_History',
        configuration={
            "query": {
                "query": sql_query_15,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Orders Staging Table Refresh - Append
    Orders_sql_path = os.path.join(SQL_DIR, "Orders/Easyecom_Orders_Append.sql")
    with open(Orders_sql_path, 'r') as file:
        sql_query_16 = file.read()

    Append_Orders = BigQueryInsertJobOperator(
        task_id='Append_Orders',
        configuration={
            "query": {
                "query": sql_query_16,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Pending_Returns_report Staging Table Refresh - Append
    Pending_Returns_report_sql_path = os.path.join(SQL_DIR, "Pending_Returns_report/Easyecom_Pending_Returns_report_Append.sql")
    with open(Pending_Returns_report_sql_path, 'r') as file:
        sql_query_17 = file.read()

    Append_Pending_Returns_report = BigQueryInsertJobOperator(
        task_id='Append_Pending_Returns_report',
        configuration={
            "query": {
                "query": sql_query_17,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Purchase_Orders Staging Table Refresh - Append
    Purchase_Orders_sql_path = os.path.join(SQL_DIR, "Purchase_Orders/Easyecom_Purchase_Orders_Append.sql")
    with open(Purchase_Orders_sql_path, 'r') as file:
        sql_query_18 = file.read()

    Append_Purchase_Orders = BigQueryInsertJobOperator(
        task_id='Append_Purchase_Orders',
        configuration={
            "query": {
                "query": sql_query_18,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Reports Staging Table Refresh - Append
    Reports_sql_path = os.path.join(SQL_DIR, "Reports/Easyecom_Reports_Create.sql")
    with open(Reports_sql_path, 'r') as file:
        sql_query_19 = file.read()

    Append_Reports = BigQueryInsertJobOperator(
        task_id='Append_Reports',
        configuration={
            "query": {
                "query": sql_query_19,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Return_report Staging Table Refresh - Append
    Return_report_sql_path = os.path.join(SQL_DIR, "Return_report/Easyecom_Return_report_Append.sql")
    with open(Return_report_sql_path, 'r') as file:
        sql_query_20 = file.read()

    Append_Return_report = BigQueryInsertJobOperator(
        task_id='Append_Return_report',
        configuration={
            "query": {
                "query": sql_query_20,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # States Staging Table Refresh - Append
    States_sql_path = os.path.join(SQL_DIR, "States/Easyecom_States_Append.sql")
    with open(States_sql_path, 'r') as file:
        sql_query_21 = file.read()

    Append_States = BigQueryInsertJobOperator(
        task_id='Append_States',
        configuration={
            "query": {
                "query": sql_query_21,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Vendors Staging Table Refresh - Append
    Vendors_sql_path = os.path.join(SQL_DIR, "Vendors/Easyecom_Vendors_Append.sql")
    with open(Vendors_sql_path, 'r') as file:
        sql_query_22 = file.read()

    Append_Vendors = BigQueryInsertJobOperator(
        task_id='Append_Vendors',
        configuration={
            "query": {
                "query": sql_query_22,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Status_Wise_Stock_report Staging Table Refresh - Append
    Status_Wise_Stock_report_sql_path = os.path.join(SQL_DIR, "Wise_Stock_report/Easyecom_Status_Wise_Stock_report_Create.sql")
    with open(Status_Wise_Stock_report_sql_path, 'r') as file:
        sql_query_23 = file.read()

    Append_Status_Wise_Stock_report = BigQueryInsertJobOperator(
        task_id='Append_Status_Wise_Stock_report',
        configuration={
            "query": {
                "query": sql_query_23,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # All_Return_Order_items Staging Table Refresh - Append
    All_Return_Order_items_sql_path = os.path.join(SQL_DIR, "All_Return_Order_items/All_Return_Order_items_Append.sql")
    with open(All_Return_Order_items_sql_path, 'r') as file:
        sql_query_24 = file.read()

    Append_All_Return_Order_items = BigQueryInsertJobOperator(
        task_id='Append_All_Return_Order_items',
        configuration={
            "query": {
                "query": sql_query_24,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # # daily_metrics Staging Table Refresh - Append
    # daily_metrics_sql_path = os.path.join(SQL_DIR, "daily_metrics/daily_metrics_Append.sql")
    # with open(daily_metrics_sql_path, 'r') as file:
    #     sql_query_25 = file.read()

    # Append_daily_metrics = BigQueryInsertJobOperator(
    #     task_id='Append_daily_metrics',
    #     configuration={
    #         "query": {
    #             "query": sql_query_25,
    #             "useLegacySql": False,
    #         },
    #         "location": LOCATION,
    #     }
    # )

    # grn_details Staging Table Refresh - Append
    grn_details_sql_path = os.path.join(SQL_DIR, "grn_details/grn_details_Append.sql")
    with open(grn_details_sql_path, 'r') as file:
        sql_query_26 = file.read()

    Append_grn_details = BigQueryInsertJobOperator(
        task_id='Append_grn_details',
        configuration={
            "query": {
                "query": sql_query_26,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # inventory_view_by_bin_report Staging Table Refresh - Append
    inventory_view_by_bin_report_sql_path = os.path.join(SQL_DIR, "inventory_view_by_bin_report/inventory_view_by_bin_report_Append.sql")
    with open(inventory_view_by_bin_report_sql_path, 'r') as file:
        sql_query_27 = file.read()

    Append_inventory_view_by_bin_report = BigQueryInsertJobOperator(
        task_id='Append_inventory_view_by_bin_report',
        configuration={
            "query": {
                "query": sql_query_27,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Kits Staging Table Refresh - Append
    Kits_sql_path = os.path.join(SQL_DIR, "Kits/Kits_Append.sql")
    with open(Kits_sql_path, 'r') as file:
        sql_query_28 = file.read()

    Append_Kits = BigQueryInsertJobOperator(
        task_id='Append_Kits',
        configuration={
            "query": {
                "query": sql_query_28,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # # order_status_metrics Staging Table Refresh - Append
    # order_status_metrics_sql_path = os.path.join(SQL_DIR, "order_status_metrics/order_status_metrics_Append.sql")
    # with open(order_status_metrics_sql_path, 'r') as file:
    #     sql_query_29 = file.read()

    # Append_order_status_metrics = BigQueryInsertJobOperator(
    #     task_id='Append_order_status_metrics',
    #     configuration={
    #         "query": {
    #             "query": sql_query_29,
    #             "useLegacySql": False,
    #         },
    #         "location": LOCATION,
    #     }
    # )

    # pending_return_orders Staging Table Refresh - Append
    pending_return_orders_sql_path = os.path.join(SQL_DIR, "pending_return_orders/pending_return_orders_Append.sql")
    with open(pending_return_orders_sql_path, 'r') as file:
        sql_query_30 = file.read()

    Append_pending_return_orders = BigQueryInsertJobOperator(
        task_id='Append_pending_return_orders',
        configuration={
            "query": {
                "query": sql_query_30,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # # shipping_status_metrics Staging Table Refresh - Append
    # shipping_status_metrics_sql_path = os.path.join(SQL_DIR, "shipping_status_metrics/shipping_status_metrics_Append.sql")
    # with open(shipping_status_metrics_sql_path, 'r') as file:
    #     sql_query_31 = file.read()

    # Append_shipping_status_metrics = BigQueryInsertJobOperator(
    #     task_id='Append_shipping_status_metrics',
    #     configuration={
    #         "query": {
    #             "query": sql_query_31,
    #             "useLegacySql": False,
    #         },
    #         "location": LOCATION,
    #     }
    # )

    # # warehouse_metrics Staging Table Refresh - Append
    # warehouse_metrics_sql_path = os.path.join(SQL_DIR, "warehouse_metrics/warehouse_metrics_Append.sql")
    # with open(warehouse_metrics_sql_path, 'r') as file:
    #     sql_query_32 = file.read()

    # Append_warehouse_metrics = BigQueryInsertJobOperator(
    #     task_id='Append_warehouse_metrics',
    #     configuration={
    #         "query": {
    #             "query": sql_query_32,
    #             "useLegacySql": False,
    #         },
    #         "location": LOCATION,
    #     }
    # )

    # Tax_report_new Staging Table Refresh - Append
    Tax_report_new_sql_path = os.path.join(SQL_DIR, "Tax_report_new/Tax_report_new_append.sql")
    with open(Tax_report_new_sql_path, 'r') as file:
        sql_query_24 = file.read()

    Append_Tax_report_new = BigQueryInsertJobOperator(
        task_id='Append_Tax_report_new',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # Sanity Check of all table
    # Load SQL query from file
    # with open("../dags/Easyecom_Data_Warehouse/sql/Easyecom_DW_sanity_check.sql", 'r') as file:
    with open("/home/airflow/gcs/dags/Easyecom_Data_Warehouse/sql/Easyecom_DW_sanity_check.sql", 'r') as file:
        sql_query_33 = file.read()

        DW_Sanity_check = BigQueryInsertJobOperator(
        task_id='DW_Sanity_check',
        configuration={
            "query": {
                "query": sql_query_33,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )
        
    def run_main_script():
        # script_path = '../dags/Easyecom_Data_Warehouse/dag/Easyecom_DW_dag.py'
        script_path = '/home/airflow/gcs/dags/Easyecom_Data_Warehouse/dag/Easyecom_DW_dag.py'
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
        python_callable=send_sanity_check_email,
    )

    # End of the pipeline
    finish_pipeline = DummyOperator(
        task_id='finish_pipeline'
    )


    # Orchestrate tasks
    start_pipeline >> [
        Append_Easyecom_All_Return_Order,
        Append_Countries,
        Append_Customers,
        Append_Easyecom_Order_Status_History,
        Append_grn_details_report_sql_path,
        Append_Inventory_Aging_report,
        Append_Inventory_details,
        Append_Inventory_Snapshot,
        Append_Locations,
        Append_Marketplace,
        Append_Marketplace_listings,
        Append_Master_Product,
        Append_Order_Status_History,
        Append_Orders,
        Append_Pending_Returns_report,
        Append_Purchase_Orders,
        Append_Reports,
        Append_Return_report,
        Append_States,
        Append_Vendors,
        #Append_Mini_Sales_Report,
        Append_Tax_report_new,
        Append_Status_Wise_Stock_report,
        Append_grn_details,
        Append_inventory_view_by_bin_report,
        Append_Kits,
        Append_pending_return_orders,
    ]

    # Ensure Append_Order_items runs after Append_Orders
    Append_Orders >> Append_Order_items
    Append_Easyecom_All_Return_Order >> Append_All_Return_Order_items

    # Ensure all tasks finish at the end
    [
        Append_Easyecom_All_Return_Order,
        Append_Countries,
        Append_Customers,
        Append_Easyecom_Order_Status_History,
        Append_grn_details_report_sql_path,
        Append_Inventory_Aging_report,
        Append_Inventory_details,
        Append_Inventory_Snapshot,
        Append_Locations,
        Append_Marketplace,
        Append_Marketplace_listings,
        Append_Master_Product,
        Append_Order_Status_History,
        Append_Order_items,
        Append_Pending_Returns_report,
        Append_Purchase_Orders,
        Append_Reports,
        Append_Return_report,
        Append_States,
        Append_Vendors,
        Append_Status_Wise_Stock_report,
        Append_All_Return_Order_items,
        Append_grn_details,
        Append_inventory_view_by_bin_report,
        Append_Kits,
        #Append_Mini_Sales_Report,
        Append_Tax_report_new,
        Append_pending_return_orders,
    ] >> DW_Sanity_check

    DW_Sanity_check >> run_python_task
    run_python_task >> finish_pipeline

    # DW_Sanity_check >> finish_pipeline