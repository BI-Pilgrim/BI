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
SQL_DIR = "/home/airflow/gcs/dags/nykaa_warehouse/sql/nykaa_to_bq"  # Adjust this path if necessary
# SQL_DIR = "../dags/nykaa_warehouse/sql/nykaa_to_bq/"  # Adjust this path if necessary

# Define the DAG
with DAG(
    dag_id='Nykaa_DataWarehouse_DAG',
    default_args=default_args,
    description='A DAG to update the Nykaa staging tables from Nykaa raw tables',
    schedule_interval='30 3 * * *',  # 3:30 AM UTC is 9:00 AM IST
    start_date=timezone.datetime(2025, 1, 3),
    catchup=False,
) as dag:

    # Start of the pipeline
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    # appointment_adherence Staging Table Refresh - append
    appointment_adherence_sql_path = os.path.join(SQL_DIR, "appointment_adherence/appointment_adherence_append.sql")
    with open(appointment_adherence_sql_path, 'r') as file:
        sql_query_1 = file.read()

    appointment_adherence_append = BigQueryInsertJobOperator(
        task_id='appointment_adherence_append',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # assortments Staging Table Refresh - append
    assortments_sql_path = os.path.join(SQL_DIR, "assortments/assortments_append.sql")
    with open(assortments_sql_path, 'r') as file:
        sql_query_2 = file.read()

    assortments_append = BigQueryInsertJobOperator(
        task_id='assortments_append',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # # brand_lvl_dashboard Staging Table Refresh - append
    brand_lvl_dashboard_sql_path = os.path.join(SQL_DIR, "brand_lvl_dashboard/brand_lvl_dashboard_append.sql")
    with open(brand_lvl_dashboard_sql_path, 'r') as file:
        sql_query_3 = file.read()

    brand_lvl_dashboard_append = BigQueryInsertJobOperator(
        task_id='brand_lvl_dashboard_append',
        configuration={
            "query": {
                "query": sql_query_3,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # fill_summary Staging Table Refresh - append
    fill_summary_sql_path = os.path.join(SQL_DIR, "fill_summary/fill_summary_append.sql")
    with open(fill_summary_sql_path, 'r') as file:
        sql_query_4 = file.read()

    fill_summary_append = BigQueryInsertJobOperator(
        task_id='fill_summary_append',
        configuration={
            "query": {
                "query": sql_query_4,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # grn_details Staging Table Refresh - append
    grn_details_sql_path = os.path.join(SQL_DIR, "grn_details/grn_details_append.sql")
    with open(grn_details_sql_path, 'r') as file:
        sql_query_5 = file.read()

    grn_details_append = BigQueryInsertJobOperator(
        task_id='grn_details_append',
        configuration={
            "query": {
                "query": sql_query_5,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # inv_ageing Staging Table Refresh - append
    inv_ageing_sql_path = os.path.join(SQL_DIR, "inv_ageing/inv_ageing_append.sql")
    with open(inv_ageing_sql_path, 'r') as file:
        sql_query_6 = file.read()

    inv_ageing_append = BigQueryInsertJobOperator(
        task_id='inv_ageing_append',
        configuration={
            "query": {
                "query": sql_query_6,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # inward_discrepancy Staging Table Refresh - append
    inward_discrepancy_sql_path = os.path.join(SQL_DIR, "inward_discrepancy/inward_discrepancy_append.sql")
    with open(inward_discrepancy_sql_path, 'r') as file:
        sql_query_7 = file.read()

    inward_discrepancy_append = BigQueryInsertJobOperator(
        task_id='inward_discrepancy_append',
        configuration={
            "query": {
                "query": sql_query_7,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # # open_po_summary Staging Table Refresh - append
    # open_po_summary_sql_path = os.path.join(SQL_DIR, "open_po_summary/open_po_summary_append.sql")
    # with open(open_po_summary_sql_path, 'r') as file:
    #     sql_query_8 = file.read()

    # open_po_summary_append = BigQueryInsertJobOperator(
    #     task_id='open_po_summary_append',
    #     configuration={
    #         "query": {
    #             "query": sql_query_8,
    #             "useLegacySql": False,
    #         },
    #         "location": LOCATION,
    #     }
    # )

    # open_rtv Staging Table Refresh - append
    open_rtv_sql_path = os.path.join(SQL_DIR, "open_rtv/open_rtv_append.sql")
    with open(open_rtv_sql_path, 'r') as file:
        sql_query_9 = file.read()

    open_rtv_append = BigQueryInsertJobOperator(
        task_id='open_rtv_append',
        configuration={
            "query": {
                "query": sql_query_9,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # sku_inv Staging Table Refresh - append
    sku_inv_sql_path = os.path.join(SQL_DIR, "sku_inv/sku_inv_append.sql")
    with open(sku_inv_sql_path, 'r') as file:
        sql_query_10 = file.read()

    sku_inv_append = BigQueryInsertJobOperator(
        task_id='sku_inv_append',
        configuration={
            "query": {
                "query": sql_query_10,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # sku_level_fill Staging Table Refresh - append
    sku_level_fill_sql_path = os.path.join(SQL_DIR, "sku_level_fill/sku_level_fill_append.sql")
    with open(sku_level_fill_sql_path, 'r') as file:
        sql_query_11 = file.read()

    sku_level_fill_append = BigQueryInsertJobOperator(
        task_id='sku_level_fill_append',
        configuration={
            "query": {
                "query": sql_query_11,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # sku_lvl_dashboard Staging Table Refresh - append
    sku_lvl_dashboard_sql_path = os.path.join(SQL_DIR, "sku_lvl_dashboard/sku_lvl_dashboard_append.sql")
    with open(sku_lvl_dashboard_sql_path, 'r') as file:
        sql_query_12 = file.read()

    sku_lvl_dashboard_append = BigQueryInsertJobOperator(
        task_id='sku_lvl_dashboard_append',
        configuration={
            "query": {
                "query": sql_query_12,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # velocity_lvl_dashboard Staging Table Refresh - append
    velocity_lvl_dashboard_sql_path = os.path.join(SQL_DIR, "velocity_lvl_dashboard/velocity_lvl_dashboard_append.sql")
    with open(velocity_lvl_dashboard_sql_path, 'r') as file:
        sql_query_13 = file.read()

    velocity_lvl_dashboard_append = BigQueryInsertJobOperator(
        task_id='velocity_lvl_dashboard_append',
        configuration={
            "query": {
                "query": sql_query_13,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # Sanity Check of all table
    # Load SQL query from file
    # with open("../dags/nykaa_warehouse/sql/datawarehouse_sanity_check/nykaa_sanity_check.sql", 'r') as file:
    with open("/home/airflow/gcs/dags/nykaa_warehouse/sql/datawarehouse_sanity_check/nykaa_sanity_check.sql", 'r') as file:
        sql_query_14 = file.read()

        DW_Sanity_check = BigQueryInsertJobOperator(
        task_id='DW_Sanity_check',
        configuration={
            "query": {
                "query": sql_query_14,
                "useLegacySql": False,
                "location": LOCATION,
            }
        }
    )
        
    def run_main_script():
        # script_path = '../dags/nykaa_warehouse/dag/Nykaa_DAG.py'
        script_path = '/home/airflow/gcs/dags/nykaa_warehouse/dag/Nykaa_DW_DAG.py'
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
        appointment_adherence_append,
        assortments_append,
        brand_lvl_dashboard_append,
        fill_summary_append,
        grn_details_append,
        inv_ageing_append,
        inward_discrepancy_append,
        # open_po_summary_append,
        open_rtv_append,
        sku_inv_append,
        sku_level_fill_append,
        sku_lvl_dashboard_append,
        velocity_lvl_dashboard_append,
    ]

    # Ensure all tasks finish at the end
    [
        appointment_adherence_append,
        assortments_append,
        brand_lvl_dashboard_append,
        fill_summary_append,
        grn_details_append,
        inv_ageing_append,
        inward_discrepancy_append,
        # open_po_summary_append,
        open_rtv_append,
        sku_inv_append,
        sku_level_fill_append,
        sku_lvl_dashboard_append,
        velocity_lvl_dashboard_append,
    ] >> DW_Sanity_check

    DW_Sanity_check >> run_python_task
    run_python_task >> finish_pipeline