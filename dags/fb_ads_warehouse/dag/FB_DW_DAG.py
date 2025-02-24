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
from fb_ads_warehouse.python.FB_DW_Sanity_Check_mail import send_sanity_check_email  # Import the function from the script
LOCATION = "US"
# SQL_DIR = "../dags/fb_ads_warehouse/sql/fb_ads_to_bq"
SQL_DIR = "/home/airflow/gcs/dags/fb_ads_warehouse/sql/fb_ads_to_bq"
# Add the path where Amazon_Seller_DW_Sanity_check_mail.py is located
# sys.path.append('/home/airflow/gcs/dags/fb_ads_warehouse/python')




# Define the start date in UTC 
START_DATE = timezone.datetime(2025, 1, 13, 7, 55, 0, tzinfo=timezone.utc)  # Corresponds to 1.15 PM IST on 2025-01-02

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "shopify-pubsub-project"
DATASET = "Data_Warehouse_Amazon_Seller_Staging"
LOCATION = "asia-south1"  # Ensure this matches your dataset location

DATASET_STAGING = "Data_Warehouse_Amazon_Seller_Staging"


default_args = {
    'owner': 'omkar.sadawarte@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': START_DATE,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='FB_DW_DAG',
    schedule_interval='30 23 * * *',  # Cron expression for 5 AM IST (11:30 PM UTC)
    default_args=default_args,
    catchup=False
) as dag:
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

    # activities Table Refresh - Append
    activities_sql_path = os.path.join(SQL_DIR, "activities/activities_append.sql")
    with open(activities_sql_path, 'r') as file:
        sql_query_1 = file.read()

    append_activities = BigQueryInsertJobOperator(
        task_id='append_activities',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads Staging Table Refresh - Append
    ads_sql_path = os.path.join(SQL_DIR, "ads/ads_append.sql")
    with open(ads_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ads = BigQueryInsertJobOperator(
        task_id='append_ads',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_carousel_card_conversion_values Staging Table Refresh - Append
    ads_insights_action_carousel_card_conversion_values_sql_path = os.path.join(SQL_DIR, "ads_insights_action_carousel_card_conversion_values/ads_insights_action_carousel_card_conversion_values_append.sql")
    with open(ads_insights_action_carousel_card_conversion_values_sql_path, 'r') as file:
        sql_query_3 = file.read()

    append_ads_insights_action_carousel_card_conversion_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_carousel_card_conversion_values',
        configuration={
            "query": {
                "query": sql_query_3,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_carousel_card_cost_per_conversion Staging Table Refresh - Append
    ads_insights_action_carousel_card_cost_per_conversion_sql_path = os.path.join(SQL_DIR, "ads_insights_action_carousel_card_cost_per_conversion/ads_insights_action_carousel_card_cost_per_conversion_append.sql")
    with open(ads_insights_action_carousel_card_cost_per_conversion_sql_path, 'r') as file:
        sql_query_4 = file.read()

    append_ads_insights_action_carousel_card_cost_per_conversion = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_carousel_card_cost_per_conversion',
        configuration={
            "query": {
                "query": sql_query_4,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_carousel_card_mobile_app_purchase_roas Staging Table Refresh - Append
    ads_insights_action_carousel_card_mobile_app_purchase_roas_sql_path = os.path.join(SQL_DIR, "ads_insights_action_carousel_card_mobile_app_purchase_roas/ads_insights_action_carousel_card_mobile_app_purchase_roas_append.sql")
    with open(ads_insights_action_carousel_card_mobile_app_purchase_roas_sql_path, 'r') as file:
        sql_query_5 = file.read()

    append_ads_insights_action_carousel_card_mobile_app_purchase_roas = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_carousel_card_mobile_app_purchase_roas',
        configuration={
            "query": {
                "query": sql_query_5,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_carousel_card_non_json Staging Table Refresh - Append
    ads_insights_action_carousel_card_non_json_sql_path = os.path.join(SQL_DIR, "ads_insights_action_carousel_card_non_json/ads_insights_action_carousel_card_non_json_append.sql")
    with open(ads_insights_action_carousel_card_non_json_sql_path, 'r') as file:
        sql_query_6 = file.read()

    append_ads_insights_action_carousel_card_non_json = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_carousel_card_non_json',
        configuration={
            "query": {
                "query": sql_query_6,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr Staging Table Refresh - Append
    ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr_sql_path = os.path.join(SQL_DIR, "ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr/ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr_append.sql")
    with open(ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr_sql_path, 'r') as file:
        sql_query_7 = file.read()

    append_ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr',
        configuration={
            "query": {
                "query": sql_query_7,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_carousel_card_video_play_actions Staging Table Refresh - Append
    ads_insights_action_carousel_card_video_play_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_carousel_card_video_play_actions/ads_insights_action_carousel_card_video_play_actions_append.sql")
    with open(ads_insights_action_carousel_card_video_play_actions_sql_path, 'r') as file:
        sql_query_8 = file.read()

    append_ads_insights_action_carousel_card_video_play_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_carousel_card_video_play_actions',
        configuration={
            "query": {
                "query": sql_query_8,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_conversion_device_actions Staging Table Refresh - Append
    ads_insights_action_conversion_device_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_conversion_device_actions/ads_insights_action_conversion_device_actions_append.sql")
    with open(ads_insights_action_conversion_device_actions_sql_path, 'r') as file:
        sql_query_9 = file.read()

    append_ads_insights_action_conversion_device_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_conversion_device_actions',
        configuration={
            "query": {
                "query": sql_query_9,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_conversion_device_action_values Staging Table Refresh - Append
    ads_insights_action_conversion_device_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_action_conversion_device_action_values/ads_insights_action_conversion_device_action_values_append.sql")
    with open(ads_insights_action_conversion_device_action_values_sql_path, 'r') as file:
        sql_query_10 = file.read()

    append_ads_insights_action_conversion_device_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_conversion_device_action_values',
        configuration={
            "query": {
                "query": sql_query_10,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_conversion_device_cost_per_unique_action_type Staging Table Refresh - Append
    ads_insights_action_conversion_device_cost_per_unique_action_type_sql_path = os.path.join(SQL_DIR, "ads_insights_action_conversion_device_cost_per_unique_action_type/ads_insights_action_conversion_device_cost_per_unique_action_type_append.sql")
    with open(ads_insights_action_conversion_device_cost_per_unique_action_type_sql_path, 'r') as file:
        sql_query_11 = file.read()

    append_ads_insights_action_conversion_device_cost_per_unique_action_type = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_conversion_device_cost_per_unique_action_type',
        configuration={
            "query": {
                "query": sql_query_11,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_conversion_device_normal Staging Table Refresh - Append
    ads_insights_action_conversion_device_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_action_conversion_device_normal/ads_insights_action_conversion_device_normal_append.sql")
    with open(ads_insights_action_conversion_device_normal_sql_path, 'r') as file:
        sql_query_12 = file.read()

    append_ads_insights_action_conversion_device_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_conversion_device_normal',
        configuration={
            "query": {
                "query": sql_query_12,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_conversion_device_unique_actions Staging Table Refresh - Append
    ads_insights_action_conversion_device_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_conversion_device_unique_actions/ads_insights_action_conversion_device_unique_actions_append.sql")
    with open(ads_insights_action_conversion_device_unique_actions_sql_path, 'r') as file:
        sql_query_13 = file.read()

    append_ads_insights_action_conversion_device_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_conversion_device_unique_actions',
        configuration={
            "query": {
                "query": sql_query_13,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_product_id_actions Staging Table Refresh - Append
    ads_insights_action_product_id_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_product_id_actions/ads_insights_action_product_id_actions_append.sql")
    with open(ads_insights_action_product_id_actions_sql_path, 'r') as file:
        sql_query_14 = file.read()

    append_ads_insights_action_product_id_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_product_id_actions',
        configuration={
            "query": {
                "query": sql_query_14,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_product_id_action_values Staging Table Refresh - Append
    ads_insights_action_product_id_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_action_product_id_action_values/ads_insights_action_product_id_action_values_append.sql")
    with open(ads_insights_action_product_id_action_values_sql_path, 'r') as file:
        sql_query_15 = file.read()

    append_ads_insights_action_product_id_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_product_id_action_values',
        configuration={
            "query": {
                "query": sql_query_15,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_product_id_cost_per_action_type Staging Table Refresh - Append
    ads_insights_action_product_id_cost_per_action_type_sql_path = os.path.join(SQL_DIR, "ads_insights_action_product_id_cost_per_action_type/ads_insights_action_product_id_cost_per_action_type_append.sql")
    with open(ads_insights_action_product_id_cost_per_action_type_sql_path, 'r') as file:
        sql_query_16 = file.read()

    append_ads_insights_action_product_id_cost_per_action_type = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_product_id_cost_per_action_type',
        configuration={
            "query": {
                "query": sql_query_16,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_product_id_normal Staging Table Refresh - Append
    ads_insights_action_product_id_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_action_product_id_normal/ads_insights_action_product_id_normal_append.sql")
    with open(ads_insights_action_product_id_normal_sql_path, 'r') as file:
        sql_query_17 = file.read()

    append_ads_insights_action_product_id_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_product_id_normal',
        configuration={
            "query": {
                "query": sql_query_17,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_product_id_purchase_roas Staging Table Refresh - Append
    ads_insights_action_product_id_purchase_roas_sql_path = os.path.join(SQL_DIR, "ads_insights_action_product_id_purchase_roas/ads_insights_action_product_id_purchase_roas_append.sql")
    with open(ads_insights_action_product_id_purchase_roas_sql_path, 'r') as file:
        sql_query_18 = file.read()

    append_ads_insights_action_product_id_purchase_roas = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_product_id_purchase_roas',
        configuration={
            "query": {
                "query": sql_query_18,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_reaction_actions Staging Table Refresh - Append
    ads_insights_action_reaction_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_reaction_actions/ads_insights_action_reaction_actions_append.sql")
    with open(ads_insights_action_reaction_actions_sql_path, 'r') as file:
        sql_query_19 = file.read()

    append_ads_insights_action_reaction_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_reaction_actions',
        configuration={
            "query": {
                "query": sql_query_19,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_reaction_action_values Staging Table Refresh - Append
    ads_insights_action_reaction_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_action_reaction_action_values/ads_insights_action_reaction_action_values_append.sql")
    with open(ads_insights_action_reaction_action_values_sql_path, 'r') as file:
        sql_query_20 = file.read()

    append_ads_insights_action_reaction_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_reaction_action_values',
        configuration={
            "query": {
                "query": sql_query_20,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_reaction_conversion_values Staging Table Refresh - Append
    ads_insights_action_reaction_conversion_values_sql_path = os.path.join(SQL_DIR, "ads_insights_action_reaction_conversion_values/ads_insights_action_reaction_conversion_values_append.sql")
    with open(ads_insights_action_reaction_conversion_values_sql_path, 'r') as file:
        sql_query_21 = file.read()

    append_ads_insights_action_reaction_conversion_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_reaction_conversion_values',
        configuration={
            "query": {
                "query": sql_query_21,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_reaction_normal Staging Table Refresh - Append
    ads_insights_action_reaction_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_action_reaction_normal/ads_insights_action_reaction_normal_append.sql")
    with open(ads_insights_action_reaction_normal_sql_path, 'r') as file:
        sql_query_22 = file.read()

    append_ads_insights_action_reaction_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_reaction_normal',
        configuration={
            "query": {
                "query": sql_query_22,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_action_type_actions Staging Table Refresh - Append
    ads_insights_action_type_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_type_actions/ads_insights_action_type_actions_append.sql")
    with open(ads_insights_action_type_actions_sql_path, 'r') as file:
        sql_query_23 = file.read()

    append_ads_insights_action_type_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_type_actions',
        configuration={
            "query": {
                "query": sql_query_23,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_type_action_values Staging Table Refresh - Append
    ads_insights_action_type_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_action_type_action_values/ads_insights_action_type_action_values_append.sql")
    with open(ads_insights_action_type_action_values_sql_path, 'r') as file:
        sql_query_24 = file.read()

    append_ads_insights_action_type_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_type_action_values',
        configuration={
            "query": {
                "query": sql_query_24,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_type_cost_per_action_type Staging Table Refresh - Append
    ads_insights_action_type_cost_per_action_type_sql_path = os.path.join(SQL_DIR, "ads_insights_action_type_cost_per_action_type/ads_insights_action_type_cost_per_action_type_append.sql")
    with open(ads_insights_action_type_cost_per_action_type_sql_path, 'r') as file:
        sql_query_25 = file.read()

    append_ads_insights_action_type_cost_per_action_type = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_type_cost_per_action_type',
        configuration={
            "query": {
                "query": sql_query_25,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_type_cost_per_unique_action_type Staging Table Refresh - Append
    ads_insights_action_type_cost_per_unique_action_type_sql_path = os.path.join(SQL_DIR, "ads_insights_action_type_cost_per_unique_action_type/ads_insights_action_type_cost_per_unique_action_type_append.sql")
    with open(ads_insights_action_type_cost_per_unique_action_type_sql_path, 'r') as file:
        sql_query_26 = file.read()

    append_ads_insights_action_type_cost_per_unique_action_type = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_type_cost_per_unique_action_type',
        configuration={
            "query": {
                "query": sql_query_26,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_type_normal Staging Table Refresh - Append
    ads_insights_action_type_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_action_type_normal/ads_insights_action_type_normal_append.sql")
    with open(ads_insights_action_type_normal_sql_path, 'r') as file:
        sql_query_27 = file.read()

    append_ads_insights_action_type_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_type_normal',
        configuration={
            "query": {
                "query": sql_query_27,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_type_unique_actions Staging Table Refresh - Append
    ads_insights_action_type_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_type_unique_actions/ads_insights_action_type_unique_actions_append.sql")
    with open(ads_insights_action_type_unique_actions_sql_path, 'r') as file:
        sql_query_28 = file.read()

    append_ads_insights_action_type_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_type_unique_actions',
        configuration={
            "query": {
                "query": sql_query_28,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_video_sound_actions Staging Table Refresh - Append
    ads_insights_action_video_sound_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_video_sound_actions/ads_insights_action_video_sound_actions_append.sql")
    with open(ads_insights_action_video_sound_actions_sql_path, 'r') as file:
        sql_query_29 = file.read()

    append_ads_insights_action_video_sound_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_video_sound_actions',
        configuration={
            "query": {
                "query": sql_query_29,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_video_sound_action_values Staging Table Refresh - Append
    ads_insights_action_video_sound_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_action_video_sound_action_values/ads_insights_action_video_sound_action_values_append.sql")
    with open(ads_insights_action_video_sound_action_values_sql_path, 'r') as file:
        sql_query_30 = file.read()

    append_ads_insights_action_video_sound_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_video_sound_action_values',
        configuration={
            "query": {
                "query": sql_query_30,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_video_sound_normal Staging Table Refresh - Append
    ads_insights_action_video_sound_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_action_video_sound_normal/ads_insights_action_video_sound_normal_append.sql")
    with open(ads_insights_action_video_sound_normal_sql_path, 'r') as file:
        sql_query_31 = file.read()

    append_ads_insights_action_video_sound_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_video_sound_normal',
        configuration={
            "query": {
                "query": sql_query_31,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_video_type_actions Staging Table Refresh - Append
    ads_insights_action_video_type_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_video_type_actions/ads_insights_action_video_type_actions_append.sql")
    with open(ads_insights_action_video_type_actions_sql_path, 'r') as file:
        sql_query_32 = file.read()

    append_ads_insights_action_video_type_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_video_type_actions',
        configuration={
            "query": {
                "query": sql_query_32,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_video_type_action_values Staging Table Refresh - Append
    ads_insights_action_video_type_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_action_video_type_action_values/ads_insights_action_video_type_action_values_append.sql")
    with open(ads_insights_action_video_type_action_values_sql_path, 'r') as file:
        sql_query_33 = file.read()

    append_ads_insights_action_video_type_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_video_type_action_values',
        configuration={
            "query": {
                "query": sql_query_33,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_video_type_normal Staging Table Refresh - Append
    ads_insights_action_video_type_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_action_video_type_normal/ads_insights_action_video_type_normal_append.sql")
    with open(ads_insights_action_video_type_normal_sql_path, 'r') as file:
        sql_query_34 = file.read()

    append_ads_insights_action_video_type_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_video_type_normal',
        configuration={
            "query": {
                "query": sql_query_34,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_video_type_unique_actions Staging Table Refresh - Append
    ads_insights_action_video_type_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_video_type_unique_actions/ads_insights_action_video_type_unique_actions_append.sql")
    with open(ads_insights_action_video_type_unique_actions_sql_path, 'r') as file:
        sql_query_35 = file.read()

    append_ads_insights_action_video_type_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_video_type_unique_actions',
        configuration={
            "query": {
                "query": sql_query_35,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_age_and_gender_actions Staging Table Refresh - Append
    ads_insights_age_and_gender_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_age_and_gender_actions/ads_insights_age_and_gender_actions_append.sql")
    with open(ads_insights_age_and_gender_actions_sql_path, 'r') as file:
        sql_query_36 = file.read()

    append_ads_insights_age_and_gender_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_age_and_gender_actions',
        configuration={
            "query": {
                "query": sql_query_36,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_age_and_gender_action_values Staging Table Refresh - Append
    ads_insights_age_and_gender_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_age_and_gender_action_values/ads_insights_age_and_gender_action_values_append.sql")
    with open(ads_insights_age_and_gender_action_values_sql_path, 'r') as file:
        sql_query_37 = file.read()

    append_ads_insights_age_and_gender_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_age_and_gender_action_values',
        configuration={
            "query": {
                "query": sql_query_37,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_age_and_gender_normal Staging Table Refresh - Append
    ads_insights_age_and_gender_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_age_and_gender_normal/ads_insights_age_and_gender_normal_append.sql")
    with open(ads_insights_age_and_gender_normal_sql_path, 'r') as file:
        sql_query_38 = file.read()

    append_ads_insights_age_and_gender_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_age_and_gender_normal',
        configuration={
            "query": {
                "query": sql_query_38,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_age_and_gender_unique_actions Staging Table Refresh - Append
    ads_insights_age_and_gender_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_age_and_gender_unique_actions/ads_insights_age_and_gender_unique_actions_append.sql")
    with open(ads_insights_age_and_gender_unique_actions_sql_path, 'r') as file:
        sql_query_39 = file.read()

    append_ads_insights_age_and_gender_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_age_and_gender_unique_actions',
        configuration={
            "query": {
                "query": sql_query_39,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_country_actions Staging Table Refresh - Append
    ads_insights_country_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_country_actions/ads_insights_country_actions_append.sql")
    with open(ads_insights_country_actions_sql_path, 'r') as file:
        sql_query_40 = file.read()

    append_ads_insights_country_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_country_actions',
        configuration={
            "query": {
                "query": sql_query_40,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_country_normal Staging Table Refresh - Append
    ads_insights_country_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_country_normal/ads_insights_country_normal_append.sql")
    with open(ads_insights_country_normal_sql_path, 'r') as file:
        sql_query_41 = file.read()

    append_ads_insights_country_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_country_normal',
        configuration={
            "query": {
                "query": sql_query_41,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_country_unique_actions Staging Table Refresh - Append
    ads_insights_country_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_country_unique_actions/ads_insights_country_unique_actions_append.sql")
    with open(ads_insights_country_unique_actions_sql_path, 'r') as file:
        sql_query_42 = file.read()

    append_ads_insights_country_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_country_unique_actions',
        configuration={
            "query": {
                "query": sql_query_42,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_device_actions Staging Table Refresh - Append
    ads_insights_delivery_device_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_device_actions/ads_insights_delivery_device_actions_append.sql")
    with open(ads_insights_delivery_device_actions_sql_path, 'r') as file:
        sql_query_43 = file.read()

    append_ads_insights_delivery_device_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_device_actions',
        configuration={
            "query": {
                "query": sql_query_43,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_device_action_values Staging Table Refresh - Append
    ads_insights_delivery_device_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_device_action_values/ads_insights_delivery_device_action_values_append.sql")
    with open(ads_insights_delivery_device_action_values_sql_path, 'r') as file:
        sql_query_44 = file.read()

    append_ads_insights_delivery_device_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_device_action_values',
        configuration={
            "query": {
                "query": sql_query_44,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_device_cost_per_action_type Staging Table Refresh - Append
    ads_insights_delivery_device_cost_per_action_type_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_device_cost_per_action_type/ads_insights_delivery_device_cost_per_action_type_append.sql")
    with open(ads_insights_delivery_device_cost_per_action_type_sql_path, 'r') as file:
        sql_query_45 = file.read()

    append_ads_insights_delivery_device_cost_per_action_type = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_device_cost_per_action_type',
        configuration={
            "query": {
                "query": sql_query_45,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_device_cost_per_unique_action_type Staging Table Refresh - Append
    ads_insights_delivery_device_cost_per_unique_action_type_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_device_cost_per_unique_action_type/ads_insights_delivery_device_cost_per_unique_action_type_append.sql")
    with open(ads_insights_delivery_device_cost_per_unique_action_type_sql_path, 'r') as file:
        sql_query_46 = file.read()

    append_ads_insights_delivery_device_cost_per_unique_action_type = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_device_cost_per_unique_action_type',
        configuration={
            "query": {
                "query": sql_query_46,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_device_normal Staging Table Refresh - Append
    ads_insights_delivery_device_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_device_normal/ads_insights_delivery_device_normal_append.sql")
    with open(ads_insights_delivery_device_normal_sql_path, 'r') as file:
        sql_query_47 = file.read()

    append_ads_insights_delivery_device_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_device_normal',
        configuration={
            "query": {
                "query": sql_query_47,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_device_unique_actions Staging Table Refresh - Append
    ads_insights_delivery_device_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_device_unique_actions/ads_insights_delivery_device_unique_actions_append.sql")
    with open(ads_insights_delivery_device_unique_actions_sql_path, 'r') as file:
        sql_query_48 = file.read()

    append_ads_insights_delivery_device_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_device_unique_actions',
        configuration={
            "query": {
                "query": sql_query_48,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_platform_actions Staging Table Refresh - Append
    ads_insights_delivery_platform_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_platform_actions/ads_insights_delivery_platform_actions_append.sql")
    with open(ads_insights_delivery_platform_actions_sql_path, 'r') as file:
        sql_query_49 = file.read()

    append_ads_insights_delivery_platform_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_platform_actions',
        configuration={
            "query": {
                "query": sql_query_49,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_platform_action_values Staging Table Refresh - Append
    ads_insights_delivery_platform_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_platform_action_values/ads_insights_delivery_platform_action_values_append.sql")
    with open(ads_insights_delivery_platform_action_values_sql_path, 'r') as file:
        sql_query_50 = file.read()

    append_ads_insights_delivery_platform_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_platform_action_values',
        configuration={
            "query": {
                "query": sql_query_50,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_platform_and_device_platform_actions Staging Table Refresh - Append
    ads_insights_delivery_platform_and_device_platform_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_platform_and_device_platform_actions/ads_insights_delivery_platform_and_device_platform_actions_append.sql")
    with open(ads_insights_delivery_platform_and_device_platform_actions_sql_path, 'r') as file:
        sql_query_51 = file.read()

    append_ads_insights_delivery_platform_and_device_platform_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_platform_and_device_platform_actions',
        configuration={
            "query": {
                "query": sql_query_51,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_platform_and_device_platform_normal Staging Table Refresh - Append
    ads_insights_delivery_platform_and_device_platform_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_platform_and_device_platform_normal/ads_insights_delivery_platform_and_device_platform_normal_append.sql")
    with open(ads_insights_delivery_platform_and_device_platform_normal_sql_path, 'r') as file:
        sql_query_52 = file.read()

    append_ads_insights_delivery_platform_and_device_platform_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_platform_and_device_platform_normal',
        configuration={
            "query": {
                "query": sql_query_52,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_platform_cost_per_action_type Staging Table Refresh - Append
    ads_insights_delivery_platform_cost_per_action_type_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_platform_cost_per_action_type/ads_insights_delivery_platform_cost_per_action_type_append.sql")
    with open(ads_insights_delivery_platform_cost_per_action_type_sql_path, 'r') as file:
        sql_query_53 = file.read()

    append_ads_insights_delivery_platform_cost_per_action_type = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_platform_cost_per_action_type',
        configuration={
            "query": {
                "query": sql_query_53,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_platform_cost_per_unique_action_type Staging Table Refresh - Append
    ads_insights_delivery_platform_cost_per_unique_action_type_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_platform_cost_per_unique_action_type/ads_insights_delivery_platform_cost_per_unique_action_type_append.sql")
    with open(ads_insights_delivery_platform_cost_per_unique_action_type_sql_path, 'r') as file:
        sql_query_54 = file.read()

    append_ads_insights_delivery_platform_cost_per_unique_action_type = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_platform_cost_per_unique_action_type',
        configuration={
            "query": {
                "query": sql_query_54,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_platform_normal Staging Table Refresh - Append
    ads_insights_delivery_platform_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_platform_normal/ads_insights_delivery_platform_normal_append.sql")
    with open(ads_insights_delivery_platform_normal_sql_path, 'r') as file:
        sql_query_55 = file.read()

    append_ads_insights_delivery_platform_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_platform_normal',
        configuration={
            "query": {
                "query": sql_query_55,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_delivery_platform_unique_actions Staging Table Refresh - Append
    ads_insights_delivery_platform_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_platform_unique_actions/ads_insights_delivery_platform_unique_actions_append.sql")
    with open(ads_insights_delivery_platform_unique_actions_sql_path, 'r') as file:
        sql_query_56 = file.read()

    append_ads_insights_delivery_platform_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_platform_unique_actions',
        configuration={
            "query": {
                "query": sql_query_56,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_demographics_age_normal Staging Table Refresh - Append
    ads_insights_demographics_age_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_age_normal/ads_insights_demographics_age_normal_append.sql")
    with open(ads_insights_demographics_age_normal_sql_path, 'r') as file:
        sql_query_57 = file.read()

    append_ads_insights_demographics_age_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_age_normal',
        configuration={
            "query": {
                "query": sql_query_57,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_demographics_country_normal Staging Table Refresh - Append
    ads_insights_demographics_country_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_country_normal/ads_insights_demographics_country_normal_append.sql")
    with open(ads_insights_demographics_country_normal_sql_path, 'r') as file:
        sql_query_58 = file.read()

    append_ads_insights_demographics_country_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_country_normal',
        configuration={
            "query": {
                "query": sql_query_58,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_demographics_dma_region_normal Staging Table Refresh - Append
    ads_insights_demographics_dma_region_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_dma_region_normal/ads_insights_demographics_dma_region_normal_append.sql")
    with open(ads_insights_demographics_dma_region_normal_sql_path, 'r') as file:
        sql_query_59 = file.read()

    append_ads_insights_demographics_dma_region_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_dma_region_normal',
        configuration={
            "query": {
                "query": sql_query_59,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_demographics_gender_normal Staging Table Refresh - Append
    ads_insights_demographics_gender_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_gender_normal/ads_insights_demographics_gender_normal_append.sql")
    with open(ads_insights_demographics_gender_normal_sql_path, 'r') as file:
        sql_query_60 = file.read()

    append_ads_insights_demographics_gender_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_gender_normal',
        configuration={
            "query": {
                "query": sql_query_60,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_dma_normal Staging Table Refresh - Append
    ads_insights_dma_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_dma_normal/ads_insights_dma_normal_append.sql")
    with open(ads_insights_dma_normal_sql_path, 'r') as file:
        sql_query_61 = file.read()

    append_ads_insights_dma_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_dma_normal',
        configuration={
            "query": {
                "query": sql_query_61,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_normal Staging Table Refresh - Append
    ads_insights_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_normal/ads_insights_normal_append.sql")
    with open(ads_insights_normal_sql_path, 'r') as file:
        sql_query_62 = file.read()

    append_ads_insights_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_normal',
        configuration={
            "query": {
                "query": sql_query_62,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_platform_and_device_normal Staging Table Refresh - Append
    ads_insights_platform_and_device_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_platform_and_device_normal/ads_insights_platform_and_device_normal_append.sql")
    with open(ads_insights_platform_and_device_normal_sql_path, 'r') as file:
        sql_query_63 = file.read()

    append_ads_insights_platform_and_device_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_platform_and_device_normal',
        configuration={
            "query": {
                "query": sql_query_63,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_region_normal Staging Table Refresh - Append
    ads_insights_region_normal_sql_path = os.path.join(SQL_DIR, "ads_insights_region_normal/ads_insights_region_normal_append.sql")
    with open(ads_insights_region_normal_sql_path, 'r') as file:
        sql_query_64 = file.read()

    append_ads_insights_region_normal = BigQueryInsertJobOperator(
        task_id='append_ads_insights_region_normal',
        configuration={
            "query": {
                "query": sql_query_64,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_sets Staging Table Refresh - Append
    ad_sets_sql_path = os.path.join(SQL_DIR, "ad_sets/ad_sets_append.sql")
    with open(ad_sets_sql_path, 'r') as file:
        sql_query_65 = file.read()

    append_ad_sets = BigQueryInsertJobOperator(
        task_id='append_ad_sets',
        configuration={
            "query": {
                "query": sql_query_65,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # campaigns Staging Table Refresh - Append
    campaigns_sql_path = os.path.join(SQL_DIR, "campaigns/campaigns_append.sql")
    with open(campaigns_sql_path, 'r') as file:
        sql_query_66 = file.read()

    append_campaigns = BigQueryInsertJobOperator(
        task_id='append_campaigns',
        configuration={
            "query": {
                "query": sql_query_66,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # clicks Staging Table Refresh - Append
    clicks_sql_path = os.path.join(SQL_DIR, "ads_insights_clicks/ads_insights_clicks_append.sql")
    with open(clicks_sql_path, 'r') as file:
        sql_query_67 = file.read()

    append_clicks = BigQueryInsertJobOperator(
        task_id='append_clicks',
        configuration={
            "query": {
                "query": sql_query_67,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # conversion_data Staging Table Refresh - Append
    conversion_data_sql_path = os.path.join(SQL_DIR, "conversion_data/conversion_data_append.sql")
    with open(conversion_data_sql_path, 'r') as file:
        sql_query_68 = file.read()

    append_conversion_data = BigQueryInsertJobOperator(
        task_id='append_conversion_data',
        configuration={
            "query": {
                "query": sql_query_68,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # images Staging Table Refresh - Append
    images_sql_path = os.path.join(SQL_DIR, "images/images_append.sql")
    with open(images_sql_path, 'r') as file:
        sql_query_69 = file.read()

    append_images = BigQueryInsertJobOperator(
        task_id='append_images',
        configuration={
            "query": {
                "query": sql_query_69,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # roas Staging Table Refresh - Append
    roas_sql_path = os.path.join(SQL_DIR, "roas/roas_append.sql")
    with open(roas_sql_path, 'r') as file:
        sql_query_70 = file.read()

    append_roas = BigQueryInsertJobOperator(
        task_id='append_roas',
        configuration={
            "query": {
                "query": sql_query_70,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # videos Staging Table Refresh - Append
    videos_sql_path = os.path.join(SQL_DIR, "videos/videos_append.sql")
    with open(videos_sql_path, 'r') as file:
        sql_query_71 = file.read()

    append_videos = BigQueryInsertJobOperator(
        task_id='append_videos',
        configuration={
            "query": {
                "query": sql_query_71,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # video_2sec_video15_30sec_and_video_avg_details Staging Table Refresh - Append
    video_2sec_video15_30sec_and_video_avg_details_sql_path = os.path.join(SQL_DIR, "video_2sec_video15_30sec_and_video_avg_details/video_2sec_video15_30sec_and_video_avg_details_append.sql")
    with open(video_2sec_video15_30sec_and_video_avg_details_sql_path, 'r') as file:
        sql_query_72 = file.read()

    append_video_2sec_video15_30sec_and_video_avg_details = BigQueryInsertJobOperator(
        task_id='append_video_2sec_video15_30sec_and_video_avg_details',
        configuration={
            "query": {
                "query": sql_query_72,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # video_actions Staging Table Refresh - Append
    video_actions_sql_path = os.path.join(SQL_DIR, "video_actions/video_actions_append.sql")
    with open(video_actions_sql_path, 'r') as file:
        sql_query_73 = file.read()

    append_video_actions = BigQueryInsertJobOperator(
        task_id='append_video_actions',
        configuration={
            "query": {
                "query": sql_query_73,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # video_p25_to_p100_details Staging Table Refresh - Append
    video_p25_to_p100_details_sql_path = os.path.join(SQL_DIR, "video_p25_to_p100_details/video_p25_to_p100_details_append.sql")
    with open(video_p25_to_p100_details_sql_path, 'r') as file:
        sql_query_74 = file.read()

    append_video_p25_to_p100_details = BigQueryInsertJobOperator(
        task_id='append_video_p25_to_p100_details',
        configuration={
            "query": {
                "query": sql_query_74,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_action_reaction_unique_actions Staging Table Refresh - Append
    ads_insights_action_reaction_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_action_reaction_unique_actions/ads_insights_action_reaction_unique_actions_append.sql")
    with open(ads_insights_action_reaction_unique_actions_sql_path, 'r') as file:
        sql_query_75 = file.read()

    append_ads_insights_action_reaction_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_action_reaction_unique_actions',
        configuration={
            "query": {
                "query": sql_query_75,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_country_action_values Staging Table Refresh - Append
    ads_insights_country_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_country_action_values/ads_insights_country_action_values_append.sql")
    with open(ads_insights_country_action_values_sql_path, 'r') as file:
        sql_query_76 = file.read()

    append_ads_insights_country_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_country_action_values',
        configuration={
            "query": {
                "query": sql_query_76,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_creatives Staging Table Refresh - Append
    ad_creatives_sql_path = os.path.join(SQL_DIR, "ad_creatives/ad_creatives_append.sql")
    with open(ad_creatives_sql_path, 'r') as file:
        sql_query_77 = file.read()

    append_ad_creatives = BigQueryInsertJobOperator(
        task_id='append_ad_creatives',
        configuration={
            "query": {
                "query": sql_query_77,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_excluded_audience Staging Table Refresh - Append
    ads_excluded_audience_sql_path = os.path.join(SQL_DIR, "ads_excluded_audience/ads_excluded_audience_append.sql")
    with open(ads_excluded_audience_sql_path, 'r') as file:
        sql_query_78 = file.read()

    append_ads_excluded_audience = BigQueryInsertJobOperator(
        task_id='append_ads_excluded_audience',
        configuration={
            "query": {
                "query": sql_query_78,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_conversion_data Staging Table Refresh - Append
    ads_insights_conversion_data_sql_path = os.path.join(SQL_DIR, "ads_insights_conversion_data/ads_insights_conversion_data_append.sql")
    with open(ads_insights_conversion_data_sql_path, 'r') as file:
        sql_query_79 = file.read()

    append_ads_insights_conversion_data = BigQueryInsertJobOperator(
        task_id='append_ads_insights_conversion_data',
        configuration={
            "query": {
                "query": sql_query_79,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_delivery_platform_and_device_platform_cost_per_action_type  Staging Table Refresh - Append
    ads_insights_delivery_platform_and_device_platform_cost_per_action_type_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_platform_and_device_platform_cost_per_action_type/ads_insights_delivery_platform_and_device_platform_cost_per_action_type_append.sql")
    with open(ads_insights_country_action_values_sql_path, 'r') as file:
        sql_query_80 = file.read()

    append_ads_insights_delivery_platform_and_device_platform_cost_per_action_type = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_platform_and_device_platform_cost_per_action_type',
        configuration={
            "query": {
                "query": sql_query_80,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_delivery_platform_and_device_platform_unique_actions  Staging Table Refresh - Append
    ads_insights_delivery_platform_and_device_platform_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_delivery_platform_and_device_platform_unique_actions/ads_insights_delivery_platform_and_device_platform_unique_actions_append.sql")
    with open(ads_insights_delivery_platform_and_device_platform_unique_actions_sql_path, 'r') as file:
        sql_query_81 = file.read()

    append_ads_insights_delivery_platform_and_device_platform_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_delivery_platform_and_device_platform_unique_actions',
        configuration={
            "query": {
                "query": sql_query_81,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_demographics_age_action_values Staging Table Refresh - Append
    ads_insights_demographics_age_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_age_action_values/ads_insights_demographics_age_action_values_append.sql")
    with open(ads_insights_demographics_age_action_values_sql_path, 'r') as file:
        sql_query_82 = file.read()

    append_ads_insights_demographics_age_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_age_action_values',
        configuration={
            "query": {
                "query": sql_query_82,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_demographics_age_actions Staging Table Refresh - Append
    ads_insights_demographics_age_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_age_actions/ads_insights_demographics_age_actions_append.sql")
    with open(ads_insights_demographics_age_actions_sql_path, 'r') as file:
        sql_query_83 = file.read()

    append_ads_insights_demographics_age_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_age_actions',
        configuration={
            "query": {
                "query": sql_query_83,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_demographics_age_cost_per_action_type Staging Table Refresh - Append
    ads_insights_demographics_age_cost_per_action_type_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_age_cost_per_action_type/ads_insights_demographics_age_cost_per_action_type_append.sql")
    with open(ads_insights_demographics_age_cost_per_action_type_sql_path, 'r') as file:
        sql_query_84 = file.read()

    append_ads_insights_demographics_age_cost_per_action_type = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_age_cost_per_action_type',
        configuration={
            "query": {
                "query": sql_query_84,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_demographics_age_unique_actions Staging Table Refresh - Append
    ads_insights_demographics_age_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_age_unique_actions/ads_insights_demographics_age_unique_actions_append.sql")
    with open(ads_insights_demographics_age_unique_actions_sql_path, 'r') as file:
        sql_query_85 = file.read()

    append_ads_insights_demographics_age_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_age_unique_actions',
        configuration={
            "query": {
                "query": sql_query_85,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_demographics_country_action_values Staging Table Refresh - Append
    ads_insights_demographics_country_action_values_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_country_action_values/ads_insights_demographics_country_action_values_append.sql")
    with open(ads_insights_demographics_country_action_values_sql_path, 'r') as file:
        sql_query_86 = file.read()

    append_ads_insights_demographics_country_action_values = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_country_action_values',
        configuration={
            "query": {
                "query": sql_query_86,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_demographics_country_actions Staging Table Refresh - Append
    ads_insights_demographics_country_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_country_actions/ads_insights_demographics_country_actions_append.sql")
    with open(ads_insights_demographics_country_actions_sql_path, 'r') as file:
        sql_query_87 = file.read()

    append_ads_insights_demographics_country_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_country_actions',
        configuration={
            "query": {
                "query": sql_query_87,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_demographics_country_unique_actions Staging Table Refresh - Append
    ads_insights_demographics_country_unique_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_demographics_country_unique_actions/ads_insights_demographics_country_unique_actions_append.sql")
    with open(ads_insights_demographics_country_unique_actions_sql_path, 'r') as file:
        sql_query_88 = file.read()

    append_ads_insights_demographics_country_unique_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_demographics_country_unique_actions',
        configuration={
            "query": {
                "query": sql_query_88,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )


    # ads_insights_video_2sec_video15_30sec_and_video_avg_details Staging Table Refresh - Append
    ads_insights_video_2sec_video15_30sec_and_video_avg_details_sql_path = os.path.join(SQL_DIR, "ads_insights_video_2sec_video15_30sec_and_video_avg_details/ads_insights_video_2sec_video15_30sec_and_video_avg_details_append.sql")
    with open(ads_insights_video_2sec_video15_30sec_and_video_avg_details_sql_path, 'r') as file:
        sql_query_89 = file.read()

    append_ads_insights_video_2sec_video15_30sec_and_video_avg_details = BigQueryInsertJobOperator(
        task_id='append_ads_insights_video_2sec_video15_30sec_and_video_avg_details',
        configuration={
            "query": {
                "query": sql_query_89,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_video_actions  Staging Table Refresh - Append
    ads_insights_video_actions_sql_path = os.path.join(SQL_DIR, "ads_insights_video_actions/ads_insights_video_actions_append.sql")
    with open(ads_insights_country_action_values_sql_path, 'r') as file:
        sql_query_90 = file.read()

    append_ads_insights_video_actions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_video_actions',
        configuration={
            "query": {
                "query": sql_query_90,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_video_p25_to_p100_details Staging Table Refresh - Append
    ads_insights_video_p25_to_p100_details_sql_path = os.path.join(SQL_DIR, "ads_insights_video_p25_to_p100_details/ads_insights_video_p25_to_p100_details_append.sql")
    with open(ads_insights_video_p25_to_p100_details_sql_path, 'r') as file:
        sql_query_91 = file.read()

    append_ads_insights_video_p25_to_p100_details = BigQueryInsertJobOperator(
        task_id='append_ads_insights_video_p25_to_p100_details',
        configuration={
            "query": {
                "query": sql_query_91,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_non_json Staging Table Refresh - Append
    ads_non_json_sql_path = os.path.join(SQL_DIR, "ads_non_json/ads_non_json_append.sql")
    with open(ads_non_json_sql_path, 'r') as file:
        sql_query_92 = file.read()

    append_ads_non_json = BigQueryInsertJobOperator(
        task_id='append_ads_non_json',
        configuration={
            "query": {
                "query": sql_query_92,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_recommendations Staging Table Refresh - Append
    ads_recommendations_sql_path = os.path.join(SQL_DIR, "ads_recommendations/ads_recommendations_append.sql")
    with open(ads_recommendations_sql_path, 'r') as file:
        sql_query_93 = file.read()

    append_ads_recommendations = BigQueryInsertJobOperator(
        task_id='append_ads_recommendations',
        configuration={
            "query": {
                "query": sql_query_93,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_isnghts_conversions Staging Table Refresh - Append
    ads_isnghts_conversions_sql_path = os.path.join(SQL_DIR, "ads_insights_conversion_values/ads_isnghts_conversion_values_append.sql")
    with open(ads_recommendations_sql_path, 'r') as file:
        sql_query_94 = file.read()

    append_ads_isnghts_conversions = BigQueryInsertJobOperator(
        task_id='append_ads_isnghts_conversions',
        configuration={
            "query": {
                "query": sql_query_94,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_conversions Staging Table Refresh - Append
    ads_insights_conversions_sql_path = os.path.join(SQL_DIR, "ads_insights_conversions/ads_insights_conversions_append.sql")
    with open(ads_recommendations_sql_path, 'r') as file:
        sql_query_95 = file.read()

    append_ads_insights_conversions = BigQueryInsertJobOperator(
        task_id='append_ads_insights_conversions',
        configuration={
            "query": {
                "query": sql_query_95,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ads_insights_cost_per_conv_value Staging Table Refresh - Append
    ads_insights_cost_per_conv_value_sql_path = os.path.join(SQL_DIR, "ads_insights_cost_per_conv_value/ads_insights_cost_per_conv_value_append.sql")
    with open(ads_recommendations_sql_path, 'r') as file:
        sql_query_96 = file.read()

    append_ads_insights_cost_per_conv_value = BigQueryInsertJobOperator(
        task_id='append_ads_insights_cost_per_conv_value',
        configuration={
            "query": {
                "query": sql_query_96,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )



    # # sanity_check Staging Table Refresh - Append
    # sanity_check_sql_path = "../dags/fb_ads_warehouse/sql/datawarehouse_sanity_check/sanity_check.sql"
    # with open(sanity_check_sql_path, 'r') as file:
    #     sql_query_94 = file.read()

    # perform_sanity_check = BigQueryInsertJobOperator(
    #     task_id='perform_sanity_check',
    #     configuration={
    #         "query": {
    #             "query": sql_query_94,
    #             "useLegacySql": False,
    #         },
    #         "location": LOCATION,
    #     }
    # )
  

# Sanity check Table 

    # Load SQL query from file
    with open('/home/airflow/gcs/dags/fb_ads_warehouse/sql/datawarehouse_sanity_check/sanity_check.sql', 'r') as file:
    # with open('../dags/fb_ads_warehouse/sql/datawarehouse_sanity_check/sanity_check.sql', 'r') as file:
        sql_query_95 = file.read()

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

    # End of the pipeline
    finish_pipeline = DummyOperator(
        task_id='finish_pipeline'
    )   

    # Task Orchestration
    start_pipeline >> [ append_activities,
                        append_ads,
                        append_ads_insights_action_carousel_card_conversion_values,
                        append_ads_insights_action_carousel_card_cost_per_conversion,
                        append_ads_insights_action_carousel_card_mobile_app_purchase_roas,
                        append_ads_insights_action_carousel_card_non_json,
                        append_ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr,
                        append_ads_insights_action_carousel_card_video_play_actions,
                        append_ads_insights_action_conversion_device_actions,
                        append_ads_insights_action_conversion_device_action_values,
                        append_ads_insights_action_conversion_device_cost_per_unique_action_type,
                        append_ads_insights_action_conversion_device_normal,
                        append_ads_insights_action_conversion_device_unique_actions,
                        append_ads_insights_action_product_id_actions,
                        append_ads_insights_action_product_id_action_values,
                        append_ads_insights_action_product_id_cost_per_action_type,
                        append_ads_insights_action_product_id_normal,
                        append_ads_insights_action_product_id_purchase_roas,
                        append_ads_insights_action_reaction_actions,
                        append_ads_insights_action_reaction_action_values,
                        append_ads_insights_action_reaction_conversion_values,
                        append_ads_insights_action_reaction_normal,
                        append_ads_insights_action_reaction_unique_actions,
                        append_ads_insights_action_type_actions,
                        append_ads_insights_action_type_action_values,
                        append_ads_insights_action_type_cost_per_action_type,
                        append_ads_insights_action_type_cost_per_unique_action_type,
                        append_ads_insights_action_type_normal,
                        append_ads_insights_action_type_unique_actions,
                        append_ads_insights_action_video_sound_actions,
                        append_ads_insights_action_video_sound_action_values,
                        append_ads_insights_action_video_sound_normal,
                        append_ads_insights_action_video_type_actions,
                        append_ads_insights_action_video_type_action_values,
                        append_ads_insights_action_video_type_normal,
                        append_ads_insights_action_video_type_unique_actions,
                        append_ads_insights_age_and_gender_actions,
                        append_ads_insights_age_and_gender_action_values,
                        append_ads_insights_age_and_gender_normal,
                        append_ads_insights_age_and_gender_unique_actions,
                        append_ads_insights_country_actions,
                        append_ads_insights_country_action_values,
                        append_ads_insights_country_normal,
                        append_ads_insights_country_unique_actions,
                        append_ads_insights_delivery_device_actions,
                        append_ads_insights_delivery_device_action_values,
                        append_ads_insights_delivery_device_cost_per_action_type,
                        append_ads_insights_delivery_device_cost_per_unique_action_type,
                        append_ads_insights_delivery_device_normal,
                        append_ads_insights_delivery_device_unique_actions,
                        append_ads_insights_delivery_platform_actions,
                        append_ads_insights_delivery_platform_action_values,
                        append_ads_insights_delivery_platform_and_device_platform_actions,
                        append_ads_insights_delivery_platform_and_device_platform_normal,
                        append_ads_insights_delivery_platform_cost_per_action_type,
                        append_ads_insights_delivery_platform_cost_per_unique_action_type,
                        append_ads_insights_delivery_platform_normal,
                        append_ads_insights_delivery_platform_unique_actions,
                        append_ads_insights_demographics_age_normal,
                        append_ads_insights_demographics_country_normal,
                        append_ads_insights_demographics_dma_region_normal,
                        append_ads_insights_demographics_gender_normal,
                        append_ads_insights_dma_normal,
                        append_ads_insights_normal,
                        append_ads_insights_platform_and_device_normal,
                        append_ads_insights_region_normal,
                        append_ad_sets,
                        append_campaigns,
                        append_clicks,
                        # append_conversion_data,
                        append_images,
                        append_roas,
                        append_videos,
                        append_video_2sec_video15_30sec_and_video_avg_details,
                        append_video_actions,
                        append_video_p25_to_p100_details,
                        append_ads_recommendations,
                        append_ads_non_json,
                        append_ads_insights_video_p25_to_p100_details,
                        append_ads_insights_video_actions,
                        append_ads_insights_video_2sec_video15_30sec_and_video_avg_details,
                        append_ads_insights_demographics_country_unique_actions,
                        append_ads_insights_demographics_country_action_values,
                        append_ads_insights_demographics_age_unique_actions,
                        append_ads_insights_demographics_age_cost_per_action_type,
                        append_ads_insights_demographics_age_actions,
                        append_ads_insights_demographics_age_action_values,
                        append_ads_excluded_audience,
                        append_ads_insights_delivery_platform_and_device_platform_cost_per_action_type,
                        append_ads_insights_conversion_data,
                        append_ads_insights_demographics_country_actions,
                        append_ads_insights_delivery_platform_and_device_platform_unique_actions,
                        append_ad_creatives,
                        append_ads_isnghts_conversions,
                        append_ads_insights_conversions,

                      ]

    [ append_activities,
      append_ads,
      append_ads_insights_action_carousel_card_conversion_values,
      append_ads_insights_action_carousel_card_cost_per_conversion,
      append_ads_insights_action_carousel_card_mobile_app_purchase_roas,
      append_ads_insights_action_carousel_card_non_json,
      append_ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr,
      append_ads_insights_action_carousel_card_video_play_actions,
      append_ads_insights_action_conversion_device_actions,
      append_ads_insights_action_conversion_device_action_values,
      append_ads_insights_action_conversion_device_cost_per_unique_action_type,
      append_ads_insights_action_conversion_device_normal,
      append_ads_insights_action_conversion_device_unique_actions,
      append_ads_insights_action_product_id_actions,
      append_ads_insights_action_product_id_action_values,
      append_ads_insights_action_product_id_cost_per_action_type,
      append_ads_insights_action_product_id_normal,
      append_ads_insights_action_product_id_purchase_roas,
      append_ads_insights_action_reaction_actions,
      append_ads_insights_action_reaction_action_values,
      append_ads_insights_action_reaction_conversion_values,
      append_ads_insights_action_reaction_normal,
      append_ads_insights_action_reaction_unique_actions,
      append_ads_insights_action_type_actions,
      append_ads_insights_action_type_action_values,
      append_ads_insights_action_type_cost_per_action_type,
      append_ads_insights_action_type_cost_per_unique_action_type,
      append_ads_insights_action_type_normal,
      append_ads_insights_action_type_unique_actions,
      append_ads_insights_action_video_sound_actions,
      append_ads_insights_action_video_sound_action_values,
      append_ads_insights_action_video_sound_normal,
      append_ads_insights_action_video_type_actions,
      append_ads_insights_action_video_type_action_values,
      append_ads_insights_action_video_type_normal,
      append_ads_insights_action_video_type_unique_actions,
      append_ads_insights_age_and_gender_actions,
      append_ads_insights_age_and_gender_action_values,
      append_ads_insights_age_and_gender_normal,
      append_ads_insights_age_and_gender_unique_actions,
      append_ads_insights_country_actions,
      append_ads_insights_country_action_values,
      append_ads_insights_country_normal,
      append_ads_insights_country_unique_actions,
      append_ads_insights_delivery_device_actions,
      append_ads_insights_delivery_device_action_values,
      append_ads_insights_delivery_device_cost_per_action_type,
      append_ads_insights_delivery_device_cost_per_unique_action_type,
      append_ads_insights_delivery_device_normal,
      append_ads_insights_delivery_device_unique_actions,
      append_ads_insights_delivery_platform_actions,
      append_ads_insights_delivery_platform_action_values,
      append_ads_insights_delivery_platform_and_device_platform_actions,
      append_ads_insights_delivery_platform_and_device_platform_normal,
      append_ads_insights_delivery_platform_cost_per_action_type,
      append_ads_insights_delivery_platform_cost_per_unique_action_type,
      append_ads_insights_delivery_platform_normal,
      append_ads_insights_delivery_platform_unique_actions,
      append_ads_insights_demographics_age_normal,
      append_ads_insights_demographics_country_normal,
      append_ads_insights_demographics_dma_region_normal,
      append_ads_insights_demographics_gender_normal,
      append_ads_insights_dma_normal,
      append_ads_insights_normal,
      append_ads_insights_platform_and_device_normal,
      append_ads_insights_region_normal,
      append_ad_sets,
      append_campaigns,
      append_clicks,
      append_conversion_data,
      append_images,
      append_roas,
      append_videos,
      append_video_2sec_video15_30sec_and_video_avg_details,
      append_video_actions,
      append_video_p25_to_p100_details,
      append_ads_recommendations,
      append_ads_non_json,
      append_ads_insights_video_p25_to_p100_details,
      append_ads_insights_video_actions,
      append_ads_insights_video_2sec_video15_30sec_and_video_avg_details,
      append_ads_insights_demographics_country_unique_actions,
      append_ads_insights_demographics_country_action_values,
      append_ads_insights_demographics_age_unique_actions,
      append_ads_insights_demographics_age_cost_per_action_type,
      append_ads_insights_demographics_age_actions,
      append_ads_insights_demographics_age_action_values,
      append_ads_excluded_audience,
      append_ads_insights_delivery_platform_and_device_platform_cost_per_action_type,
    #   append_ads_insights_conversion_data,
      append_ads_insights_demographics_country_actions,
      append_ads_insights_delivery_platform_and_device_platform_unique_actions,
      append_ad_creatives,
      append_ads_isnghts_conversions,
      append_ads_insights_conversions
    ] >> sanity_check >> finish_pipeline
    
    sanity_check >> run_python_task
    run_python_task >> finish_pipeline   
