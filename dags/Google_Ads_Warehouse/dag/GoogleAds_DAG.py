# Import Functions
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import os

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
SQL_DIR = "../dags/Google_Ads_Warehouse/sql/GoogleAds_to_bq"  # Adjust this path if necessary

# Define the DAG
with DAG(
    dag_id='google_ads_warehouse',
    default_args=default_args,
    description='A DAG to copy invoices from EasyEcom to S3 and update Google Ads tables.',
    schedule_interval='30 3 * * *',  # 3:30 AM UTC is 9:00 AM IST
    start_date=timezone.datetime(2025, 1, 3),
    catchup=False,
) as dag:

    # Start of the pipeline
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    # account_performance_report Table Refresh - Append
    account_performance_report_sql_path = os.path.join(SQL_DIR, "account_performance_report/account_performance_report_append.sql")
    with open(account_performance_report_sql_path, 'r') as file:
        sql_query_1 = file.read()

    append_account_performance_report = BigQueryInsertJobOperator(
        task_id='append_account_performance_report',
        configuration={
            "query": {
                "query": sql_query_1,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group Staging Table Refresh - Append
    ad_group_sql_path = os.path.join(SQL_DIR, "ad_group/ad_group_append.sql")
    with open(ad_group_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group = BigQueryInsertJobOperator(
        task_id='append_ad_group',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_App_ad_desc Staging Table Refresh - Append
    ad_group_ad_App_ad_desc_sql_path = os.path.join(SQL_DIR, "ad_group_ad_App_ad_desc/ad_group_ad_App_ad_desc_append.sql")
    with open(ad_group_ad_App_ad_desc_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_App_ad_desc = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_App_ad_desc',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_App_ad_head Staging Table Refresh - Append
    ad_group_ad_App_ad_head_sql_path = os.path.join(SQL_DIR, "ad_group_ad_App_ad_head/ad_group_ad_App_ad_head_append.sql")
    with open(ad_group_ad_App_ad_head_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_App_ad_head = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_App_ad_head',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_App_ad_img Staging Table Refresh - Append
    ad_group_ad_App_ad_img_sql_path = os.path.join(SQL_DIR, "ad_group_ad_App_ad_img/ad_group_ad_App_ad_img_append.sql")
    with open(ad_group_ad_App_ad_img_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_App_ad_img = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_App_ad_img',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_App_ad_yt_vids Staging Table Refresh - Append
    ad_group_ad_App_ad_yt_vids_sql_path = os.path.join(SQL_DIR, "ad_group_ad_App_ad_yt_vids/ad_group_ad_App_ad_yt_vids_append.sql")
    with open(ad_group_ad_App_ad_yt_vids_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_App_ad_yt_vids = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_App_ad_yt_vids',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_App_eng_ad_head Staging Table Refresh - Append
    ad_group_ad_App_eng_ad_head_sql_path = os.path.join(SQL_DIR, "ad_group_ad_App_eng_ad_head/ad_group_ad_App_eng_ad_head_append.sql")
    with open(ad_group_ad_App_eng_ad_head_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_App_eng_ad_head = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_App_eng_ad_head',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_App_eng_ad_vids Staging Table Refresh - Append
    ad_group_ad_App_eng_ad_vids_sql_path = os.path.join(SQL_DIR, "ad_group_ad_App_eng_ad_vids/ad_group_ad_App_eng_ad_vids_append.sql")
    with open(ad_group_ad_App_eng_ad_vids_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_App_eng_ad_vids = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_App_eng_ad_vids',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_final_app_urls Staging Table Refresh - Append
    ad_group_ad_final_app_urls_sql_path = os.path.join(SQL_DIR, "ad_group_ad_final_app_urls/ad_group_ad_final_app_urls_append.sql")
    with open(ad_group_ad_final_app_urls_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_final_app_urls = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_final_app_urls',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_label Staging Table Refresh - Append
    ad_group_ad_label_sql_path = os.path.join(SQL_DIR, "ad_group_ad_label/ad_group_ad_label_append.sql")
    with open(ad_group_ad_label_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_label = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_label',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_legacy Staging Table Refresh - Append
    ad_group_ad_legacy_sql_path = os.path.join(SQL_DIR, "ad_group_ad_legacy/ad_group_ad_legacy_append.sql")
    with open(ad_group_ad_legacy_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_legacy = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_legacy',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_normal Staging Table Refresh - Append
    ad_group_ad_normal_sql_path = os.path.join(SQL_DIR, "ad_group_ad_normal/ad_group_ad_normal_append.sql")
    with open(ad_group_ad_normal_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_normal = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_normal',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_respnsive_display_ad_head Staging Table Refresh - Append
    ad_group_ad_respnsive_display_ad_head_sql_path = os.path.join(SQL_DIR, "ad_group_ad_respnsive_display_ad_head/ad_group_ad_respnsive_display_ad_head_append.sql")
    with open(ad_group_ad_respnsive_display_ad_head_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_respnsive_display_ad_head = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_respnsive_display_ad_head',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_rresponsive_ad_long_head Staging Table Refresh - Append
    ad_group_ad_rresponsive_ad_long_head_sql_path = os.path.join(SQL_DIR, "ad_group_ad_rresponsive_ad_long_head/ad_group_ad_rresponsive_ad_long_head_append.sql")
    with open(ad_group_ad_rresponsive_ad_long_head_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_rresponsive_ad_long_head = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_rresponsive_ad_long_head',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_search_ad_desc Staging Table Refresh - Append
    ad_group_ad_search_ad_desc_sql_path = os.path.join(SQL_DIR, "ad_group_ad_search_ad_desc/ad_group_ad_search_ad_desc_append.sql")
    with open(ad_group_ad_search_ad_desc_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_search_ad_desc = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_search_ad_desc',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_url_custom_parameters Staging Table Refresh - Append
    ad_group_ad_url_custom_parameters_sql_path = os.path.join(SQL_DIR, "ad_group_ad_url_custom_parameters/ad_group_ad_url_custom_parameters_append.sql")
    with open(ad_group_ad_url_custom_parameters_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_url_custom_parameters = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_url_custom_parameters',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_video_responsive_ad_descriptions Staging Table Refresh - Append
    ad_group_ad_video_responsive_ad_descriptions_sql_path = os.path.join(SQL_DIR, "ad_group_ad_video_responsive_ad_descriptions/ad_group_ad_video_responsive_ad_descriptions_append.sql")
    with open(ad_group_ad_video_responsive_ad_descriptions_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_video_responsive_ad_descriptions = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_video_responsive_ad_descriptions',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_video_responsive_ad_headlines Staging Table Refresh - Append
    ad_group_ad_video_responsive_ad_headlines_sql_path = os.path.join(SQL_DIR, "ad_group_ad_video_responsive_ad_headlines/ad_group_ad_video_responsive_ad_headlines_append.sql")
    with open(ad_group_ad_video_responsive_ad_headlines_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_video_responsive_ad_headlines = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_video_responsive_ad_headlines',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_video_responsive_search_ad_headlines Staging Table Refresh - Append
    ad_group_ad_video_responsive_search_ad_headlines_sql_path = os.path.join(SQL_DIR, "ad_group_ad_video_responsive_search_ad_headlines/ad_group_ad_video_responsive_search_ad_headlines_append.sql")
    with open(ad_group_ad_video_responsive_search_ad_headlines_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_video_responsive_search_ad_headlines = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_video_responsive_search_ad_headlines',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_ad_vid_resp_ad_call_to_actions Staging Table Refresh - Append
    ad_group_ad_vid_resp_ad_call_to_actions_sql_path = os.path.join(SQL_DIR, "ad_group_ad_vid_resp_ad_call_to_actions/ad_group_ad_vid_resp_ad_call_to_actions_append.sql")
    with open(ad_group_ad_vid_resp_ad_call_to_actions_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_ad_vid_resp_ad_call_to_actions = BigQueryInsertJobOperator(
        task_id='append_ad_group_ad_vid_resp_ad_call_to_actions',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_bidding_strategy Staging Table Refresh - Append
    ad_group_bidding_strategy_sql_path = os.path.join(SQL_DIR, "ad_group_bidding_strategy/ad_group_bidding_strategy_append.sql")
    with open(ad_group_bidding_strategy_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_bidding_strategy = BigQueryInsertJobOperator(
        task_id='append_ad_group_bidding_strategy',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_criterion Staging Table Refresh - Append
    ad_group_criterion_sql_path = os.path.join(SQL_DIR, "ad_group_criterion/ad_group_criterion_append.sql")
    with open(ad_group_criterion_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_criterion = BigQueryInsertJobOperator(
        task_id='append_ad_group_criterion',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_criterion_label Staging Table Refresh - Append
    ad_group_criterion_label_sql_path = os.path.join(SQL_DIR, "ad_group_criterion_label/ad_group_criterion_label_append.sql")
    with open(ad_group_criterion_label_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_criterion_label = BigQueryInsertJobOperator(
        task_id='append_ad_group_criterion_label',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_group_label Staging Table Refresh - Append
    ad_group_label_sql_path = os.path.join(SQL_DIR, "ad_group_label/ad_group_label_append.sql")
    with open(ad_group_label_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_group_label = BigQueryInsertJobOperator(
        task_id='append_ad_group_label',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # ad_listing_group_criterion Staging Table Refresh - Append
    ad_listing_group_criterion_sql_path = os.path.join(SQL_DIR, "ad_listing_group_criterion/ad_listing_group_criterion_append.sql")
    with open(ad_listing_group_criterion_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_ad_listing_group_criterion = BigQueryInsertJobOperator(
        task_id='append_ad_listing_group_criterion',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # audience Staging Table Refresh - Append
    audience_sql_path = os.path.join(SQL_DIR, "audience/audience_append.sql")
    with open(audience_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_audience = BigQueryInsertJobOperator(
        task_id='append_audience',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # campaign Staging Table Refresh - Append
    campaign_sql_path = os.path.join(SQL_DIR, "campaign/campaign_append.sql")
    with open(campaign_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_campaign = BigQueryInsertJobOperator(
        task_id='append_campaign',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # campaign_bidding_strategy Staging Table Refresh - Append
    campaign_bidding_strategy_sql_path = os.path.join(SQL_DIR, "campaign_bidding_strategy/campaign_bidding_strategy_append.sql")
    with open(campaign_bidding_strategy_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_campaign_bidding_strategy = BigQueryInsertJobOperator(
        task_id='append_campaign_bidding_strategy',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # campaign_budget Staging Table Refresh - Append
    campaign_budget_sql_path = os.path.join(SQL_DIR, "campaign_budget/campaign_budget_append.sql")
    with open(campaign_budget_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_campaign_budget = BigQueryInsertJobOperator(
        task_id='append_campaign_budget',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # campaign_criterion Staging Table Refresh - Append
    campaign_criterion_sql_path = os.path.join(SQL_DIR, "campaign_criterion/campaign_criterion_append.sql")
    with open(campaign_criterion_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_campaign_criterion = BigQueryInsertJobOperator(
        task_id='append_campaign_criterion',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # campaign_label Staging Table Refresh - Append
    campaign_label_sql_path = os.path.join(SQL_DIR, "campaign_label/campaign_label_append.sql")
    with open(campaign_label_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_campaign_label = BigQueryInsertJobOperator(
        task_id='append_campaign_label',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # click_view Staging Table Refresh - Append
    click_view_sql_path = os.path.join(SQL_DIR, "click_view/click_view_append.sql")
    with open(click_view_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_click_view = BigQueryInsertJobOperator(
        task_id='append_click_view',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # customer Staging Table Refresh - Append
    customer_sql_path = os.path.join(SQL_DIR, "customer/customer_append.sql")
    with open(customer_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_customer = BigQueryInsertJobOperator(
        task_id='append_customer',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # display_keyword_view Staging Table Refresh - Append
    display_keyword_view_sql_path = os.path.join(SQL_DIR, "display_keyword_view/display_keyword_view_append.sql")
    with open(display_keyword_view_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_display_keyword_view = BigQueryInsertJobOperator(
        task_id='append_display_keyword_view',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # geographic_view Staging Table Refresh - Append
    geographic_view_sql_path = os.path.join(SQL_DIR, "geographic_view/geographic_view_append.sql")
    with open(geographic_view_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_geographic_view = BigQueryInsertJobOperator(
        task_id='append_geographic_view',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # keyword_view Staging Table Refresh - Append
    keyword_view_sql_path = os.path.join(SQL_DIR, "keyword_view/keyword_view_append.sql")
    with open(keyword_view_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_keyword_view = BigQueryInsertJobOperator(
        task_id='append_keyword_view',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # label Staging Table Refresh - Append
    label_sql_path = os.path.join(SQL_DIR, "label/label_append.sql")
    with open(label_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_label = BigQueryInsertJobOperator(
        task_id='append_label',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # shopping_performance_view Staging Table Refresh - Append
    shopping_performance_view_sql_path = os.path.join(SQL_DIR, "shopping_performance_view/shopping_performance_view_append.sql")
    with open(shopping_performance_view_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_shopping_performance_view = BigQueryInsertJobOperator(
        task_id='append_shopping_performance_view',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # user_interest Staging Table Refresh - Append
    user_interest_sql_path = os.path.join(SQL_DIR, "user_interest/user_interest_append.sql")
    with open(user_interest_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_user_interest = BigQueryInsertJobOperator(
        task_id='append_user_interest',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # user_location_view Staging Table Refresh - Append
    user_location_view_sql_path = os.path.join(SQL_DIR, "user_location_view/user_location_view_append.sql")
    with open(user_location_view_sql_path, 'r') as file:
        sql_query_2 = file.read()

    append_user_location_view = BigQueryInsertJobOperator(
        task_id='append_user_location_view',
        configuration={
            "query": {
                "query": sql_query_2,
                "useLegacySql": False,
            },
            "location": LOCATION,
        }
    )

    # End of the pipeline
    finish_pipeline = DummyOperator(
        task_id='finish_pipeline'
    )

    # Task Orchestration
    start_pipeline >> [append_account_performance_report, append_ad_group, append_ad_group_ad_App_ad_desc, append_ad_group_ad_App_ad_head, append_ad_group_ad_App_ad_img, append_ad_group_ad_App_ad_yt_vids, append_ad_group_ad_App_eng_ad_head, append_ad_group_ad_App_eng_ad_vids, append_ad_group_ad_final_app_urls, append_ad_group_ad_label, append_ad_group_ad_legacy, append_ad_group_ad_normal, append_ad_group_ad_respnsive_display_ad_head, append_ad_group_ad_rresponsive_ad_long_head, append_ad_group_ad_search_ad_desc, append_ad_group_ad_url_custom_parameters, append_ad_group_ad_video_responsive_ad_descriptions, append_ad_group_ad_video_responsive_ad_headlines, append_ad_group_ad_video_responsive_search_ad_headlines, append_ad_group_ad_vid_resp_ad_call_to_actions, append_ad_group_bidding_strategy, append_ad_group_criterion, append_ad_group_criterion_label, append_ad_group_label, append_ad_listing_group_criterion, append_audience, append_campaign, append_campaign_bidding_strategy, append_campaign_budget, append_campaign_criterion, append_campaign_label, append_click_view, append_customer, append_display_keyword_view, append_geographic_view, append_keyword_view, append_label, append_shopping_performance_view, append_user_interest, append_user_location_view]

[append_account_performance_report, append_ad_group, append_ad_group_ad_App_ad_desc, append_ad_group_ad_App_ad_head, append_ad_group_ad_App_ad_img, append_ad_group_ad_App_ad_yt_vids, append_ad_group_ad_App_eng_ad_head, append_ad_group_ad_App_eng_ad_vids, append_ad_group_ad_final_app_urls, append_ad_group_ad_label, append_ad_group_ad_legacy, append_ad_group_ad_normal, append_ad_group_ad_respnsive_display_ad_head, append_ad_group_ad_rresponsive_ad_long_head, append_ad_group_ad_search_ad_desc, append_ad_group_ad_url_custom_parameters, append_ad_group_ad_video_responsive_ad_descriptions, append_ad_group_ad_video_responsive_ad_headlines, append_ad_group_ad_video_responsive_search_ad_headlines, append_ad_group_ad_vid_resp_ad_call_to_actions, append_ad_group_bidding_strategy, append_ad_group_criterion, append_ad_group_criterion_label, append_ad_group_label, append_ad_listing_group_criterion, append_audience, append_campaign, append_campaign_bidding_strategy, append_campaign_budget, append_campaign_criterion, append_campaign_label, append_click_view, append_customer, append_display_keyword_view, append_geographic_view, append_keyword_view, append_label, append_shopping_performance_view, append_user_interest, append_user_location_view] >> finish_pipeline
