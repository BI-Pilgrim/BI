

# Import Functions 
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago, timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator, BigQueryExecuteQueryOperator


# Define the start date in UTC 
START_DATE = timezone.datetime(2025, 2, 25, 7, 55, 0, tzinfo=timezone.utc)  # Corresponds to 1.15 PM IST on 2025-01-02
default_args = {
    'owner': 'shafiq@discoverpilgrim.com',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'start_date': START_DATE,
    'retry_delay': timedelta(minutes=5),
}

# Define constants
LOCATION = "asia-south1" 

with DAG(
    dag_id='Goonj_sync',
    schedule_interval='30 23 7 * *',  # Cron expression for 5 AM IST (11:30 PM UTC)
    default_args=default_args,
    catchup=False
) as dag:
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )
    # Google Play store rating Table
    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Project_Goonj/Monhtly_dashboard/sql/Google_Playstor_Ratings.sql', 'r') as file:
        sql_query_1 = file.read()
        google_playstore = BigQueryExecuteQueryOperator(
        task_id='google_playstore',
        gcp_conn_id="google_cloud_default",  # Ensure this is set correctly
        location="asia-south1",  # Change based on your dataset location
        impersonation_chain=["composer-bi-scheduling@shopify-pubsub-project.iam.gserviceaccount.com"],
        sql=sql_query_1,
        use_legacy_sql=False,
    )

    # Market place rating table
    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Project_Goonj/Monhtly_dashboard/sql/Market_Place_Ratings.sql', 'r') as file:
        sql_query_2 = file.read()
        Market_Place_Ratings = BigQueryExecuteQueryOperator(
        task_id='Market_Place_Ratings',
        gcp_conn_id="google_cloud_default",  # Ensure this is set correctly
        location="asia-south1",  # Change based on your dataset location
        impersonation_chain=["composer-bi-scheduling@shopify-pubsub-project.iam.gserviceaccount.com"],
        sql=sql_query_2,
        use_legacy_sql=False,
    )


    #Customer Mertic Table
    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Project_Goonj/Monhtly_dashboard/sql/Customer_Metrics_NR_PC_MF.sql', 'r') as file:
        sql_query_3 = file.read()
        Customer_Metrics = BigQueryExecuteQueryOperator(
        task_id='Customer_Metrics',
        gcp_conn_id="google_cloud_default",  # Ensure this is set correctly
        location="asia-south1",  # Change based on your dataset location
        impersonation_chain=["composer-bi-scheduling@shopify-pubsub-project.iam.gserviceaccount.com"],
        sql=sql_query_3,
        use_legacy_sql=False,
    )


    #State wise KPI table
    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Project_Goonj/Monhtly_dashboard/sql/State_wise_KPI.sql', 'r') as file:
        sql_query_4 = file.read()
        State_wise_KPI = BigQueryExecuteQueryOperator(
        task_id='State_wise_KPI',
        gcp_conn_id="google_cloud_default",  # Ensure this is set correctly
        location="asia-south1",  # Change based on your dataset location
        impersonation_chain=["composer-bi-scheduling@shopify-pubsub-project.iam.gserviceaccount.com"],
        sql=sql_query_4,
        use_legacy_sql=False,
    )


    #State wise AOV KPI
    # Load SQL query from file
    with open('/home/airflow/gcs/dags/Project_Goonj/Monhtly_dashboard/sql/State_wise_AOV_KPI.sql', 'r') as file:
        sql_query_5 = file.read()
        State_wise_AOV_KPI = BigQueryExecuteQueryOperator(
        task_id='State_wise_AOV_KPI',
        gcp_conn_id="google_cloud_default",  # Ensure this is set correctly
        location="asia-south1",  # Change based on your dataset location
        impersonation_chain=["composer-bi-scheduling@shopify-pubsub-project.iam.gserviceaccount.com"],
        sql=sql_query_5,
        use_legacy_sql=False,
    )
start_pipeline >> [google_playstore,MP_Ratings,Customer_Metrics,State_wise_KPI,State_wise_AOV_KPI]
