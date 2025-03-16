from datetime import datetime, timedelta
import logging
from airflow.decorators import task, dag
from airflow.models import Variable
from google.oauth2 import service_account
from google.cloud import bigquery
import base64
import json
import pandas as pd
from massist.scraper import MassistAPI
from utils.google_cloud import get_bq_client


TABLE_NAME_ALL_DATA = "pilgrim_bi_massist.order_data"
TABLE_NAME_ALL_COUNTING = "pilgrim_bi_massist.order_counting"

@dag("massist_order_data", schedule='0 7 * * *', start_date=datetime(year=2024, month=12, day=1), tags=["massist"], catchup=False)
def massist_order_data():
    @task.python
    def fetch_and_store_data():
        start_date = (datetime.today() - timedelta(days=1))
        end_date = datetime.today()
        
        api = MassistAPI()
        massist_response = api.fetch_order_data(start_date, end_date)
        
        df_all_data = pd.DataFrame([data.model_dump() for data in massist_response.AllData])
        df_all_counting = pd.DataFrame([data.model_dump() for data in massist_response.AllCounting])
        
        df_all_data["pg_extracted_at"] = datetime.now()
        df_all_data["pg_start_date"] = start_date
        df_all_data["pg_end_date"] = end_date
        df_all_counting["pg_extracted_at"] = datetime.now()
        df_all_counting["pg_start_date"] = start_date
        df_all_counting["pg_end_date"] = end_date
        
        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        
        client = get_bq_client(credentials_info)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        try:
            job = client.load_table_from_dataframe(df_all_data, TABLE_NAME_ALL_DATA, job_config=job_config)
            job.result()
            job2 = client.load_table_from_dataframe(df_all_counting, TABLE_NAME_ALL_COUNTING, job_config=job_config)
            job2.result()
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            raise e
    
    fetch_and_store_data()

dag = massist_order_data()
