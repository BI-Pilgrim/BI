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

@dag("massist_order_data", schedule='0 7 * * *', start_date=datetime(year=2024, month=12, day=1))
def massist_order_data():
    @task.python
    def fetch_and_store_data():
        start_date = (datetime.today() - timedelta(days=1)).strftime("%m/%d/%Y")
        end_date = datetime.today().strftime("%m/%d/%Y")
        
        api = MassistAPI()
        massist_response = api.fetch_order_data(start_date, end_date)
        
        df_all_data = pd.DataFrame([data.model_dump() for data in massist_response.AllData])
        df_all_counting = pd.DataFrame([data.model_dump() for data in massist_response.AllCounting])
        
        df_all_data["pg_extracted_at"] = datetime.now()
        df_all_counting["pg_extracted_at"] = datetime.now()
        
        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        
        client = get_bq_client(credentials_info)
        client.insert_rows_from_dataframe(TABLE_NAME, df, selected_fields=table.schema)
    
    fetch_and_store_data()

dag = massist_order_data()
