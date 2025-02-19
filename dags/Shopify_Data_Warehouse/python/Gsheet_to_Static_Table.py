import pandas as pd
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import base64 
from datetime import datetime
from airflow.models import Variable

def get_bq_client(credentials_info:str)->bigquery.Client:
    credentials_info = base64.b64decode(credentials_info).decode("utf-8")
    credentials_info = json.loads(credentials_info)

    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = bigquery.Client(credentials=credentials, project="shopify-pubsub-project")
    return client



def write_to_gbq(client,df,p_id,t_id):
    print('Inside the gbq write function ------------')
    # WRITE_APPEND to append in the existing table WRITE_TRUNCATE to create a new table by deleting the existing one
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = f"{p_id}.{t_id}" 
    job = client.load_table_from_dataframe(df, table_id,job_config=job_config) 
    job.result()  
    
    print(f"Data loaded successfully to {table_id}")

def read_from_gbq(bq_client,p_id,t_id):
    df = bq_client.query(f"SELECT * FROM `{p_id}.{t_id}`").to_dataframe() 
    return df

project_id = 'shopify-pubsub-project'
product_sku_mapping = 'adhoc_data_asia.Product_SKU_mapping_D2C'
product_sku_mapping_static = 'adhoc_data_asia.Product_SKU_mapping_D2C_static'

credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS") 
bq_client = get_bq_client(credentials_info)
product_list = read_from_gbq(bq_client,project_id, product_sku_mapping)  
write_to_gbq(bq_client,product_list,project_id,product_sku_mapping_static)
