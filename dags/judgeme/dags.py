import datetime
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from utils.google_cloud import get_bq_client
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from judgeme.scraper import JudgeMeScraper
import pandas as pd


TABLE_NAME = "pilgrim_bi_judgeme.reviews"


def default_json_encoder(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    
# Define default arguments

def fetch_greatest_id(bq_client)->int:
    query = f"""
        SELECT MAX(id) as max_id
        FROM `{TABLE_NAME}`
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    for row in results:
        return row.max_id or 0


@dag("judgeme_sync", schedule='0 2 * * *', start_date=days_ago(1), tags=["judgeme"])
def judgeme_sync():

    @task.python
    def sync_reviews(max_id):
        api_key = Variable.get("judgeme_api_key")
        shop_domain = Variable.get("judgeme_shop_domain")
        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        bq_client = get_bq_client(credentials_info)

        scraper = JudgeMeScraper(api_key=api_key, shop_domain=shop_domain)
        max_id = fetch_greatest_id(bq_client)
        cur_id = max_id+1
        cur_page=1

        while cur_id>max_id:
            print(cur_page, cur_id, max_id)
            reviews = scraper.get_reviews(per_page=100, page=cur_page)
            if len(reviews.reviews)==0: break
            df = pd.DataFrame(reviews.model_dump().get("reviews", []))
            df["ee_extracted_at"] = datetime.datetime.now()
            df_to_insert = df.loc[df['id']>max_id]
            table = bq_client.get_table(TABLE_NAME)
            df_to_insert["reviewer"] = df_to_insert["reviewer"].apply(lambda x: json.loads(json.dumps(x, default=default_json_encoder)))
            df_to_insert["pictures"] = df_to_insert["pictures"].apply(lambda x: json.loads(json.dumps(x, default=default_json_encoder)))
            bq_client.insert_rows_from_dataframe(TABLE_NAME, df_to_insert, selected_fields=table.schema)
            cur_page = reviews.current_page+1
            cur_id = int(df.id.min())

    sync_reviews

dag_sync = judgeme_sync()
    


