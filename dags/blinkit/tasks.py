from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import task, dag
from airflow.models import Variable
from google.cloud import bigquery
from blinkit.scraper import BlinkItAdsScraper
from utils.google_cloud import get_bq_client

TABLE_NAME_PRODUCT_LISTING = "pilgrim_bi_blinkit.product_listing"
TABLE_NAME_BRAND_BOOSTER = "pilgrim_bi_blinkit.brand_booster"
TABLE_NAME_PRODUCT_RECOMMENDATION = "pilgrim_bi_blinkit.product_recommendation"

@dag("blinkit_ad_summary", schedule='0 7 * * *', start_date=datetime(year=2024, month=12, day=1), tags=["blinkit"], catchup=False)
def blinkit_ad_summary():
    @task.bash
    def install_playwright_chromium():
        return "playwright install-deps && playwright install chromium"

    @task.python
    def fetch_and_store_data():
        start_date = (datetime.today() - timedelta(days=2))
        end_date = (datetime.today() - timedelta(days=1))
        
        scraper = BlinkItAdsScraper()
        dfs = scraper.download_ad_summary(start_date, end_date)
        
        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        client = get_bq_client(credentials_info)
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        
        if not dfs:
            print("No reports found")
            return
        
        for table_name, df_key in zip([TABLE_NAME_PRODUCT_LISTING, TABLE_NAME_BRAND_BOOSTER, TABLE_NAME_PRODUCT_RECOMMENDATION], dfs):
            df = scraper._process_dataframe(dfs[df_key], start_date, end_date)
            try:
                job = client.load_table_from_dataframe(df, table_name, job_config=job_config)
                job.result()
            except Exception as e:
                print(f"Error loading data to BigQuery: {e}")
                raise e
    
    install_playwright_chromium() >> fetch_and_store_data()

dag = blinkit_ad_summary()
