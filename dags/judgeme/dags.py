from airflow import DAG
from airflow.utils.dates import days_ago
from utils.google_cloud import get_bq_client
import requests
from airflow.decorators import dag, tasks


# Define default arguments

@dag("judgeme", schedule='2 0 * * *', start_date=days_ago(1), tags=["judgeme"])
def judgeme_scraper():
    
    @tasks.python
    def fetch_greatest_id():
        client = get_bq_client()
        query = """
            SELECT MAX(id) as max_id
            FROM `your_project.your_dataset.your_table`
        """
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            return row.max_id

    @tasks.python
    def sync_reviews(max_id):
        
        if not reviews:
            break
        # Process and insert reviews into BigQuery
        # ...

