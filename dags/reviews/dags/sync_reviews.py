from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta

@dag("sync_reviews", schedule='0 7 * * *', start_date=datetime(year=2024,month=1,day=1), tags=["reviews", "daily"])
def sync_reviews():
    # order data sync
    @task.python
    def sync():
        from reviews.play_store.get_playstore_reviews import GooglePlayRatingsAPI
        GooglePlayRatingsAPI().sync_data()
    resp = sync()