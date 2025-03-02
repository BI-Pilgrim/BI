from airflow import DAG
from airflow.decorators import task, dag
from reviews.play_store.get_playstore_reviews import GooglePlayRatingsAPI, GooglePlayRatingPrivate
from datetime import datetime, timedelta

@dag("sync_reviews", schedule='0 7 * * *', start_date=datetime(year=2024,month=1,day=1), catchup=False, tags=["reviews", "daily"])
def sync_reviews():
    # order data sync
    @task.python
    def sync():
        GooglePlayRatingsAPI().sync_data()
    resp = sync()

@dag("sync_reviews_priv", schedule='0 7 * * *', start_date=datetime(year=2024,month=1,day=1), catchup=False, tags=["reviews", "daily"])
def sync_reviews_priv():
    # order data sync
    @task.python
    def sync():
        GooglePlayRatingsAPI().sync_data()
    resp = sync()

sync_reviews()
sync_reviews_priv()