from datetime import datetime
import requests
from airflow.models import Variable
from utils.google_cloud import get_playstore_token
from reviews.play_store.reviews_schema import GooglePlayRatings, GooglePlayRatingsPriv
from sqlalchemy import create_engine, inspect, MetaData, Table
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

from google_play_scraper import Sort, reviews, reviews_all

import os
import base64
import time
import json

def date_from_ts_s(ts_s):
    if ts_s is not None:
        return datetime.fromtimestamp(int(ts_s))
    return None
class GooglePlayRatingsAPI:
    dataset_id = "pilgrim_bi_google_play"
    project_id = "shopify-pubsub-project"
    package_name = "com.discoverpilgrim"
    table = GooglePlayRatings

    @property
    def table_id(self):
        return f'{self.project_id}.{self.dataset_id}.{self.table.__tablename__}'
    
    def __init__(self):
        # self.project_id = "shopify-pubsub-project"
        # self.dataset_id = "pilgrim_bi_google_play"
        # self.table_id = f'{self.project_id}.{self.dataset_id}.{self.table.__tablename__}'

        # BigQuery connection string
        self._initialize()

    def _initialize(self):
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.engine = create_engine(connection_string, credentials_info=credentials_info)

        self.create_table()

    def create_table(self):
        """Create table in BigQuery if it does not exist."""
        if not self.table_exists(self.table.__tablename__):
            print("Creating Google Play Ratings table in BigQuery")
            self.table.metadata.create_all(self.engine)
            print(f"{self.table_id} table created in BigQuery")
        else:
            print(f"{self.table_id} table already exists in BigQuery")

    def table_exists(self, table_name):
        """Check if table exists in BigQuery."""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def convert(self, val, _type, **kwargs):
        if val is None: return val
        try:
            if _type == datetime:
                if type(val) == datetime:
                    val = str(val)
                return datetime.strptime(val, kwargs["strptime"])
            return _type(val)
        except:
            return None

    def transform_data(self, data):
        """Transform the data into the required schema."""
        transformed_data = []
        for record in data:
            transformed_record = {
                "review_id": self.convert(record["reviewId"], str),
                "user_name": self.convert(record["userName"], str),
                "user_image": self.convert(record["userImage"], str),
                "content": self.convert(record["content"], str),
                "score": self.convert(record["score"], int),
                "thumbs_up_count": self.convert(record["thumbsUpCount"], int),
                "review_created_version": self.convert(record["reviewCreatedVersion"], str),
                "review_given_at": self.convert(record["at"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "reply_content": self.convert(record["replyContent"], str),
                "replied_at": self.convert(record["repliedAt"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "app_version": self.convert(record["appVersion"], str),
            }
            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self):
        """Sync data from the API to BigQuery."""
        reviews = self.get_data()
        if not reviews:
            print("No new reviews to sync")
            return

        print('Transforming Reviews data')
        transformed_data = self.transform_data(data=reviews)
        extrated_at = datetime.now()

        # Insert the transformed data into the table
        print("Truncating the table")
        self.truncate_table()
        print("Total no of reviews to sync: ", len(transformed_data))
        self.load_data_to_bigquery(transformed_data, extrated_at)

    def truncate_table(self):
        """Truncate the BigQuery table by deleting all rows."""
        table_ref = self.client.dataset(self.dataset_id).table(self.table.__tablename__)
        truncate_query = f"DELETE FROM `{table_ref}` WHERE true"
        self.client.query(truncate_query).result()  # Executes the DELETE query

    def load_data_to_bigquery(self, data, extracted_at, passing_df = False, _retry=0, max_retry=3):
        """Load the data into BigQuery."""
        print("Loading Google playstore reviews data to BigQuery")
        data = pd.DataFrame(data)
        data["ee_extracted_at"] = extracted_at

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        try:
            job = self.client.load_table_from_dataframe(data, self.table_id, job_config=job_config)
            job.result()
        except Exception as e:
            print(f"Error loading data to BigQuery: {e}")
            raise e

    def get_data(self):
        """Fetch Google Playstore reviews data from the the library."""
        all_reviews = []
        result = reviews_all(
            self.package_name,
            lang="en",  # defaults to 'en'
            country="us",  # defaults to 'us'
            sort=Sort.NEWEST,  # defaults to Sort.MOST_RELEVANT
        )
        return result

class GooglePlayRatingPrivate(GooglePlayRatingsAPI):
    """
    Can't get mor than last 7 days data from the API
    https://support.google.com/googleplay/android-developer/thread/239806165/google-play-api-list-reviews-only-returns-last-7-days-of-reviews-and-does-not-return-pagination?hl=en
    """
    table = GooglePlayRatingsPriv
    def __init__(self):
        # super().__init__()
        self.ACCESS_TOKEN = get_playstore_token(json.loads(Variable.get("GOOGLE_PLAYSTORE_TOKEN", "{}")))
        self.session = requests.Session()
        self.URL = f'https://www.googleapis.com/androidpublisher/v3/applications/{self.package_name}/reviews'
        self._initialize()
        # self.table_id = f'{self.project_id}.{self.dataset_id}.{GooglePlayRatingsPriv.__tablename__}'
    
    def get_max_stored_date(self):
        data = self.engine.execute(
            f"select max(userComment_lastModified_seconds) from {self.table_id}"
            ).fetchall()
        if(len(data) == 0 or data[0][0] is None): return datetime.min
        return data[0][0]
    
    def sync_data(self):
        """Sync data from the API to BigQuery."""

        print('Transforming Reviews data')
        last_stored_date = self.get_max_stored_date()
        transformed_data = [
            x for x in filter(
            lambda x: x.get("userComment_lastModified_seconds", datetime.max) > last_stored_date,
            (self.transform_data(review) for review in self.get_data())
            )
        ]
        extrated_at = datetime.now()

        print("Total no of reviews to sync: ", len(transformed_data))
        if(len(transformed_data)>0): self.load_data_to_bigquery(transformed_data, extrated_at)
    
    def get_data(self, next_token=None):
        
        params = {
            'access_token': self.ACCESS_TOKEN,
            'maxResults': 50,
            'token': next_token
        }
        while True:
            response = self.session.get(self.URL, params=params)
            if response.status_code == 200:
                data = response.json()
                reviews = data.get('reviews', [])
                for review in reviews:
                    yield review
                next_token = data.get('tokenPagination', {}).get('nextPageToken')
                params['token'] = next_token
                if not params['token']:
                    break
            else:
                print(f"Failed to fetch data: {response.status_code}, {response.text}")
                break   

    def transform_data(self, data):
        user_comment = data.get('comments', [{}])[0].get('userComment', {})
        developer_comment = data.get('comments', [{}])[0].get('developerComment', {})
        
        return {
        'reviewId': data.get('reviewId', None),
        'authorName': data.get('authorName', None),
        'userComment_text': user_comment.get('text', None),
        'userComment_lastModified_seconds': date_from_ts_s(user_comment.get('lastModified', {}).get('seconds', None)),
        'userComment_lastModified_nanos': user_comment.get('lastModified', {}).get('nanos', None),
        'userComment_starRating': user_comment.get('starRating', None),
        'userComment_reviewerLanguage': user_comment.get('reviewerLanguage', None),
        'userComment_device': user_comment.get('device', None),
        'userComment_androidOsVersion': user_comment.get('androidOsVersion', None),
        'userComment_appVersionCode': user_comment.get('JSON', None),
        'userComment_appVersionName': user_comment.get('appVersionName', None),
        'userComment_thumbsUpCount': user_comment.get('thumbsUpCount', None),
        'userComment_thumbsDownCount': user_comment.get('thumbsDownCount', None),
        'userComment_deviceMetadata': json.dumps(user_comment.get('deviceMetadata', None)),
        'developerComment_text': developer_comment.get('text', None),
        'developerComment_lastModified_seconds': date_from_ts_s(developer_comment.get('lastModified', {}).get('seconds', None)),
        'developerComment_lastModified_nanos': developer_comment.get('lastModified', {}).get('nanos', None),
        }