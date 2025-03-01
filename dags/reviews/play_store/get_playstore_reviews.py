from datetime import datetime
import requests
from airflow.models import Variable
from reviews.play_store.reviews_schema import GooglePlayRatings
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

from google_play_scraper import Sort, reviews, reviews_all

import os
import base64
import time
import json

class GooglePlayRatingsAPI:
    dataset_id = "reviews"
    project_id = "shopify-pubsub-project"
    package_name = "com.discoverpilgrim"
    
    def __init__(self):
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "pilgrim_bi_google_play"
        self.table_id = f'{self.project_id}.{self.dataset_id}.{GooglePlayRatings.__tablename__}'

        # BigQuery connection string
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
        if not self.table_exists(GooglePlayRatings.__tablename__):
            print("Creating Google Play Ratings table in BigQuery")
            GooglePlayRatings.metadata.create_all(self.engine)
            print("Google Play Ratings table created in BigQuery")
        else:
            print("Google Play Ratings table already exists in BigQuery")

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
        table_ref = self.client.dataset(self.dataset_id).table(GooglePlayRatings.__tablename__)
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
    def __init__(self):
        self.API_KEY = "ya29.a0AXeO80R3UjcUx2yaFEkTwFW7XCGqUi5rkUi5OO60Y9cJ0kiOuEWS8jiIQOy8KJkYtvuJul_LrDhSemJl1jQo7qYN1CEhualH-P-2Rm9l9ntpFZFLZcHDOnynbVS7RKb8d0zYHucpusb4GBsJPJTn737VD7nCo9nATCkOBmxVaCgYKARUSARMSFQHGX2MiRoiHe-kt4WOJh3_pcHOw5w0175"
        super().__init__()
    
    def sync_data(self):
        """Sync data from the API to BigQuery."""
        import pdb; pdb.set_trace()
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
    
    def get_data(self, next_token=None):
        """Fetch Google Playstore reviews data using the API key."""
        all_reviews = []
        while True:
            headers = {
                "Authorization": f"Bearer {self.API_KEY}"
            }
            url = f"https://playstore.googleapis.com/v1/reviews?packageName={self.package_name}"
            if next_token:
                url += f"&pageToken={next_token}"
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Error fetching data: {response.status_code}")
                break
            data = response.json()
            reviews = data.get("reviews", [])
            all_reviews.extend(reviews)
            next_token = data.get("nextPageToken", None)
            if not next_token:
                break
        return all_reviews

    def transform_data(self, data):
        """Transform the data into the required schema."""
        transformed_data = []
        for record in data:
            user_comment = record.get('comments', [{}])[0].get('userComment', {})
            developer_comment = record.get('comments', [{}])[1].get('developerComment', {})
            transformed_record = {
                'reviewId': record.get('reviewId', None),
                'authorName': record.get('authorName', None),
                'userComment_text': user_comment.get('text', None),
                'userComment_lastModified_seconds': user_comment.get('lastModified', {}).get('seconds', None),
                'userComment_lastModified_nanos': user_comment.get('lastModified', {}).get('nanos', None),
                'userComment_starRating': user_comment.get('starRating', None),
                'userComment_reviewerLanguage': user_comment.get('reviewerLanguage', None),
                'userComment_device': user_comment.get('device', None),
                'userComment_androidOsVersion': user_comment.get('androidOsVersion', None),
                'userComment_appVersionCode': user_comment.get('appVersionCode', None),
                'userComment_appVersionName': user_comment.get('appVersionName', None),
                'userComment_thumbsUpCount': user_comment.get('thumbsUpCount', None),
                'userComment_thumbsDownCount': user_comment.get('thumbsDownCount', None),
                'userComment_deviceMetadata': user_comment.get('deviceMetadata', None),
                'developerComment_text': developer_comment.get('text', None),
                'developerComment_lastModified_seconds': developer_comment.get('lastModified', {}).get('seconds', None),
                'developerComment_lastModified_nanos': developer_comment.get('lastModified', {}).get('nanos', None),
            }
            transformed_data.append(transformed_record)
        return transformed_data