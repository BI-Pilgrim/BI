import requests
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.states.states_schema import States
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json

from datetime import datetime

class easyEComStatesAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/getStates"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.table_id = f'{self.project_id}.{self.dataset_id}.{States.__tablename__}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = self.get_google_credentials_info()
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.engine = create_engine(connection_string, credentials_info=credentials_info)

        self.create_states_table()

    def create_states_table(self):
        """Create table in BigQuery if it does not exist."""
        if not self.table_exists(States.__tablename__):
            print("Creating States table in BigQuery")
            States.metadata.create_all(self.engine)
            print("States table created in BigQuery")
        else:
            print("States table already exists in BigQuery")

    def table_exists(self, table_name):
        """Check if table exists in BigQuery."""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()

    def transform_data(self, data):
        """Transform the data into the required schema."""
        transformed_data = []
        for record in data:
            transformed_record = {
                "state_id": record["id"],
                "name": record["name"],
                "is_union_territory": record["is_union_territory"],
                "zip_start_range": record["zip_start_range"],
                "zip_end_range": record["zip_end_range"],
                "postal_code": record["postal_code"] or '',
                "country_id": record["country_id"],
                "zone": record["Zone"] or ''
            }

            transformed_data.append(transformed_record)

        return transformed_data

    def sync_data(self, country_id, extracted_at):
        """Sync data from the API to BigQuery."""
        states = self.get_data(country_id)
        if not states:
            print("No states data found for Easy eCom")
            return

        # extracted_at = datetime.now()
        print('Transforming states data for Easy eCom')
        transformed_data = self.transform_data(data=states)

        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data, extracted_at)

    def truncate_table(self):
        """Truncate the BigQuery table by deleting all rows."""
        table_ref = self.client.dataset(self.dataset_id).table(States.__tablename__)
        truncate_query = f"DELETE FROM `{table_ref}` WHERE true"
        self.client.query(truncate_query).result()  # Executes the DELETE query

    def load_data_to_bigquery(self, data):
        """Load the data into BigQuery."""
        print("Loading States data to BigQuery")
        df = pd.DataFrame(data)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        job = self.client.load_table_from_dataframe(df, self.table_id, job_config=job_config)
        job.result()

    def get_data(self, country_id):
        """Fetch States data from the API."""
        # NOTE: This method does not support nextUrl pagination so this will run at max 1 time for now but keeping it this way for future use
        print("Getting States data for Easy eCom")
        all_states = []
        
        data = self.send_get_request(self.url, params={"countryId": country_id})
        all_states = data.get("states", [])

        return all_states
