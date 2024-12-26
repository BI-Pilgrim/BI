import requests
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from airflow.models import Variable
from easy_com.states.get_states import easyEComStatesAPI
from easy_com.countries.countries_schema import Countries
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json
from datetime import datetime


class easyEComCountriesAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/getCountries"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.table_id = f'{self.project_id}.{self.dataset_id}.{Countries.__tablename__}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = self.get_google_credentials_info()
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.engine = create_engine(connection_string, credentials_info=credentials_info)

        self.create_countries_table()
        self.states_api = easyEComStatesAPI()

    def create_countries_table(self):
        """Create table in BigQuery if it does not exist."""
        if not self.table_exists(Countries.__tablename__):
            print("Creating Countries table in BigQuery")
            Countries.metadata.create_all(self.engine)
            print("Countries table created in BigQuery")
        else:
            print("Countries table already exists in BigQuery")

    def table_exists(self, table_name):
        """Check if table exists in BigQuery."""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()

    def transform_data(self, data):
        """Transform the data into the required schema."""
        transformed_data = []
        for record in data:
            transformed_record = {
                "country_id": record["id"],
                "country": record["country"],
                "default_currency_code": record["default_currency_code"],
                "code_2": record["code_2"],
                "code_3": record["code_3"],
            }
            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self):
        """Sync data from the API to BigQuery."""
        locations = self.get_data()
        if not locations:
            print("No countries data found for Easy eCom")
            return

        print('Transforming countries data for Easy eCom')
        transformed_data = self.transform_data(data=locations)
        extracted_at = datetime.now()

        # Truncate the table by deleting all rows
        self.truncate_table()

        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data, extracted_at)

    def truncate_table(self):
        """Truncate the BigQuery table by deleting all rows."""
        table_ref = self.client.dataset(self.dataset_id).table(Countries.__tablename__)
        truncate_query = f"DELETE FROM `{table_ref}` WHERE true"
        self.client.query(truncate_query).result()  # Executes the DELETE query

    def load_data_to_bigquery(self, data, extracted_at):
        """Load the data into BigQuery."""
        print("Loading Countries data to BigQuery")
        df = pd.DataFrame(data)
        df["ee_extracted_at"] = extracted_at
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        job = self.client.load_table_from_dataframe(df, self.table_id, job_config=job_config)
        job.result()

    def get_data(self):
        """Fetch Locations data from the API."""
        print("Getting Countries data for Easy eCom")
        all_countries = []
        data = self.send_get_request(self.url)
        countries_data = data.get("countries", [])
        
        # populate the states table
        self.states_api.truncate_table() # we will delete the states table at the very first step and not in sync because we are calling sync multiple times
        for country in countries_data:
            # print(country)
            self.states_api.sync_data(country_id=country["id"])

        all_countries = countries_data

        return all_countries
