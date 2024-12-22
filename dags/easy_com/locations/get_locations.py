from datetime import datetime
import requests
from airflow.models import Variable
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.locations.locations_schema import Location
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json


class easyEComLocationsAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/getAllLocation"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.table_id = f'{self.project_id}.{self.dataset_id}.{Location.__tablename__}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.engine = create_engine(connection_string, credentials_info=credentials_info)

        self.create_locations_table()

    def create_locations_table(self):
        """Create table in BigQuery if it does not exist."""
        if not self.table_exists(Location.__tablename__):
            print("Creating Locations table in BigQuery")
            Location.metadata.create_all(self.engine)
            print("Locations table created in BigQuery")
        else:
            print("Locations table already exists in BigQuery")

    def table_exists(self, table_name):
        """Check if table exists in BigQuery."""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()

    def transform_data(self, data):
        """Transform the data into the required schema."""
        transformed_data = []
        for record in data:
            transformed_record = {
                "location_name": record["location_name"],
                "location_key": record["location_key"],
                "is_store": record["is_store"],
                "city": record["city"],
                "state": record["state"],
                "country": record["country"],
                "zip": record["zip"],
                "copy_master_from_primary": record["copy_master_from_primary"],
                "address": record["address"],
                "api_token": record["api_token"],
                "user_id": record["userId"],
                "phone_number": record["phone number"],
                "stockHandle": record["stockHandle"]
            }
            if record.get("address type"):
                address_type = record["address type"]
                billing_address = address_type.get("billing_address", {}) or {}
                pickup_address = address_type.get("pickup_address", {}) or {}
                transformed_record.update({
                    "billing_street": billing_address.get("street"),
                    "billing_state": billing_address.get("state"),
                    "billing_zipcode": billing_address.get("zipcode"),
                    "billing_country": billing_address.get("country"),
                    "pickup_street": pickup_address.get("street"),
                    "pickup_state": pickup_address.get("state"),
                    "pickup_zipcode": pickup_address.get("zipcode"),
                    "pickup_country": pickup_address.get("country")
                })
            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self):
        """Sync data from the API to BigQuery."""
        locations = self.get_data()
        if not locations:
            print("No locations data found for Easy eCom")
            return
        extracted_at = datetime.now()
        print('Transforming locations data for Easy eCom')
        transformed_data = self.transform_data(data=locations)

        # Truncate the table by deleting all rows
        self.truncate_table()

        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data, extracted_at)

    def truncate_table(self):
        """Truncate the BigQuery table by deleting all rows."""
        table_ref = self.client.dataset(self.dataset_id).table(Location.__tablename__)
        truncate_query = f"DELETE FROM `{table_ref}` WHERE true"
        self.client.query(truncate_query).result()  # Executes the DELETE query

    def load_data_to_bigquery(self, data, extracted_at):
        """Load the data into BigQuery."""
        print("Loading Locations data to BigQuery")
        df = pd.DataFrame(data)
        df["ee_extracted_at"] = extracted_at
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        job = self.client.load_table_from_dataframe(df, self.table_id, job_config=job_config)
        job.result()

    def get_data(self):
        """Fetch Locations data from the API."""
        # NOTE: This method does not support nextUrl pagination so this will run at max 1 time for now but keeping it this way for future use
        print("Getting Locations data for Easy eCom")
        all_locations = []
        next_url = self.url
        max_count = 0

        while next_url:
            if max_count >= 10:
                print("Reached maximum limit of 10 API requests")
                break
            try:
                max_count += 1
                data = self.send_get_request(next_url)
                all_locations.extend(data.get("data", []))
                next_url = data.get("nextUrl")
                next_url = self.base_url + next_url if next_url else None
            except Exception as e:
                print(f"Error in getting locations data for Easy eCom: {e}")
                if all_locations:
                    print('Processing the locations fetched so far')
                    return all_locations
                else:
                    break

        return all_locations
    
    def get_all_location_keys(self):
        """Get all location keys from the Locations table."""
        query = f"SELECT location_key FROM {Location.__tablename__}"
        with self.engine.connect() as connection:
            result = connection.execute(query)
            location_keys = [row[0] for row in result]
        return location_keys
