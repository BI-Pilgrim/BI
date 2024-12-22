from datetime import datetime
import requests
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.vendors.vendors_schema import Vendor
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json


class easyEComVendorsAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/wms/V2/getVendors"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.table_id = f'{self.project_id}.{self.dataset_id}.{Vendor.__tablename__}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.engine = create_engine(connection_string, credentials_info=credentials_info)

        self.create_vendors_table()

    def create_vendors_table(self):
        """Create table in BigQuery if it does not exist."""
        if not self.table_exists(Vendor.__tablename__):
            print("Creating Vendors table in BigQuery")
            Vendor.metadata.create_all(self.engine)
            print("Vendors table created in BigQuery")
        else:
            print("Vendors table already exists in BigQuery")

    def table_exists(self, table_name):
        """Check if table exists in BigQuery."""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()

    def transform_data(self, data):
        """Transform the data into the required schema."""
        transformed_data = []
        for record in data:
            transformed_record = {
                "vendor_name": record["vendor_name"],
                "vendor_c_id": record["vendor_c_id"],
                "api_token": record["api_token"],
                "dl_number": record["dl_number"],
                "dl_expiry": record["dl_expiry"],
                "fssai_number": record["fssai_number"],
                "fssai_expiry": record["fssai_expiry"],
            }
            if record.get("address"):
                address = record["address"]
                dispatch_address = address.get("dispatch", {}) or {}
                billing_address = address.get("billing", {}) or {}
                transformed_record.update({
                    "dispatch_address": dispatch_address.get("address"),
                    "dispatch_city": dispatch_address.get("city"),
                    "dispatch_state_id": dispatch_address.get("state_id"),
                    "dispatch_state_name": dispatch_address.get("state_name"),
                    "dispatch_zip": dispatch_address.get("zip"),
                    "dispatch_country": dispatch_address.get("country"),
                    "billing_address": billing_address.get("address"),
                    "billing_city": billing_address.get("city"),
                    "billing_state_id": billing_address.get("state_id"),
                    "billing_state_name": billing_address.get("state_name"),
                    "billing_zip": billing_address.get("zip"),
                    "billing_country": billing_address.get("country"),
                })
            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self):
        """Sync data from the API to BigQuery."""
        vendors = self.get_data()
        if not vendors:
            print("No vendors data found for Easy eCom")
            return

        print('Transforming vendors data for Easy eCom')
        transformed_data = self.transform_data(data=vendors)
        extrated_at = datetime.now()

        # Truncate the table by deleting all rows
        self.truncate_table()

        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data, extrated_at)

    def truncate_table(self):
        """Truncate the BigQuery table by deleting all rows."""
        table_ref = self.client.dataset(self.dataset_id).table(Vendor.__tablename__)
        truncate_query = f"DELETE FROM `{table_ref}` WHERE true"
        self.client.query(truncate_query).result()  # Executes the DELETE query

    def load_data_to_bigquery(self, data, extracted_at):
        """Load the data into BigQuery."""
        print("Loading vendors data to BigQuery")
        df = pd.DataFrame(data)
        df["ee_extracted_at"] = extracted_at
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        job = self.client.load_table_from_dataframe(df, self.table_id, job_config=job_config)
        job.result()

    def get_data(self):
        """Fetch vendors data from the API."""
        print("Getting vendors data for Easy eCom")
        all_vendors = []
        next_url = self.url
        max_count = 0

        while next_url and max_count < 10:
            try:
                max_count += 1
                data = self.send_get_request(next_url)
                all_vendors.extend(data.get("data", []))
                next_url = data.get("nextUrl")
                next_url = self.base_url + next_url if next_url else None
            except Exception as e:
                print(f"Error in getting vendors data for Easy eCom: {e}")
                if all_vendors:
                    print('Processing the vendors fetched so far')
                    return all_vendors
                else:
                    break

        return all_vendors
