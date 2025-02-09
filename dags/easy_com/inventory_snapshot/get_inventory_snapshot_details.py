import requests
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from airflow.models import Variable
from easy_com.inventory_snapshot.inventory_snapshot_schema import InventorySnapshotDetails
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json
import uuid

from datetime import datetime, timedelta

class easyEComInventorySnapshotDetailsAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/inventory/getInventorySnapshotApi"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "Inventory snapshot"
        self.table = InventorySnapshotDetails

        self.table_id = f'{self.project_id}.{self.dataset_id}.{self.table.__tablename__}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = self.get_google_credentials_info()
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.engine = create_engine(connection_string, credentials_info=credentials_info)

        

        self.create_table()

    

    def transform_data(self, data):
        """Transform the data into the required schema."""
        transformed_data = []
        for record in data:
            transformed_record = {
                "id": str(uuid.uuid4()),
                "c_id": record["c_id"],
                "companyname": record["companyname"],
                "job_type_id": record["job_type_id"],
                "entry_date": datetime.strptime(record["entry_date"], "%Y-%m-%d %H:%M:%S") if record.get("entry_date") else None,
                "file_url": record["file_url"],
            }

            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self, start_datetime=None, end_datetime=None):
        """Sync data from the API to BigQuery."""
        if not start_datetime or not end_datetime:
            start_datetime = (datetime.now() - timedelta(days=1))
            end_datetime = start_datetime
        
        start_datetime = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
        end_datetime = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

        table_data = self.get_data(start_datetime, end_datetime)
        if not table_data:
            print(f"No {self.name} data found for Easy eCom")
            return

        print(f'Transforming {self.name} data for Easy eCom')
        transformed_data = self.transform_data(data=table_data)

        extracted_at = datetime.now()
        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data, extracted_at)

    def get_data(self, start_datetime, end_datetime):
        """Fetch data from the API."""
        # NOTE: This method does not support nextUrl pagination so this will run at max 1 time for now but keeping it this way for future use
        print(f"Getting {self.name} data for Easy eCom")
        table_data = []
        next_url = self.url
        max_count = 0

        while next_url:
            if max_count >= 10:
                print("Reached maximum limit of 10 API requests")
                break
            try:
                max_count += 1
                data = self.send_get_request(next_url, params={"start_date": start_datetime, "end_date": end_datetime})
                table_data.extend(data.get("data", []))
                next_url = data.get("nextUrl")
                next_url = self.base_url + next_url if next_url else None
            except Exception as e:
                print(f"Error in getting {self.name} data for Easy eCom: {e}")
                if table_data:
                    print(f'Processing the {self.name} fetched so far')
                    return table_data
                else:
                    break

        return table_data
