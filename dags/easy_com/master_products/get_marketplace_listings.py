import requests
from airflow.models import Variable
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.marketplace_listings.marketplace_listings_schema import MarketPlaceListings
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json

from datetime import datetime

class easyEComMarketPlaceListingsAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "//Listings/getMarketPlaceListing"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "marketplace listings"
        self.table = MarketPlaceListings

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
                "name": record["name"],
                "master_sku": record["MasterSKU"],
                "sku": record["sku"],
                "mrp": record["mrp"],
                "site_uid": record["site_uid"],
                "uid": record["UID"],
                "listing_ref_number": record["listing_ref_number"],
                "identifier": record["identifier"],
                "title": record["title"],
            }

            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self, marketplace_id):
        """Sync data from the API to BigQuery."""
        table_data = self.get_data(marketplace_id)
        if not table_data:
            print(f"No {self.name} data found for Easy eCom whose marketplace id is {marketplace_id}")
            return

        extracted_at = datetime.now()
        print(f'Transforming {self.name} data for Easy eCom')
        transformed_data = self.transform_data(data=table_data)

        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data, extracted_at)

    def get_data(self, marketplace_id):
        """Fetch data from the API."""
        # NOTE: This method does not support nextUrl pagination so this will run at max 1 time for now but keeping it this way for future use
        print(f"Getting {self.name} data for Easy eCom")
        table_data = []

        page_size = 300
        page = 1

        max_recurssion = 10
        page = 1
        while True:
            if page > max_recurssion:
                print('Max Recurssion reached please look into the details once more')
                break
            data = self.send_get_request(self.url, params={"pageNumber": page, "pageSize": page_size, "marketPlaceID": marketplace_id})
            data = data.get("data", [])
            if not data:
                break
            table_data.extend(data)
            page += 1

        return table_data
