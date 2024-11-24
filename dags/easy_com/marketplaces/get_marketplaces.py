import requests
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from airflow.models import Variable
from easy_com.marketplaces.marketplace_schema import MarketPlaces
from easy_com.marketplace_listings.get_marketplace_listings import easyEComMarketPlaceListingsAPI
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json

from datetime import datetime

class easyEComMarketPlacesAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/marketplaces/list"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "marketplaces"
        self.table = MarketPlaces

        self.marketplaces_listing_api = easyEComMarketPlaceListingsAPI()

        self.table_id = f'{self.project_id}.{self.dataset_id}.{self.table.__tablename__}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
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
                "marketplace_id": record["id"],
                "name": record["name"]
            }

            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self):
        """Sync data from the API to BigQuery."""
        table_data = self.get_data()
        if not table_data:
            print(f"No {self.name} data found for Easy eCom")
            return

        print(f'Transforming {self.name} data for Easy eCom')
        transformed_data = self.transform_data(data=table_data)

        # Truncate the table by deleting all rows
        self.truncate_table()

        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data)

    def get_data(self):
        """Fetch data from the API."""
        # NOTE: This method does not support nextUrl pagination so this will run at max 1 time for now but keeping it this way for future use
        print(f"Getting {self.name} data for Easy eCom")
        table_data = []
        data = self.send_get_request(self.url)
        data = data.get("data", [])


        # populate the marketplaces listing table
        self.marketplaces_listing_api.truncate_table() # we will delete the states table at the very first step and not in sync because we are calling sync multiple times
        
        # TODO: we are limiting it to only 10 marketplaces for now because to avoid making api requests for 1k market places.
        # We have to find a better way to do this or limit the market places or do parallel processing
        for marketplace in data[:10]:
            self.marketplaces_listing_api.sync_data(marketplace_id=marketplace["id"])


        table_data = data

        return table_data
