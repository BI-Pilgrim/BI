import requests
from airflow.models import Variable
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.kits.kits_schema import Kits
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json

from datetime import datetime

class easyEComKitsAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/Products/getKits"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "kits"
        self.table = Kits

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
                "product_id": record["productId"],
                "sku": record["sku"],
                "accounting_sku": record["accountingSku"],
                "accounting_unit": record["accountingUnit"],
                "mrp": record["mrp"],
                "add_date":  datetime.strptime(record["add_date"], "%Y-%m-%d %H:%M:%S") if record.get("add_date") else None,
                "last_update_date": datetime.strptime(record["lastUpdateDate"], "%Y-%m-%d %H:%M:%S") if record.get("lastUpdateDate") else None,
                "cost": record["cost"],
                "hsn_code": record["HSNCode"],
                "colour": record["colour"],
                "height": record["height"],
                "width": record["width"],
                "length": record["length"],
                "weight": record["weight"],
                "size": record["size"],
                "material_type": record["material_type"],
                "model_number": record["modelNumber"],
                "model_name": record["modelName"],
                "category": record["category"],
                "brand": record["brand"],
                "c_id": record["c_id"],
                "sub_products": json.dumps([
                    {
                        "sku": sub_product["sku"],
                        "product_id": sub_product["productId"],
                        "qty": sub_product["qty"],
                        "description": sub_product["description"],
                        "cost": sub_product["cost"],
                        "available_inventory": sub_product["availableInventory"],
                    } for sub_product in record["subProducts"]
                ])
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
        next_url = self.url
        max_count = 0

        while next_url:
            if max_count >= 10:
                print("Reached maximum limit of 10 API requests")
                break
            try:
                max_count += 1
                data = self.send_get_request(next_url)
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
