import requests
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.master_products.master_products_schema import MasterProducts
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json

from datetime import datetime

class easyEComMasterProductAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/Products/GetProductMaster"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "master products"
        self.table = MasterProducts

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
                "cp_id": record["cp_id"],
                "product_id": record["product_id"],
                "sku": record["sku"],
                "product_name": record["product_name"],
                "description": record.get("description"),
                "active": record.get("active"),
                "created_at": datetime.strptime(record["created_at"], "%Y-%m-%d %H:%M:%S") if record.get("created_at") else None,
                "updated_at": datetime.strptime(record["updated_at"], "%Y-%m-%d %H:%M:%S") if record.get("updated_at") else None,
                "inventory": record.get("inventory"),
                "product_type": record.get("product_type"),
                "brand": record.get("brand"),
                "colour": record.get("colour"),
                "category_id": record.get("category_id"),
                "brand_id": record.get("brand_id"),
                "accounting_sku": record.get("accounting_sku"),
                "accounting_unit": record.get("accounting_unit"),
                "category_name": record.get("category_name"),
                "expiry_type": record.get("expiry_type"),
                "company_name": record.get("company_name"),
                "c_id": record.get("c_id"),
                "height": record.get("height"),
                "length": record.get("length"),
                "width": record.get("width"),
                "weight": record.get("weight"),
                "cost": record.get("cost"),
                "mrp": record.get("mrp"),
                "size": record.get("size"),
                "cp_sub_products_count": record.get("cp_sub_products_count"),
                "model_no": record.get("model_no"),
                "hsn_code": record.get("hsn_code"),
                "tax_rate": record.get("tax_rate"),
                "product_shelf_life": record.get("product shelf life"),
                "product_image_url": record.get("product_image_url"),
                "cp_inventory": record.get("cp_inventory"),
                "custom_fields": json.dumps(record.get("custom_fields")) if record.get("custom_fields") else None,
                "sub_products": json.dumps([record.get("sub_products")]) if record.get("sub_products") else None
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

        self.truncate_table()

        extracted_at = datetime.now()
        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data, extracted_at)

    def get_data(self):
        """Fetch data from the API."""
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
                data = self.send_get_request(next_url, params={"custom_fields": 1})
                
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
