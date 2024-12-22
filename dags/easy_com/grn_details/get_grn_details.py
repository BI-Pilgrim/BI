import requests
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.grn_details.grn_details_schema import GrnDetails
from easy_com.locations.get_locations import easyEComLocationsAPI
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json

from datetime import datetime

from easy_com.easy_com_api_connector import generate_location_key_token

class easyEComGrnDetailsAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/Grn/V2/getGrnDetails"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "Grn Details"
        self.table = GrnDetails
        self.locations_api = easyEComLocationsAPI()

        self.table_id = f'{self.project_id}.{self.dataset_id}.{self.table.__tablename__}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = os.getenv("GOOGLE_BIGQUERY_CREDENTIALS")
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
                "grn_id": record["grn_id"],
                "grn_invoice_number": record["grn_invoice_number"],
                "total_grn_value": record["total_grn_value"],
                "grn_status_id": record["grn_status_id"],
                "grn_status": record["grn_status"],
                "grn_created_at": datetime.strptime(record["grn_created_at"], "%Y-%m-%d %H:%M:%S") if record.get("grn_created_at") else None,
                "grn_invoice_date": datetime.strptime(record["grn_invoice_date"], "%Y-%m-%d %H:%M:%S") if record.get("grn_invoice_date") else None,
                "po_id": record["po_id"],
                "po_number": record["po_number"],
                "po_ref_num": record["po_ref_num"],
                "po_status_id": record["po_status_id"],
                "po_created_date": datetime.strptime(record["po_created_date"], "%Y-%m-%d %H:%M:%S") if record.get("po_created_date") else None,
                "po_updated_date": datetime.strptime(record["po_updated_date"], "%Y-%m-%d %H:%M:%S") if record.get("po_updated_date") else None,
                "inwarded_warehouse": record["inwarded_warehouse"],
                "inwarded_warehouse_c_id": record["inwarded_warehouse_c_id"],
                "vendor_name": record["vendor_name"],
                "vendor_c_id": record["vendor_c_id"],
                "grn_items": json.dumps(record["grn_items"])
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
        
        print(f"Getting {self.name} data for Easy eCom")
        table_data = []
       
        location_keys = self.locations_api.get_all_location_keys()
        for location_key in location_keys:
            token = generate_location_key_token(location_key)
            
            next_url = self.url
            max_count = 0

            while next_url:
                if max_count >= 10:
                    print("Reached maximum limit of 10 API requests")
                    break
                try:
                    max_count += 1
                    data = self.send_get_request(next_url, auth_token=token)
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
