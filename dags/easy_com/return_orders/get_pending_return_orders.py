import requests
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.return_orders.return_orders_schema import PendingReturnOrders
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
from easy_com.locations.get_locations import easyEComLocationsAPI


class easyEComPendingReturnOrdersAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/getPendingReturns"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "Pending Return Orders"
        self.table = PendingReturnOrders
        self.locations_api = easyEComLocationsAPI()

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
                "invoice_id": record["invoice_id"],
                "order_id": record["order_id"],
                "reference_code": record["reference_code"],
                "company_name": record["company_name"],
                "ware_house_id": record["warehouseId"],
                "seller_gst": record["seller_gst"],
                "forward_shipment_pickup_address": record["forward_shipment_pickup_address"],
                "forward_shipment_pickup_city": record["forward_shipment_pickup_city"],
                "forward_shipment_pickup_state": record["forward_shipment_pickup_state"],
                "forward_shipment_pickup_pin_code": record["forward_shipment_pickup_pin_code"],
                "forward_shipment_pickup_country": record["forward_shipment_pickup_country"],
                "order_type": record["order_type"],
                "order_type_key": record["order_type_key"],
                "replacement_order": record["replacement_order"],
                "marketplace": record["marketplace"],
                "marketplace_id": record["marketplace_id"],
                "salesman_user_id": record["salesmanUserId"],
                "order_date": datetime.strptime(record["order_date"], "%Y-%m-%d %H:%M:%S") if record.get("order_date") else None,
                "invoice_date": datetime.strptime(record["invoice_date"], "%Y-%m-%d %H:%M:%S") if record.get("invoice_date") else None,
                "import_date": datetime.strptime(record["import_date"], "%Y-%m-%d %H:%M:%S") if record.get("import_date") else None,
                "last_update_date": datetime.strptime(record["last_update_date"], "%Y-%m-%d %H:%M:%S") if record.get("last_update_date") else None,
                "manifest_date": datetime.strptime(record["manifest_date"], "%Y-%m-%d %H:%M:%S") if record.get("manifest_date") else None,
                "manifest_no": record["manifest_no"],
                "invoice_number": record["invoice_number"],
                "marketplace_invoice_num": record["marketplace_invoice_num"],
                "batch_id": record["batch_id"],
                "payment_mode": record["payment_mode"],
                "payment_mode_id": record["payment_mode_id"],
                "buyer_gst": record["buyer_gst"],
                "forward_shipment_customer_name": record["forward_shipment_customer_name"],
                "forward_shipment_customer_contact_num": record["forward_shipment_customer_contact_num"],
                "forward_shipment_customer_address_line_1": record["forward_shipment_customer_address_line_1"],
                "forward_shipment_customer_address_line_2": record["forward_shipment_customer_address_line_2"],
                "forward_shipment_customer_city": record["forward_shipment_customer_city"],
                "forward_shipment_customer_pin_code": record["forward_shipment_customer_pin_code"],
                "forward_shipment_customer_state": record["forward_shipment_customer_state"],
                "forward_shipment_customer_country": record["forward_shipment_customer_country"],
                "forward_shipment_customer_email": record["forward_shipment_customer_email"],
                "forward_shipment_billing_name": record["forward_shipment_billing_name"],
                "forward_shipment_billing_address_1": record["forward_shipment_billing_address_1"],
                "forward_shipment_billing_address_2": record["forward_shipment_billing_address_2"],
                "forward_shipment_billing_city": record["forward_shipment_billing_city"],
                "forward_shipment_billing_state": record["forward_shipment_billing_state"],
                "forward_shipment_billing_pin_code": record["forward_shipment_billing_pin_code"],
                "forward_shipment_billing_country": record["forward_shipment_billing_country"],
                "forward_shipment_billing_mobile": record["forward_shipment_billing_mobile"],
                "order_quantity": record["order_quantity"],
                "total_invoice_amount": record["total_invoice_amount"],
                "total_invoice_tax": record["total_invoice_tax"],
                "invoice_collectable_amount": record["invoice_collectable_amount"],
                "items": json.dumps(record["items"]),
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

        extracted_at = datetime.now()
        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data, extracted_at)

    def get_data(self):
        """Fetch data from the API."""
        # NOTE: This method does not support nextUrl pagination so this will run at max 1 time for now but keeping it this way for future use
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
                    pending_returns = data.get("data") or {}
                    table_data.extend(pending_returns.get('pending_returns', []))
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
