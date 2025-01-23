import requests
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.orders.orders_schema import Orders
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import os
import base64
import json

from datetime import datetime, timedelta

class easyEComOrdersAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/orders/V2/getAllOrders"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "orders"
        self.table = Orders

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
            transformed_record ={
                "invoice_id": self.convert(record.get("invoice_id"), int),
                "order_id": self.convert(record.get("order_id"), int),
                "queue_message": self.convert(record.get("queue_message"), str),
                "queue_status": self.convert(record.get("queue_status"), int),
                "order_priority": self.convert(record.get("order_priority"), int),
                "block_split": self.convert(record.get("blockSplit"), int),
                "reference_code": self.convert(record.get("reference_code"), str),
                "company_name": self.convert(record.get("company_name"), str),
                "location_key": self.convert(record.get("location_key"), str),
                "warehouse_id": self.convert(record.get("warehouseId"), int),
                "seller_gst": self.convert(record.get("seller_gst"), str),
                "import_warehouse_id": self.convert(record.get("import_warehouse_id"), int),
                "import_warehouse_name": self.convert(record.get("import_warehouse_name"), str),
                "pickup_address": self.convert(record.get("pickup_address"), str),
                "pickup_city": self.convert(record.get("pickup_city"), str),
                "pickup_state": self.convert(record.get("pickup_state"), str),
                "pickup_pin_code": self.convert(record.get("pickup_pin_code"), str),
                "pickup_country": self.convert(record.get("pickup_country"), str),
                "invoice_currency_code": self.convert(record.get("invoice_currency_code"), str),
                "order_type": self.convert(record.get("order_type"), str),
                "order_type_key": self.convert(record.get("order_type_key"), str),
                "replacement_order": self.convert(record.get("replacement_order"), int),
                "marketplace": self.convert(record.get("marketplace"), str),
                "marketplace_id": self.convert(record.get("marketplace_id"), int),
                "salesman_user_id": self.convert(record.get("salesmanUserId"), int),
                "order_date": self.convert(record["order_date"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "tat": self.convert(record["tat"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "available_after": self.convert(record["available_after"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "invoice_date": self.convert(record[ "invoice_date"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "import_date": self.convert(record["import_date"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "last_update_date": self.convert(record["last_update_date"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "manifest_date": self.convert(record["manifest_date"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "manifest_no": self.convert(record.get("manifest_no"), str),
                "invoice_number": self.convert(record.get("invoice_number"), str),
                "marketplace_invoice_num": self.convert(record.get("marketplace_invoice_num"), str),
                "shipping_last_update_date": self.convert(record["shipping_last_update_date"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "batch_id": self.convert(record.get("batch_id"), int),
                "batch_created_at": self.convert(record["batch_created_at"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "message": self.convert(record.get("message"), str),
                "courier_aggregator_name": self.convert(record.get("courier_aggregator_name"), str),
                "courier": self.convert(record.get("courier"), str),
                "carrier_id": self.convert(record.get("carrier_id"), int),
                "awb_number": self.convert(record.get("awb_number"), str),
                "package_weight": self.convert(record.get("Package Weight"), float),
                "package_height": self.convert(record.get("Package Height"), float),
                "package_length": self.convert(record.get("Package Length"), float),
                "package_width": self.convert(record.get("Package Width"), float),
                "order_status": self.convert(record.get("order_status"), str),
                "order_status_id": self.convert(record.get("order_status_id"), int),
                "suborder_count": self.convert(None if record.get("suborder_count") == "NA" else record.get("suborder_count"), int),
                "shipping_status": self.convert(record.get("shipping_status"), str),
                "shipping_status_id": self.convert(record.get("shipping_status_id"), int),
                "shipping_history": self.convert(json.dumps([record.get("shipping_history")]) if record.get("shipping_history") else None, str),
                "delivery_date": self.convert(record["delivery_date"], datetime, strptime="%Y-%m-%d %H:%M:%S"),
                "payment_mode": self.convert(record.get("payment_mode"), str),
                "payment_mode_id": self.convert(record.get("payment_mode_id"), int),
                "payment_gateway_transaction_number": self.convert(record.get("payment_gateway_transaction_number"), str),
                "payment_gateway_name": self.convert(record.get("payment_gateway_name"), str),
                "buyer_gst": self.convert(record.get("buyer_gst"), str),
                "customer_name": self.convert(record.get("customer_name"), str),
                "contact_num": self.convert(record.get("contact_num"), str),
                "address_line_1": self.convert(record.get("address_line_1"), str),
                "address_line_2": self.convert(record.get("address_line_2"), str),
                "city": self.convert(record.get("city"), str),
                "pin_code": self.convert(record.get("pin_code"), str),
                "state": self.convert(record.get("state"), str),
                "state_code": self.convert(record.get("state_code"), str),
                "country": self.convert(record.get("country"), str),
                "email": self.convert(record.get("email"), str),
                "latitude": self.convert(record.get("latitude"), str),
                "longitude": self.convert(record.get("longitude"), str),
                "billing_name": self.convert(record.get("billing_name"), str),
                "billing_address_1": self.convert(record.get("billing_address_1"), str),
                "billing_address_2": self.convert(record.get("billing_address_2"), str),
                "billing_city": self.convert(record.get("billing_city"), str),
                "billing_state": self.convert(record.get("billing_state"), str),
                "billing_state_code": self.convert(record.get("billing_state_code"), str),
                "billing_pin_code": self.convert(record.get("billing_pin_code"), str),
                "billing_country": self.convert(record.get("billing_country"), str),
                "billing_mobile": self.convert(record.get("billing_mobile"), str),
                "order_quantity": self.convert(record.get("order_quantity"), int),
                "meta": self.convert(json.dumps([record.get("meta")]) if record.get("meta") else None, str),
                "documents": self.convert(json.dumps([record.get("documents")]) if record.get("documents") else None, str),
                "total_amount": self.convert(record.get("total_amount") if record.get("total_amount") else None, float),
                "total_tax": self.convert(record.get("total_tax") if record.get("total_tax") else None, float),
                "total_shipping_charge": self.convert(record.get("total_shipping_charge") if record.get("total_shipping_charge") else None, float),
                "total_discount": self.convert(record.get("total_discount") if record.get("total_discount") else None, float),
                "collectable_amount": self.convert(record.get("collectable_amount") if record.get("collectable_amount") else None, float),
                "tcs_rate": self.convert(record.get("tcs_rate") if record.get("tcs_rate") else None, float),
                "tcs_amount": self.convert(record.get("tcs_amount") if record.get("tcs_amount") else None, float),
                "customer_code": self.convert(str(record.get("customer_code")) if record.get("customer_code") else None, str),
                "suborders": self.convert(json.dumps([record.get("suborders")]) if record.get("suborders") else None, str),
                "fulfillable_status": self.convert(json.dumps([record.get("fulfillable_status")]) if record.get("fulfillable_status") else None, str),
            }
            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self, start_date=None, end_date=None):
        """Sync data from the API to BigQuery."""
        if not (start_date and end_date):
            end_date = (datetime.now() - timedelta(days=1))
            start_date = end_date - timedelta(days=90) # Add a 90 day look back period

        start_date = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
        table_data = self.get_data(start_date, end_date)
        if not table_data:
            print(f"No {self.name} data found for Easy eCom")
            return "No data found"

        print(f'Transforming {self.name} data for Easy eCom')
        transformed_data = self.transform_data(data=table_data)

        extracted_at = datetime.now()
        # Insert the transformed data into the table
        # import pdb; pdb.set_trace()
        self.load_data_to_bigquery(transformed_data, extracted_at)
        return transformed_data

    def get_data(self, start_date, end_date):
        """Fetch data from the API."""
        print(f"Getting {self.name} data for Easy eCom")
        print(f"Start Date: {start_date}, End Date: {end_date}")
        
        table_data = []
        next_url = self.url
        max_count = 0

        while next_url:
            if max_count >= 10000:
                print(f"Reached maximum limit of {max_count} API requests")
                return None
            response_data = None
            try:
                max_count += 1
                # print(f"Making request {max_count} for {self.name}")
                response_data = self.send_get_request(next_url, params={"start_date": start_date, "end_date": end_date, "limit": 250})
                data = response_data.get("data", []).get("orders", [])
                
                table_data.extend(data)
                next_url = response_data.get("data", {}).get("nextUrl")
                next_url = self.base_url + next_url if next_url else None
            except Exception as e:
                print(response_data)
                print(f"Error in getting {self.name} data for Easy eCom: {e}")
                return []            

        return table_data