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
            transformed_record ={
                "invoice_id": record.get("invoice_id"),
                "order_id": record.get("order_id"),
                "queue_message": record.get("queue_message"),
                "queue_status": record.get("queue_status"),
                "order_priority": record.get("order_priority"),
                "block_split": record.get("blockSplit"),
                "reference_code": record.get("reference_code"),
                "company_name": record.get("company_name"),
                "location_key": record.get("location_key"),
                "warehouse_id": record.get("warehouseId"),
                "seller_gst": record.get("seller_gst"),
                "import_warehouse_id": record.get("import_warehouse_id"),
                "import_warehouse_name": record.get("import_warehouse_name"),
                "pickup_address": record.get("pickup_address"),
                "pickup_city": record.get("pickup_city"),
                "pickup_state": record.get("pickup_state"),
                "pickup_pin_code": record.get("pickup_pin_code"),
                "pickup_country": record.get("pickup_country"),
                "invoice_currency_code": record.get("invoice_currency_code"),
                "order_type": record.get("order_type"),
                "order_type_key": record.get("order_type_key"),
                "replacement_order": record.get("replacement_order"),
                "marketplace": record.get("marketplace"),
                "marketplace_id": record.get("marketplace_id"),
                "salesman_user_id": record.get("salesmanUserId"),
                "order_date": datetime.strptime(record["order_date"], "%Y-%m-%d %H:%M:%S") if record.get("order_date") else None,
                "tat": datetime.strptime(record["tat"], "%Y-%m-%d %H:%M:%S") if record.get("tat") else None,
                "available_after": datetime.strptime(record["available_after"], "%Y-%m-%d %H:%M:%S") if record.get("available_after") else None,
                "invoice_date": datetime.strptime(record[ "invoice_date"], "%Y-%m-%d %H:%M:%S") if record.get("invoice_date") else None,
                "import_date": datetime.strptime(record["import_date"], "%Y-%m-%d %H:%M:%S") if record.get("import_date") else None,
                "last_update_date": datetime.strptime(record["last_update_date"], "%Y-%m-%d %H:%M:%S") if record.get("last_update_date") else None,
                "manifest_date": datetime.strptime(record["manifest_date"], "%Y-%m-%d %H:%M:%S") if record.get("manifest_date") else None,
                "manifest_no": record.get("manifest_no"),
                "invoice_number": record.get("invoice_number"),
                "marketplace_invoice_num": record.get("marketplace_invoice_num"),
                "shipping_last_update_date": datetime.strptime(record["shipping_last_update_date"], "%Y-%m-%d %H:%M:%S") if record.get("shipping_last_update_date") else None,
                "batch_id": record.get("batch_id"),
                "batch_created_at": datetime.strptime(record["batch_created_at"], "%Y-%m-%d %H:%M:%S") if record.get("batch_created_at") else None,
                "message": record.get("message"),
                "courier_aggregator_name": record.get("courier_aggregator_name"),
                "courier": record.get("courier"),
                "carrier_id": record.get("carrier_id"),
                "awb_number": record.get("awb_number"),
                "package_weight": record.get("Package Weight"),
                "package_height": record.get("Package Height"),
                "package_length": record.get("Package Length"),
                "package_width": record.get("Package Width"),
                "order_status": record.get("order_status"),
                "order_status_id": record.get("order_status_id"),
                "suborder_count": None if record.get("suborder_count") == "NA" else record.get("suborder_count"),
                "shipping_status": record.get("shipping_status"),
                "shipping_status_id": record.get("shipping_status_id"),
                "shipping_history": json.dumps([record.get("shipping_history")]) if record.get("shipping_history") else None,
                "delivery_date": datetime.strptime(record["delivery_date"], "%Y-%m-%d %H:%M:%S") if record.get("delivery_date") else None,
                "payment_mode": record.get("payment_mode"),
                "payment_mode_id": record.get("payment_mode_id"),
                "payment_gateway_transaction_number": record.get("payment_gateway_transaction_number"),
                "payment_gateway_name": record.get("payment_gateway_name"),
                "buyer_gst": record.get("buyer_gst"),
                "customer_name": record.get("customer_name"),
                "contact_num": record.get("contact_num"),
                "address_line_1": record.get("address_line_1"),
                "address_line_2": record.get("address_line_2"),
                "city": record.get("city"),
                "pin_code": record.get("pin_code"),
                "state": record.get("state"),
                "state_code": record.get("state_code"),
                "country": record.get("country"),
                "email": record.get("email"),
                "latitude": record.get("latitude"),
                "longitude": record.get("longitude"),
                "billing_name": record.get("billing_name"),
                "billing_address_1": record.get("billing_address_1"),
                "billing_address_2": record.get("billing_address_2"),
                "billing_city": record.get("billing_city"),
                "billing_state": record.get("billing_state"),
                "billing_state_code": record.get("billing_state_code"),
                "billing_pin_code": record.get("billing_pin_code"),
                "billing_country": record.get("billing_country"),
                "billing_mobile": record.get("billing_mobile"),
                "order_quantity": record.get("order_quantity"),
                "meta": json.dumps([record.get("meta")]) if record.get("meta") else None,
                "documents": json.dumps([record.get("documents")]) if record.get("documents") else None,
                "total_amount": record.get("total_amount") if record.get("total_amount") else None,
                "total_tax": record.get("total_tax") if record.get("total_tax") else None,
                "total_shipping_charge": record.get("total_shipping_charge") if record.get("total_shipping_charge") else None,
                "total_discount": record.get("total_discount") if record.get("total_discount") else None,
                "collectable_amount": record.get("collectable_amount") if record.get("collectable_amount") else None,
                "tcs_rate": record.get("tcs_rate") if record.get("tcs_rate") else None,
                "tcs_amount": record.get("tcs_amount") if record.get("tcs_amount") else None,
                "customer_code": str(record.get("customer_code")) if record.get("customer_code") else None,
                "suborders": json.dumps([record.get("suborders")]) if record.get("suborders") else None,
                "fulfillable_status": json.dumps([record.get("fulfillable_status")]) if record.get("fulfillable_status") else None,
            }
            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self, start_date=None, end_date=None):
        """Sync data from the API to BigQuery."""
        if not (start_date and end_date):
            end_date = (datetime.now() - timedelta(days=1))
            start_date = end_date - timedelta(days=1)

        start_date = start_date.strftime("%Y-%m-%d 00:00:00")
        end_date = end_date.strftime("%Y-%m-%d 00:00:00")
        table_data = self.get_data(start_date, end_date)
        if not table_data:
            print(f"No {self.name} data found for Easy eCom")
            return "No data found"

        print(f'Transforming {self.name} data for Easy eCom')
        transformed_data = self.transform_data(data=table_data)

        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data)

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