from datetime import datetime
import requests
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.customers.customers_schema import Customer
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow.models import Variable
import pandas as pd

import os
import base64
import json


class easyEComCustomersAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/Wholesale/v2/UserManagement"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.table_id = f'{self.project_id}.{self.dataset_id}.{Customer.__tablename__}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.engine = create_engine(connection_string, credentials_info=credentials_info)

        self.create_customers_table()

    def create_customers_table(self):
        """Create table in BigQuery if it does not exist."""
        if not self.table_exists(Customer.__tablename__):
            print("Creating Customers table in BigQuery")
            Customer.metadata.create_all(self.engine)
            print("Customers table created in BigQuery")
        else:
            print("Customers table already exists in BigQuery")

    def table_exists(self, table_name):
        """Check if table exists in BigQuery."""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()

    def transform_data(self, data):
        """Transform the data into the required schema."""
        transformed_data = []
        for record in data:
            transformed_record = {
                "company_invoice_group_id": record["company_invoice_group_id"],
                "c_id": record["c_id"],
                "company_name": record["companyname"],
                "pricing_group": record["pricingGroup"],
                "customer_support_email": record["customer_support_email"],
                "customer_support_contact": record["customer_support_contact"],
                "brand_description": record["branddescription"],
                "currency_code": record["currency_code"],
                "gst_num": record["gstNum"],
                "type_of_customer": record["type_of_customer"],
                "billing_street": record["billingStreet"],
                "billing_city": record["billingCity"],
                "billing_zipcode": record["billingZipcode"],
                "billing_state": record["billingState"],
                "billing_country": record["billingCountry"],
                "dispatch_street": record["dispatchStreet"],
                "dispatch_city": record["dispatchCity"],
                "dispatch_zipcode": record["dispatchZipcode"],
                "dispatch_state": record["dispatchState"],
                "dispatch_country": record["dispatchCountry"],
            }
            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self):
        """Sync data from the API to BigQuery."""
        customers = self.get_data()
        if not customers:
            print("No customers data found for Easy eCom")
            return

        print('Transforming customers data for Easy eCom')
        transformed_data = self.transform_data(data=customers)

        extracted_at = datetime.now()
        # Truncate the table by deleting all rows
        self.truncate_table()

        # Insert the transformed data into the table
        self.load_data_to_bigquery(transformed_data, extracted_at)

    def truncate_table(self):
        """Truncate the BigQuery table by deleting all rows."""
        table_ref = self.client.dataset(self.dataset_id).table(Customer.__tablename__)
        truncate_query = f"DELETE FROM `{table_ref}` WHERE true"
        self.client.query(truncate_query).result()  # Executes the DELETE query

    def load_data_to_bigquery(self, data, extracted_at):
        """Load the data into BigQuery."""
        print("Loading Customers data to BigQuery")
        df = pd.DataFrame(data)
        df["ee_extracted_at"] = extracted_at
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        job = self.client.load_table_from_dataframe(df, self.table_id, job_config=job_config)
        job.result()

    def get_data(self):
        """Fetch Customers data from the API."""
        # NOTE: This method does not support nextUrl pagination so this will run at max 1 time for now but keeping it this way for future use
        print("Getting Customers data for Easy eCom")
        all_customers = []
        

        for customer_type in ["b2b", "stn"]:
            max_count = 0
            next_url = self.url
            while next_url:
                if max_count >= 10:
                    print("Reached maximum limit of 10 API requests")
                    break
                try:
                    max_count += 1
                    data = self.send_get_request(next_url, params={"type": customer_type})
                    customer_data = data.get("data", [])
                    customer_data = [{**record, "type_of_customer": customer_type} for record in customer_data]
                    all_customers.extend(customer_data)
                    next_url = data.get("nextUrl")
                    next_url = self.base_url + next_url if next_url else None
                except Exception as e:
                    print(f"Error in getting customers data for Easy eCom: {e}")
                    if all_customers:
                        print('Processing the customers fetched so far')
                        return all_customers
                    else:
                        break

        return all_customers
