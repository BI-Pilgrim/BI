import requests
from airflow.models import Variable
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.kits.kits_schema import Kits
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

from easy_com.reports.get_reports import easyEComReportsAPI

import os
import base64
import json

from datetime import datetime
from easy_com.reports import constants

@DeprecationWarning
class TaxReportParserAPI(EasyComApiConnector):
    def __init__(self, report_type:str=constants.ReportTypes.TAX_REPORT_RETURN.value):
        super().__init__()
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "Tax reports"
        self.table_name = "tax_reports"
        self.report_type = report_type

        self.records_api = easyEComReportsAPI()

        self.table_id = f'{self.project_id}.{self.dataset_id}.{self.table_name}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = self.get_google_credentials_info()
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.engine = create_engine(connection_string, credentials_info=credentials_info)

    def transform_data(self, data):
        """Transform the data into the required schema."""
        transformed_data = []
        for df in data:
            df = df.astype(str)
            df.columns = [self.clean_column_name(col) for col in df.columns]
            # transformed_data.append(df.to_dict(orient='records'))
            transformed_data.append(df)
            
        # import pdb; pdb.set_trace()
        return transformed_data
    
    def sync_data(self):
        """Sync data from the API to BigQuery."""

        completed_reports = self.records_api.get_completed_reports(report_type=self.report_type)
        if not completed_reports[:1]:
            print(f"No comepleted reports found")
            return
        
        print(f"len(completed_reports): {len(completed_reports)}")
        # download the csv and convert every row to a record and insert into the big query table
        for report in completed_reports:
            print(f"Downloading the csv for report id: {report['report_id']}")
            data = self.download_csv(report['csv_url'], self.report_type)
            if not data:
                print(f"No data found in the csv for report id: {report['report_id']}")
                continue

            print(f"Transforming the csv data for report id: {report['report_id']}")
            transformed_data = self.transform_data(data)
            # Truncate the table by deleting all rows
            # self.truncate_table()

            # Insert the transformed data into the table
            for data in transformed_data:
                self.load_data_to_bigquery(data, passing_df=True)