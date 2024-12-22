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

class TaxReportParserAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "Tax reports"
        self.table_name = "tax_reports"
        self.report_type = constants.ReportTypes.TAX_REPORT.value

        self.records_api = easyEComReportsAPI()

        self.table_id = f'{self.project_id}.{self.dataset_id}.{self.table_name}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.engine = create_engine(connection_string, credentials_info=credentials_info)

    def transform_data(self, data, report_data):
        """Transform the data into the required schema."""
        transformed_data = []
        for df in data:
            df = df.astype(str)
            df.columns = [self.clean_column_name(col) for col in df.columns]
            df = df.assign(**report_data)
            transformed_data.append(df)
            
        return transformed_data
    
    def get_report_data(self, report):
        """Get the report data."""
        del report['status']
        del report['csv_url']
        return report
    
    def sync_data(self):
        """Sync data from the API to BigQuery."""

        completed_reports = self.records_api.get_completed_reports(report_type=self.report_type)
        if not completed_reports:
            print(f"No comepleted reports found")
            return
        extracted_at = datetime.now()
        print(f"len(completed_reports): {len(completed_reports)}")
        # download the csv and convert every row to a record and insert into the big query table
        for report in completed_reports:
            print(f"Downloading the csv for report id: {report['report_id']}")
            data = self.download_csv(report['csv_url'], self.report_type)
            if not data:
                print(f"No data found in the csv for report id: {report['report_id']}")
                continue

            print(f"Transforming the csv data for report id: {report['report_id']}")
            report_data = self.get_report_data(report)
            transformed_data = self.transform_data(data, report_data)
            # Truncate the table by deleting all rows
            # self.truncate_table()

            # Insert the transformed data into the table in chunks
            for data in transformed_data:
                self.load_data_to_bigquery(data, extracted_at, passing_df=True)

            # Update the data in the table
            report_ids = [report['report_id']]
            self.records_api.mark_report_status(report_ids, constants.ReportStatus.PROCESSED.value)
