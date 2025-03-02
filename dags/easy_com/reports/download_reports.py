import requests
from airflow.models import Variable
from utils.google_cloud import get_base_name_from_uri
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.locations.get_locations import easyEComLocationsAPI
from easy_com.reports.reports_schema import Reports
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery, storage
import pandas as pd
from easy_com.reports import constants

import os
import base64
import json

from datetime import datetime, timedelta

class easyEComDownloadReportsAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/reports/download"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "Download reports"
        self.table = Reports


        self.table_id = f'{self.project_id}.{self.dataset_id}.{self.table.__tablename__}'
        self.temp_table_id = f'{self.project_id}.{self.dataset_id}.temp_{self.table.__tablename__}'

        # BigQuery connection string
        connection_string = f"bigquery://{self.project_id}/{self.dataset_id}"

        credentials_info = self.get_google_credentials_info()
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)

        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        self.storage_client = storage.Client(credentials=credentials, project=self.project_id)
        self.engine = create_engine(connection_string, credentials_info=credentials_info)

        self.create_table()
    
    def sync_data(self, report_ids = []):
        """Sync/Download data from the API to BigQuery."""
        
        if not report_ids:
            report_ids = self.get_in_progress_reports()

        completed_reports = self.get_data(report_ids)
        if not completed_reports:
            print(f"No comepleted reports found")
            return

        
        # update the status and csv url in the table
        print(f"Updating the status and csv url in the table")
        merge_query = f'''
            MERGE {self.table_id} T
            USING {self.temp_table_id} S
            ON T.report_id = S.report_id
            WHEN MATCHED THEN
                UPDATE SET T.status = S.status, T.csv_url = S.csv_url, T.ee_extracted_at=CURRENT_DATETIME
        '''
        self.update_data(completed_reports, merge_query)

    def get_data(self, report_ids):
        """Fetch data from the API."""
        print(f"Downloading report url for in progress reports")
        
        completed_reports = []
        for report_id in report_ids:
            response = self.send_get_request(self.url, params={"reportId": report_id})
            data = response.get("data", {})
            if not data:
                print(f"Unable to download {self.name} data found for report id {report_id}")
                continue
            
            if data.get("reportStatus") == constants.ReportStatus.COMPLETED.value and data.get("downloadUrl").startswith("https"):
                
                destination_blob_name = f"easyecom/reports/{report_id}/{get_base_name_from_uri(data.get('downloadUrl'))}"
                self.download_and_upload_report(data.get("downloadUrl"), destination_blob_name)
                
                completed_reports.append({
                    "report_id": report_id,
                    "status": constants.ReportStatus.COMPLETED.value,
                    "csv_url": data.get("downloadUrl")
                })

        return completed_reports

    def download_and_upload_report(self, report_url, destination_blob_name):
        """Download a report from a URL and upload it to Google Cloud Storage."""
        response = requests.get(report_url)
        if response.status_code == 200:
            bucket_name = "your-gcs-bucket-name"
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(response.content)
            print(f"Report uploaded to {bucket_name}/{destination_blob_name}")
        else:
            print(f"Failed to download report from {report_url}")
        
    def get_in_progress_reports(self):
        """Fetch all in progress reports."""
        query = f"SELECT report_id FROM {self.table_id} WHERE status = 'IN_PROGRESS'"
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return [row[0] for row in result]
        