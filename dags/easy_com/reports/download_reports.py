import requests
from airflow.models import Variable
# from utils.google_cloud import get_base_name_from_uri
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
            report_id_types = self.get_in_progress_reports()

        completed_reports = self.get_data(report_id_types)
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
                UPDATE SET T.status = S.status, T.csv_url = S.csv_url, T.ee_extracted_at=CURRENT_DATETIME, 
                T.gcs_uri = S.gcs_uri
        '''
        self.update_data(completed_reports, merge_query)

    def get_data(self, report_id_types):
        """Fetch data from the API."""
        print(f"Downloading report url for in progress reports")
        
        completed_reports = []
        for report_id, report_type in report_id_types:
            response = self.send_get_request(self.url, params={"reportId": report_id})
            data = response.get("data", {})
            if not data:
                print(f"Unable to download {self.name} data found for report id {report_id}")
                continue
            
            if data.get("reportStatus") == constants.ReportStatus.COMPLETED.value and data.get("downloadUrl").startswith("https"):
                
                # destination_blob_name = f"easyecom/reports/{report_id}/{get_base_name_from_uri(data.get('downloadUrl'))}"
                gcs_uri = self.download_and_upload_report(data.get("downloadUrl"), report_type, report_id)
                
                
                completed_reports.append({
                    "report_id": report_id,
                    "status": constants.ReportStatus.COMPLETED.value,
                    "csv_url": data.get("downloadUrl"),
                    "gcs_uri": gcs_uri
                })

        return completed_reports

    def download_and_upload_report(self, report_url, report_type, report_id):
        """Download a report from a URL and upload it to Google Cloud Storage."""
        file_path = self._download_csv(report_url, report_type)
        return self.upload_to_gcs(file_path, file_name=f"easyecom/reports/{report_type}/{report_id}.csv")
        
    def get_in_progress_reports(self):
        """Fetch all in progress reports."""
        query = f"SELECT report_id, report_type  FROM {self.table_id} WHERE status = 'IN_PROGRESS'"
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return [(row[0], row[1]) for row in result]
        