import requests
from easy_com.easy_com_api_connector import EasyComApiConnector
from easy_com.locations.get_locations import easyEComLocationsAPI
from easy_com.reports.reports_schema import Reports
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
from easy_com.reports import constants

import os
import base64
import json
import uuid

from datetime import datetime, timedelta

class easyEComReportsAPI(EasyComApiConnector):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + "/reports/queue"
        self.project_id = "shopify-pubsub-project"
        self.dataset_id = "easycom"
        self.name = "reports"
        self.table = Reports


        self.locations_api = easyEComLocationsAPI()
        self.table_id = f'{self.project_id}.{self.dataset_id}.{self.table.__tablename__}'
        self.temp_table_id = f'{self.project_id}.{self.dataset_id}.temp_{self.table.__tablename__}_{uuid.uuid4().hex}'

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
                "report_id": record["report_id"],
                "report_type": record["report_type"],
                "start_date": record["start_date"],
                "end_date": record["end_date"],
                "created_on": record["created_on"],
                "status": record["status"],
                "csv_url": record["csv_url"],
                "inventory_type": record["inventory_type"]
            }
            transformed_data.append(transformed_record)
        return transformed_data

    def sync_data(self, start_date = None, end_date = None):
        """Sync data from the API to BigQuery."""
        if not (start_date and end_date):
            end_datetime = (datetime.now() - timedelta(days=1))
            start_datetime = end_datetime
        else:
            start_datetime = start_date
            end_datetime = end_date

        end_datetime = end_datetime.replace(hour=23, minute=59, second=59, microsecond=0)
        extracted_at = datetime.now()
        for report_type in constants.ReportTypes.get_all_types():
            if self.report_already_exists(report_type, start_datetime, end_datetime):
                print(f"Report {report_type} already exists in BigQuery")
                continue
            report_data = self.get_data(report_type, start_datetime, end_datetime)
            if not report_data:
                print(f"No {self.name} data found for report type {report_type}")
                continue

            # transform data
            report_data = self.transform_data(report_data)

            # insert data into BigQuery
            print(f'Inserting {self.name} data for report type {report_type} into BigQuery')
            self.load_data_to_bigquery(report_data, extracted_at)

    def get_data(self, report_type, start_datetime, end_datetime):
        """Fetch data from the API."""
        print(f"Getting {self.name} data for report type {report_type}")
        all_data = []
        body = self.get_request_body(report_type, start_datetime, end_datetime)

        if report_type == constants.ReportTypes.INVENTORY_VIEW_BY_BIN_REPORT.value:
            for inventory_type in constants.InventoryAgingTypes.get_all_types():
                print(f'Generating it for inventory type {inventory_type}')
                body["params"] = {"inventoryType": inventory_type}
                report_data = self.send_post_request(self.url, body)
                data = report_data.get("data", None)
                print(report_data)
                if not data:
                    print(f"Unable to fetch {self.name} data for report type {report_type}. Reason: {report_data.get('message')}")
                    continue
                data.update({"inventory_type": inventory_type})
                all_data.append(data)

        else:
            report_data = self.send_post_request(self.url, body)
            data = report_data.get("data", None)
            print(report_data)
            if not data:
                print(f"Unable to fetch {self.name} data for report type {report_type}. Reason: {report_data.get('message')}")
                return None
            all_data.append(data)
    
        all_data = filter(None, all_data)
        
        
        return [self.format_response_data(data, report_type, start_datetime, end_datetime) for data in all_data]
    
    def format_response_data(self, data, report_type, start_datetime, end_datetime):
        return {
            "report_id": data.get("reportId"),
            "report_type": report_type,
            "start_date": start_datetime.replace(hour=0, minute=0, second=0, microsecond=0),
            "end_date": end_datetime.replace(hour=23, minute=59, second=59, microsecond=0),
            "created_on": datetime.now(),
            "status": constants.ReportStatus.IN_PROGRESS.value,
            "csv_url": None,
            "inventory_type": data.get("inventory_type", None)
        }
    
    def get_request_body(self, report_type, start_datetime, end_datetime):
        """Get the request body for the API request."""
        if report_type == constants.ReportTypes.MINI_SALES_REPORT.value:
            return {
                "reportType": report_type,
                "params" : {
                    "invoiceType" : "ALL",
                    "warehouseIds" : ",".join(self.locations_api.get_all_location_keys()),
                    "dateType" : "ORDER_DATE",
                    "startDate" : start_datetime.strftime("%Y-%m-%d"),
                    "endDate" : end_datetime.strftime("%Y-%m-%d"),
                }
            }
        elif report_type == constants.ReportTypes.TAX_REPORT_RETURN.value:
            return {
                "reportType": report_type,
                "params" : {
                    "taxReportType" : "RETURN",
                    "warehouseIds" : ",".join(self.locations_api.get_all_location_keys()),
                    "startDate" : start_datetime.strftime("%Y-%m-%d"),
                    "endDate" : end_datetime.strftime("%Y-%m-%d"),
                }
            }
        elif report_type == constants.ReportTypes.TAX_REPORT_SALES.value:
            return {
                "reportType": report_type,
                "params" : {
                    "taxReportType" : "SALES",
                    "warehouseIds" : ",".join(self.locations_api.get_all_location_keys()),
                    "startDate" : start_datetime.strftime("%Y-%m-%d"),
                    "endDate" : end_datetime.strftime("%Y-%m-%d"),
                }
            }
        elif report_type == constants.ReportTypes.STATUS_WISE_STOCK_REPORT.value:
            return {
                "reportType": report_type,
            }
        elif report_type == constants.ReportTypes.INVENTORY_AGING_REPORT.value:
            return {
                "reportType": report_type,
            }
        elif report_type == constants.ReportTypes.INVENTORY_VIEW_BY_BIN_REPORT.value:
            return {
                "reportType": report_type,
            }
        elif report_type == constants.ReportTypes.RETURN_REPORT.value:
            return {
                "reportType": report_type,
                "params" : {
                    "startDate" : start_datetime.strftime("%Y-%m-%d"),
                    "endDate" : end_datetime.strftime("%Y-%m-%d"),
                }
            }
        elif report_type == constants.ReportTypes.PENDING_RETURN_REPORT.value:
            return {
                "reportType": report_type,
                "params" : {
                    "startDate" : start_datetime.strftime("%Y-%m-%d"),
                    "endDate" : end_datetime.strftime("%Y-%m-%d"),
                }
            }
        elif report_type == constants.ReportTypes.GRN_DETAILS_REPORT.value:
            return {
                "reportType": report_type,
                "params" : {
                    "startDate" : start_datetime.strftime("%Y-%m-%d"),
                    "endDate" : end_datetime.strftime("%Y-%m-%d"),
                }
            }
        else:
            raise ValueError(f"Invalid report type {report_type}")
        
    def delete_record_id(self, record_id):
        """Delete a record from BigQuery."""
        table = self.table.__tablename__
        delete_query = f"DELETE FROM {self.table_id} WHERE report_id = '{record_id}'"
        print(delete_query)
        self.client.query(delete_query).result()

    def get_completed_reports(self, report_type):
        """Fetch all in progress reports."""
        query = f"SELECT * FROM {self.table_id} WHERE status = 'COMPLETED' AND report_type = '{report_type}'"
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return [dict(row) for row in result]
        
    def mark_report_status(self, report_ids, status):
        """Mark the report status in BigQuery."""
        print(f"Marking the status of the reports as {status}")
        merge_query = f'''
            MERGE {self.table_id} T
            USING {self.temp_table_id} S
            ON T.report_id = S.report_id
            WHEN MATCHED THEN
                UPDATE SET T.status = '{status}', ee_extracted_at=CURRENT_DATETIME
        '''
        update_data = [{"report_id": report_id, "status": status} for report_id in report_ids]
        self.update_data(update_data, merge_query)

    def report_already_exists(self, report_type, start_datetime, end_datetime):
        """Check if the report already exists in BigQuery."""
        print(report_type, start_datetime, end_datetime)
        query = f"SELECT * FROM {self.table_id} WHERE report_type = '{report_type}' AND start_date = '{start_datetime}' AND end_date = '{end_datetime}'"
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return result.first() is not None
        