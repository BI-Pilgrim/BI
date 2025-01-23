from google.cloud import bigquery
import logging
import base64
import json
from google.oauth2 import service_account
import os
# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def get_google_credentials_info():
    """Get Google credentials from environment variables"""
    return os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')

project_id = 'shopify-pubsub-project'
dataset_id = 'easycom'

def create_bigquery_tables():
    """Create BigQuery tables if they don't exist"""
    try:
        logging.info("Starting BigQuery table creation")
        credentials_info = get_google_credentials_info()
        credentials_info = base64.b64decode(credentials_info).decode("utf-8")
        credentials_info = json.loads(credentials_info)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(credentials=credentials, project=project_id)
        dataset_ref = client.dataset(dataset_id)
        
        # Define table schemas
        daily_metrics_schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("total_orders", "INTEGER"),
            bigquery.SchemaField("delayed_dispatch_count", "INTEGER"),
            bigquery.SchemaField("compliance_rate", "FLOAT"),
            bigquery.SchemaField("avg_delay_hours", "FLOAT"),
            bigquery.SchemaField("median_delay_hours", "FLOAT"),
            bigquery.SchemaField("analysis_date", "TIMESTAMP")
        ]
        
        warehouse_metrics_schema = [
            bigquery.SchemaField("location_key", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("warehouse_name", "STRING"),
            bigquery.SchemaField("total_orders", "INTEGER"),
            bigquery.SchemaField("delayed_orders", "INTEGER"),
            bigquery.SchemaField("compliance_rate", "FLOAT"),
            bigquery.SchemaField("avg_delay_hours", "FLOAT"),
            bigquery.SchemaField("median_delay_hours", "FLOAT"),
            bigquery.SchemaField("analysis_date", "TIMESTAMP"),
        ]
        
        # New schemas for status breakdowns
        order_status_schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("count", "INTEGER"),
            bigquery.SchemaField("analysis_date", "TIMESTAMP")
        ]
        
        shipping_status_schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("count", "INTEGER"),
            bigquery.SchemaField("analysis_date", "TIMESTAMP")
        ]
        
        tables = {
            'daily_metrics': daily_metrics_schema,
            'warehouse_metrics': warehouse_metrics_schema,
            'order_status_metrics': order_status_schema,
            'shipping_status_metrics': shipping_status_schema,
        }
        
        for table_name, schema in tables.items():
            table_id = f"{project_id}.{dataset_id}.{table_name}"
            try:
                # Try to get the table to check if it exists
                client.get_table(table_id)
                logging.info(f"Table {table_name} already exists")
            except Exception as e:
                # If table doesn't exist, create it
                table = bigquery.Table(table_id, schema=schema)
                client.create_table(table)
                logging.info(f"Created table {table_name}")
        
        logging.info("Successfully created/verified BigQuery tables")
    except Exception as e:
        logging.error(f"Error creating BigQuery tables: {str(e)}")
        raise

if __name__ == "__main__":
    create_bigquery_tables() 