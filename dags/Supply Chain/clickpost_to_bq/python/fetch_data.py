import requests
import json
import pandas as pd
import logging
from datetime import datetime, timedelta
import time
from google.cloud import bigquery
from typing import Dict, List
import base64
from google.oauth2 import service_account
import os
import sys
from airflow.models import Variable

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Constants
API_KEY = Variable.get("CLICKPOST_API_KEY")
PROJECT_ID = 'shopify-pubsub-project'
DATASET_ID = 'clickpost_data'

# def get_google_credentials_info():
#     """Get Google credentials from environment variables"""
#     return os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')

def generate_report() -> str:
    """Initiates report generation and returns reference number"""
    invalid_fields_list = [28, 29, 77, 96, 97, 98, 99, 100, 101, 102, 112, 113, 138, 139, 140]
    fields_list = [i for i in range(1, 144) if i not in invalid_fields_list]
    
    url = f"https://www.clickpost.in/api/v1/reports/enterprise_dashboard_report/?key={API_KEY}"
    
    # Get date range from environment variables or use default (previous day)
    start_time = os.environ.get('CLICKPOST_START_TIME')
    end_time = os.environ.get('CLICKPOST_END_TIME')
    
    if not start_time or not end_time:
        previous_day = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_time = f"{previous_day} 00:00:00"
        end_time = f"{previous_day} 23:59:59"
    
    payload = json.dumps({
        "filters": {
            "updated_at_start_time": start_time,
            "updated_at_end_time": end_time
        },
        "report_name": "TRACK_ORDER_DASHBOARD_REPORT",
        "fields": fields_list
    })
    
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(url, headers=headers, data=payload)
        response_data = response.json()
        
        if response_data['meta']['status'] == 200:
            reference_number = response_data['result'][0]['report_reference_number']
            logger.info(f"Report generation initiated with reference number: {reference_number}")
            return reference_number
        else:
            raise Exception(f"Failed to generate report: {response_data['meta']['message']}")
    except Exception as e:
        logger.error(f"Error in generate_report: {str(e)}")
        raise

def check_report_status(reference_number: str) -> Dict:
    """Checks the status of report generation"""
    url = f"https://www.clickpost.in/api/v1/reports/enterprise_dashboard_report/?key={API_KEY}&report_reference_number={reference_number}"
    
    try:
        response = requests.get(url)
        return response.json()
    except Exception as e:
        logger.error(f"Error checking report status: {str(e)}")
        raise

def process_dataframe(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Process the raw dataframe into separate tables"""
    logger.info(f"Starting dataframe processing")
    logger.info(f"Input dataframe shape: {df.shape}")
    
    # Add ingestion_date to all dataframes
    ingestion_date = datetime.now().date()
    logger.info(f"Using ingestion date: {ingestion_date}")
    
    # Convert timestamp columns
    timestamp_columns = [
        'Created at', 'Pickup Date', 'Latest Timestamp', 'Delivery Date', 
        'Updated at', 'Out For Delivery 1st Attempt', 'Out For Delivery 2nd Attempt',
        'Out For Delivery 3rd Attempt', 'Out For Delivery 4th Attempt',
        'Out For Delivery 5th Attempt', 'Time Stamp Of Last Failed Delivery',
        'RTO Mark Date', 'Out For Pickup 1st Attempt', 'Out For Pickup 2nd Attempt',
        'Out For Pickup 3rd Attempt', 'Destination Hub In Scan Time',
        'Time Stamp Of First Failed Pickup', 'Time Stamp Of Last Failed Pickup',
        'Time Stamp Of First Failed Delivery', 'Origin Hub In Timestamp',
        'Origin Hub Out Timestamp', 'RTO Intransit Timestamp', 'RTO Delivery Date',
        'Original Appointment Start Time', 'Original Appointment End Time',
        'Current Appointment Start Time', 'Current Appointment End Time'
    ]
    
    # Convert date columns
    date_columns = [
        'Order Date', 'Invoice Date', 'Expected Delivery Date By Courier Partner',
        'Expected Date Of Delivery Min', 'Expected Date Of Delivery Max'
    ]
    
    # Define column mappings for each table
    table_columns = {
        'orders': [
            'Order ID', 'Reference Number', 'Order Date', 'Invoice Number', 
            'Invoice Date', 'Invoice Value', 'COD Value', 'Payment Mode',
            'Channel Name', 'Account Code', 'Items', 'Items Quantity', 'Product SKU Code'
        ],
        'shipping': [
            'Order ID', 'Courier Partner', 'AWB', 'RTO AWB', 'Clickpost Unified Status',
            'Committed SLA', 'Zone', 'Pricing Zone', 'Shipping Cost',
            'Mode of Shipment', 'Carrier via Aggregator', 'From Warehouse', 
            'To Warehouse', 'Box Count'
        ],
        'addresses': [
            'Order ID',
            # Pickup details
            'Pickup Name', 'Pickup Phone', 'Pickup Address', 'Pickup Pincode',
            'Pickup City', 'Pickup State', 'Pickup Country', 'Pickup District',
            'Pickup Organisation', 'PickUp Address Type',
            # Drop details
            'Drop Name', 'Drop Phone', 'Drop Email', 'Drop Address', 'Drop Pincode',
            'Drop City', 'Drop District', 'Drop Organisation', 'Drop Address Type',
            # Return details
            'Return Name', 'Return Phone', 'Return Address', 'Return Pincode',
            'Return City', 'Return State', 'Return Country'
        ],
        'tracking': [
            'Order ID', 'Created at', 'Pickup Date', 'Current Location', 
            'Latest Timestamp', 'Latest Remark', 'Delivery Date', 'Updated at',
            'Expected delivery date by Courier Partner', 'Expected Date of Delivery (Min)',
            'Expected Date of Delivery (Max)', 'Out For Delivery 1st Attempt',
            'Out For Delivery 2nd Attempt', 'Out For Delivery 3rd Attempt',
            'Out For Delivery 4th Attempt', 'Out For Delivery 5th Attempt',
            'Out For Delivery Attempts', 'Reason For Last Failed Delivery',
            'TimeStamp Of Last Failed Delivery', 'Remark Of Last Failed Delivery',
            'Is shipment stuck in lifecycle', 'RTO Mark Date',
            'Out For Pickup 1st Attempt', 'Out For Pickup 2nd Attempt',
            'Out For Pickup 3rd Attempt', 'Out For Pickup Attempts',
            'Destination Hub In Scan Time', 'Origin Hub In Timestamp',
            'Origin Hub Out Timestamp', 'RTO Intransit Timestamp', 'RTO Delivery Date',
            'Original Appointment Start Time', 'Original Appointment End Time',
            'Current Appointment Start Time', 'Current Appointment End Time'
        ],
        'package': [
            'Order ID', 'Shipment Length', 'Shipment Breadth', 'Shipment Height',
            'Shipment Weight', 'Item Length', 'Item Breadth', 'Item Height',
            'Actual Weight', 'Chargeable Weight', 'Volumetric Weight', 'SKU',
            'SKU Wise Item Quantity', 'Return Product URL'
        ],
        'additional_info': [
            'Order ID', 'TIN', 'QC Type', 'Holiday Count in EDD', 'Sub Category',
            'UDF_1 (as sent in the OC API)', 'UDF_2 (as sent in the OC API)', 'UDF_3 (as sent in the OC API)', 'UDF_4 (as sent in the OC API)', 'OTP Verified Delivery',
            'OTP Verified Cancellation', 'Consignor Code (Gati/Saksham/Bluedart)', 'RVP Reason',
            'RVP Reason SKU Wise', 'EDD sent over OC API'
        ]
    }
    
    processed_dfs = {}
    for table_name, columns in table_columns.items():
        logger.info(f"\nProcessing table: {table_name}")
        logger.info(f"Columns to extract: {len(columns)}")
        
        # Select columns and create a copy
        table_df = df[columns].copy()
        
        # Convert timestamp columns
        for col in timestamp_columns:
            if col in table_df.columns:
                table_df[col] = pd.to_datetime(table_df[col], errors='coerce')
        
        # Convert date columns
        for col in date_columns:
            if col in table_df.columns:
                table_df[col] = pd.to_datetime(table_df[col], errors='coerce').dt.date
        
        # Convert numeric columns based on table
        if table_name == 'orders':
            # Convert specific columns to float
            float_columns = ['COD Value', 'Invoice Value']
            for col in float_columns:
                if col in table_df.columns:
                    table_df[col] = clean_numeric_column(table_df[col])
                    table_df[col] = table_df[col].fillna(0.0)
            
            # Convert Items Quantity to integer
            if 'Items Quantity' in table_df.columns:
                table_df['Items Quantity'] = pd.to_numeric(table_df['Items Quantity'], errors='coerce').fillna(0).astype('int64')
        
        elif table_name == 'shipping':
            numeric_columns = ['Committed SLA', 'Shipping Cost', 'Box Count']
            for col in numeric_columns:
                if col in table_df.columns:
                    table_df[col] = clean_numeric_column(table_df[col])
                    table_df[col] = table_df[col].fillna(0.0)
        
        elif table_name == 'package':
            int_columns = ['Shipment Length', 'Shipment Breadth', 'Shipment Height', 
                         'Shipment Weight', 'SKU Wise Item Quantity']
            float_columns = ['Item Length', 'Item Breadth', 'Item Height', 
                           'Actual Weight', 'Chargeable Weight', 'Volumetric Weight']
            
            for col in int_columns:
                if col in table_df.columns:
                    table_df[col] = pd.to_numeric(table_df[col], errors='coerce').astype('Int64')
            for col in float_columns:
                if col in table_df.columns:
                    table_df[col] = pd.to_numeric(table_df[col], errors='coerce')
        
        # Convert string columns
        string_columns = [col for col in table_df.columns if col not in timestamp_columns + date_columns]
        for col in string_columns:
            table_df[col] = table_df[col].astype(str).replace('nan', None)
        
        # Add ingestion date
        table_df['ingestion_date'] = ingestion_date
        
        processed_dfs[table_name] = table_df
        logger.info(f"Completed processing {table_name}: {table_df.shape[0]} rows")
    
    logger.info("Dataframe processing completed")
    return processed_dfs

def create_tables_if_not_exist(client: bigquery.Client) -> None:
    """Create BigQuery dataset and tables if they don't exist"""
    # First, create dataset if it doesn't exist
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_id)
        logger.info(f"Dataset {dataset_id} already exists")
    except Exception:
        # Create dataset
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"  # Specify the location
        dataset = client.create_dataset(dataset, timeout=30)
        logger.info(f"Created dataset {dataset_id}")

    # Then create tables if they don't exist
    table_schemas = {
        'orders': [
            bigquery.SchemaField("Order ID", "STRING"),
            bigquery.SchemaField("Reference Number", "STRING"),
            bigquery.SchemaField("Order Date", "DATE"),
            bigquery.SchemaField("Invoice Number", "STRING"),
            bigquery.SchemaField("Invoice Date", "DATE"),
            bigquery.SchemaField("Invoice Value", "FLOAT64"),
            bigquery.SchemaField("COD Value", "FLOAT64"),
            bigquery.SchemaField("Payment Mode", "STRING"),
            bigquery.SchemaField("Channel Name", "STRING"),
            bigquery.SchemaField("Account Code", "STRING"),
            bigquery.SchemaField("Items", "STRING"),
            bigquery.SchemaField("Items Quantity", "INTEGER"),
            bigquery.SchemaField("Product SKU Code", "STRING"),
            bigquery.SchemaField("Ingestion Date", "DATE")
        ],
        'shipping': [
            bigquery.SchemaField("Order ID", "STRING"),
            bigquery.SchemaField("Courier Partner", "STRING"),
            bigquery.SchemaField("AWB", "STRING"),
            bigquery.SchemaField("RTO AWB", "STRING"),
            bigquery.SchemaField("Clickpost Unified Status", "STRING"),
            bigquery.SchemaField("Committed SLA", "FLOAT64"),
            bigquery.SchemaField("Zone", "STRING"),
            bigquery.SchemaField("Pricing Zone", "STRING"),
            bigquery.SchemaField("Shipping Cost", "FLOAT64"),
            bigquery.SchemaField("Mode of Shipment", "STRING"),
            bigquery.SchemaField("Carrier via Aggregator", "STRING"),
            bigquery.SchemaField("From Warehouse", "STRING"),
            bigquery.SchemaField("To Warehouse", "STRING"),
            bigquery.SchemaField("Box Count", "INTEGER"),
            bigquery.SchemaField("Ingestion Date", "DATE")
        ],
        'addresses': [
            bigquery.SchemaField("Order ID", "STRING"),
            # Pickup fields
            bigquery.SchemaField("Pickup Name", "STRING"),
            bigquery.SchemaField("Pickup Phone", "STRING"),
            bigquery.SchemaField("Pickup Address", "STRING"),
            bigquery.SchemaField("Pickup Pincode", "STRING"),
            bigquery.SchemaField("Pickup City", "STRING"),
            bigquery.SchemaField("Pickup State", "STRING"),
            bigquery.SchemaField("Pickup Country", "STRING"),
            bigquery.SchemaField("Pickup District", "STRING"),
            bigquery.SchemaField("Pickup Organisation", "STRING"),
            bigquery.SchemaField("PickUp Address Type", "STRING"),
            # Drop fields
            bigquery.SchemaField("Drop Name", "STRING"),
            bigquery.SchemaField("Drop Phone", "STRING"),
            bigquery.SchemaField("Drop Email", "STRING"),
            bigquery.SchemaField("Drop Address", "STRING"),
            bigquery.SchemaField("Drop Pincode", "STRING"),
            bigquery.SchemaField("Drop City", "STRING"),
            bigquery.SchemaField("Drop District", "STRING"),
            bigquery.SchemaField("Drop Organisation", "STRING"),
            bigquery.SchemaField("Drop Address Type", "STRING"),
            # Return fields
            bigquery.SchemaField("Return Name", "STRING"),
            bigquery.SchemaField("Return Phone", "STRING"),
            bigquery.SchemaField("Return Address", "STRING"),
            bigquery.SchemaField("Return Pincode", "STRING"),
            bigquery.SchemaField("Return City", "STRING"),
            bigquery.SchemaField("Return State", "STRING"),
            bigquery.SchemaField("Return Country", "STRING"),
            bigquery.SchemaField("Ingestion Date", "DATE")
        ]
    }

    for table_name, schema in table_schemas.items():
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        try:
            # Check if table exists
            client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
        except Exception:
            # Create table
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)
            logger.info(f"Created table {table_id}")

def load_to_bigquery(dataframes: Dict[str, pd.DataFrame]) -> None:
    """Load dataframes to BigQuery tables"""
    logger.info("Starting BigQuery load process...")
    logger.info(f"Processing {len(dataframes)} tables: {', '.join(dataframes.keys())}")
    
    # credentials_info = get_google_credentials_info()
    # if not credentials_info:
    #     raise ValueError("Google credentials not found")
    
    # credentials_info = base64.b64decode(credentials_info).decode("utf-8")
    # credentials_info = json.loads(credentials_info)
    # credentials = service_account.Credentials.from_service_account_info(credentials_info)
    
    # client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    client = bigquery.Client(project=PROJECT_ID)
    logger.info(f"Connected to BigQuery project: {PROJECT_ID}")
    
    # Create tables if they don't exist
    create_tables_if_not_exist(client)
    
    for table_name, df in dataframes.items():
        logger.info(f"\nProcessing table: {table_name}")
        logger.info(f"Number of rows to load: {len(df)}")
        
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        try:
            df.rename(columns={
                'ingestion_date': 'Ingestion Date',
            }, inplace=True)
        except:
            pass
            
        # Handle column renames
        if table_name == 'tracking':
            df.rename(columns={
                'Expected Date of Delivery (Min)': 'Expected Date Of Delivery Min',
                'Expected Date of Delivery (Max)': 'Expected Date Of Delivery Max',
            }, inplace=True)
            
        if table_name == 'additional_info':
            df.rename(columns={
                'UDF_1 (as sent in the OC API)': 'UDF 1',
                'UDF_2 (as sent in the OC API)': 'UDF 2',
                'UDF_3 (as sent in the OC API)': 'UDF 3',
                'UDF_4 (as sent in the OC API)': 'UDF 4',
                'Consignor Code (Gati/Saksham/Bluedart)': 'Consignor Code',
            }, inplace=True)
        
        try:
            # Define schema for numeric columns
            schema = []
            if table_name == 'orders':
                schema = [
                    bigquery.SchemaField("COD Value", "FLOAT64"),
                    bigquery.SchemaField("Invoice Value", "FLOAT64"),
                    bigquery.SchemaField("Items Quantity", "INTEGER"),
                ]
            elif table_name == 'shipping':
                schema = [
                    bigquery.SchemaField("Committed SLA", "FLOAT64"),
                    bigquery.SchemaField("Shipping Cost", "FLOAT64"),
                    bigquery.SchemaField("Box Count", "INTEGER"),
                    bigquery.SchemaField("RTO AWB", "STRING"),
                    bigquery.SchemaField("Zone", "STRING"),
                    bigquery.SchemaField("From Warehouse", "STRING"),
                    bigquery.SchemaField("To Warehouse", "STRING")
                ]
            elif table_name == 'tracking':
                schema = [
                    bigquery.SchemaField("Created at", "TIMESTAMP"),
                    bigquery.SchemaField("Pickup Date", "TIMESTAMP"),
                    bigquery.SchemaField("Latest Timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("Delivery Date", "TIMESTAMP"),
                    bigquery.SchemaField("Updated at", "TIMESTAMP"),
                    bigquery.SchemaField("Out For Delivery 1st Attempt", "TIMESTAMP"),
                    bigquery.SchemaField("Out For Delivery 2nd Attempt", "TIMESTAMP"),
                    bigquery.SchemaField("Out For Delivery 3rd Attempt", "TIMESTAMP"),
                    bigquery.SchemaField("Out For Delivery 4th Attempt", "TIMESTAMP"),
                    bigquery.SchemaField("Out For Delivery 5th Attempt", "TIMESTAMP"),
                    bigquery.SchemaField("TimeStamp Of Last Failed Delivery", "TIMESTAMP"),
                    bigquery.SchemaField("RTO Mark Date", "TIMESTAMP"),
                    bigquery.SchemaField("Out For Pickup 1st Attempt", "TIMESTAMP"),
                    bigquery.SchemaField("Out For Pickup 2nd Attempt", "TIMESTAMP"),
                    bigquery.SchemaField("Out For Pickup 3rd Attempt", "TIMESTAMP"),
                    bigquery.SchemaField("Destination Hub In Scan Time", "TIMESTAMP"),
                    bigquery.SchemaField("Origin Hub In Timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("Origin Hub Out Timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("RTO Intransit Timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("RTO Delivery Date", "TIMESTAMP"),
                    bigquery.SchemaField("Expected delivery date by Courier Partner", "DATE"),
                    bigquery.SchemaField("Expected Date Of Delivery Min", "DATE"),
                    bigquery.SchemaField("Expected Date Of Delivery Max", "DATE"),
                    bigquery.SchemaField("Out For Delivery Attempts", "INTEGER"),
                    bigquery.SchemaField("Out For Pickup Attempts", "INTEGER"),
                    bigquery.SchemaField("Original Appointment Start Time", "TIMESTAMP"),
                    bigquery.SchemaField("Original Appointment End Time", "TIMESTAMP"),
                    bigquery.SchemaField("Current Appointment Start Time", "TIMESTAMP"),
                    bigquery.SchemaField("Current Appointment End Time", "TIMESTAMP")
                ]
            elif table_name == 'package':
                schema = [
                    bigquery.SchemaField("Shipment Length", "INTEGER"),
                    bigquery.SchemaField("Shipment Breadth", "INTEGER"),
                    bigquery.SchemaField("Shipment Height", "INTEGER"),
                    bigquery.SchemaField("Shipment Weight", "INTEGER"),
                    bigquery.SchemaField("Item Length", "FLOAT64"),
                    bigquery.SchemaField("Item Breadth", "FLOAT64"),
                    bigquery.SchemaField("Item Height", "FLOAT64"),
                    bigquery.SchemaField("Actual Weight", "FLOAT64"),
                    bigquery.SchemaField("Chargeable Weight", "FLOAT64"),
                    bigquery.SchemaField("Volumetric Weight", "FLOAT64"),
                    bigquery.SchemaField("SKU Wise Item Quantity", "INTEGER"),
                    bigquery.SchemaField("Return Product URL", "STRING"),
                    bigquery.SchemaField("SKU", "STRING")
                ]
            elif table_name == 'additional_info':
                schema = [
                    bigquery.SchemaField("Holiday Count in EDD", "INTEGER"),
                    bigquery.SchemaField("TIN", "STRING"),
                    bigquery.SchemaField("QC Type", "STRING"),
                    bigquery.SchemaField("Sub Category", "STRING"),
                    bigquery.SchemaField("UDF 1", "STRING"),
                    bigquery.SchemaField("UDF 2", "STRING"),
                    bigquery.SchemaField("UDF 3", "STRING"),
                    bigquery.SchemaField("UDF 4", "STRING"),
                    bigquery.SchemaField("OTP Verified Delivery", "STRING"),
                    bigquery.SchemaField("OTP Verified Cancellation", "STRING"),
                    bigquery.SchemaField("Consignor Code", "STRING"),
                    bigquery.SchemaField("RVP Reason", "STRING"),
                    bigquery.SchemaField("RVP Reason SKU Wise", "STRING"),
                    bigquery.SchemaField("EDD sent over OC API", "STRING")
                ]
            
            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=schema if schema else None,
            )
            
            # Convert columns to appropriate types explicitly
            if table_name == 'orders':
                df['COD Value'] = df['COD Value'].astype(float)
                df['Invoice Value'] = df['Invoice Value'].astype(float)
                df['Items Quantity'] = df['Items Quantity'].astype(int)
            elif table_name == 'shipping':
                df['Committed SLA'] = df['Committed SLA'].astype(float)
                df['Shipping Cost'] = df['Shipping Cost'].astype(float)
                df['Box Count'] = pd.to_numeric(df['Box Count'], errors='coerce').fillna(0).astype(int)

                # Handle Pricing Zone specifically
                if 'Pricing Zone' in df.columns:
                    # Convert to string but handle null values properly
                    df['Pricing Zone'] = df['Pricing Zone'].apply(
                        lambda x: None if pd.isna(x) or str(x).lower() in ['nan', 'none', ''] else str(x)
                    )

                # Convert other string columns
                string_columns = ['RTO AWB', 'Zone', 'From Warehouse', 'To Warehouse', 'Pricing Zone']
                for col in string_columns:
                    if col in df.columns:
                        # Handle null values properly - convert to string but keep None values as None
                        df[col] = df[col].astype(str).replace(['nan', 'None', ''], None)
                        
            elif table_name == 'tracking':
                # Convert timestamp columns
                timestamp_columns = [
                    "Created at", "Pickup Date", "Latest Timestamp", "Delivery Date",
                    "Updated at", "Out For Delivery 1st Attempt", "Out For Delivery 2nd Attempt",
                    "Out For Delivery 3rd Attempt", "Out For Delivery 4th Attempt",
                    "Out For Delivery 5th Attempt", "TimeStamp Of Last Failed Delivery",
                    "RTO Mark Date", "Out For Pickup 1st Attempt", "Out For Pickup 2nd Attempt",
                    "Out For Pickup 3rd Attempt", "Destination Hub In Scan Time",
                    "Origin Hub In Timestamp", "Origin Hub Out Timestamp",
                    "RTO Intransit Timestamp", "RTO Delivery Date",
                    "Original Appointment Start Time",
                    "Original Appointment End Time",
                    "Current Appointment Start Time",
                    "Current Appointment End Time"
                ]
                for col in timestamp_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Convert date columns
                date_columns = [
                    "Expected delivery date by Courier Partner",
                    "Expected Date Of Delivery Min",
                    "Expected Date Of Delivery Max"
                ]
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                        df[col] = df[col].where(df[col].notna(), None)
                
                # Convert integer columns
                integer_columns = [
                    "Out For Delivery Attempts",
                    "Out For Pickup Attempts"
                ]
                for col in integer_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            elif table_name == 'package':
                # Convert integer columns
                int_columns = [
                    'Shipment Length', 'Shipment Breadth', 'Shipment Height', 
                    'Shipment Weight', 'SKU Wise Item Quantity'
                ]
                for col in int_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                
                # Convert float columns
                float_columns = [
                    'Item Length', 'Item Breadth', 'Item Height', 
                    'Actual Weight', 'Chargeable Weight', 'Volumetric Weight'
                ]
                for col in float_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                
                # Convert string columns
                string_columns = ['Return Product URL', 'SKU']
                for col in string_columns:
                    if col in df.columns:
                        df[col] = df[col].astype(str).replace('nan', None)
            elif table_name == 'additional_info':
                # Convert integer columns
                if 'Holiday Count in EDD' in df.columns:
                    df['Holiday Count in EDD'] = pd.to_numeric(df['Holiday Count in EDD'], errors='coerce').fillna(0).astype(int)
                
                # Convert string columns
                string_columns = [
                    'TIN', 'QC Type', 'Sub Category', 'UDF 1', 'UDF 2', 'UDF 3', 'UDF 4',
                    'OTP Verified Delivery', 'OTP Verified Cancellation', 'Consignor Code',
                    'RVP Reason', 'RVP Reason SKU Wise', 'EDD sent over OC API'
                ]
                for col in string_columns:
                    if col in df.columns:
                        df[col] = df[col].astype(str).replace('nan', None)
            
            # Remove ingestion_date column if it exists
            if 'ingestion_date' in df.columns:
                df = df.drop(columns=['ingestion_date'])
            
            job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            
            logger.info(f"✅ Successfully loaded {len(df)} rows to {table_name}")
            
            # Get table info for verification
            table = client.get_table(table_id)
            logger.info(f"Table {table_name} now has {table.num_rows} total rows")
            
        except Exception as e:
            logger.error(f"❌ Error loading data to {table_name}")
            logger.error(f"Error details: {str(e)}")
            raise

def clean_numeric_column(series):
    # Remove any commas and spaces
    if series.dtype == 'object':
        series = series.str.replace(',', '').str.strip()
    # Convert to float, replacing errors with NaN
    return pd.to_numeric(series, errors='coerce')

def main():
    try:
        logger.info("=== Starting Clickpost data extraction and loading process ===")
        
        # Check if credentials are available
        # credentials_info = get_google_credentials_info()
        # if not credentials_info:
        #     raise ValueError("Google credentials not found in environment")
        # logger.info("✅ Google credentials verified")
        
        # Generate report
        logger.info("\n📊 Generating Clickpost report...")
        reference_number = generate_report()
        logger.info(f"Report reference number: {reference_number}")
        
        # Check status every 5 minutes
        max_attempts = 24  # 2 hours maximum wait time
        attempt = 0
        
        while attempt < max_attempts:
            logger.info(f"\n🔄 Checking report status (attempt {attempt + 1}/{max_attempts})")
            status_response = check_report_status(reference_number)
            
            if status_response['meta']['status'] == 200:
                result = status_response['result'][0]
                
                if result['s3_link']:
                    logger.info("✅ Report generation completed")
                    logger.info(f"S3 link: {result['s3_link']}")
                    
                    # Download and process the data
                    logger.info("\n📥 Downloading and processing data...")
                    df = pd.read_csv(result['s3_link'])
                    logger.info(f"Downloaded data shape: {df.shape}")
                    
                    # Process and load data
                    processed_dfs = process_dataframe(df)
                    logger.info("\n⬆️ Loading data to BigQuery...")
                    load_to_bigquery(processed_dfs)
                    
                    logger.info("\n✅ Process completed successfully")
                    break
                    
                elif result['report_status'] == 'IN_PROCESS':
                    logger.info("⏳ Report still generating... waiting 5 minutes")
                    time.sleep(300)  # Wait 5 minutes
                else:
                    raise Exception(f"Unexpected report status: {result['report_status']}")
            else:
                raise Exception(f"Error checking status: {status_response['meta']['message']}")
                
            attempt += 1
            
        if attempt >= max_attempts:
            raise Exception("Maximum attempts reached. Report generation timed out.")
            
    except Exception as e:
        logger.error("\n❌ Process failed")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error("Detailed traceback:")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 