import os
from urllib.parse import urlparse
from google.oauth2 import service_account
from google.cloud import bigquery, storage
import json
import base64
import json

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import base64


def get_bq_client(credentials_info:str)->bigquery.Client:
    credentials_info = base64.b64decode(credentials_info).decode("utf-8")
    credentials_info = json.loads(credentials_info)
    
    SCOPES = ['https://www.googleapis.com/auth/bigquery',
          'https://www.googleapis.com/auth/drive.readonly']
    
    credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
    client = bigquery.Client(credentials=credentials, project="shopify-pubsub-project")
    return client


def get_gmail_client(auth_json):
    creds = Credentials.from_authorized_user_info(auth_json)
    creds.refresh(Request())
    return build("gmail", "v1", credentials=creds)

def get_gsheets_client(auth_json):
    creds = Credentials.from_authorized_user_info(auth_json)
    creds.refresh(Request())
    return build("sheets", "v4", credentials=creds)

def get_base_name_from_uri(uri):
    parsed_url = urlparse(uri)
    return os.path.basename(parsed_url.path)

def get_playstore_token(auth_json):
    creds = Credentials.from_authorized_user_info(auth_json)
    creds.refresh(Request())
    return creds.token

def get_gcs_client(credentials_info)->storage.Client:
    credentials_info = json.loads(credentials_info)
    
    SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
    
    credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
    client = storage.Client(credentials=credentials, project="shopify-pubsub-project")
    return client

def upload_to_gcs(client:storage.Client, bucket_name:str, file_path:str, destination_blob_name:str):
    """Uploads a file to Google Cloud Storage."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    return f"gs://{bucket_name}/{destination_blob_name}"

def get_blob_uri(bucket_name, blob_name):
    """Returns the URI of a blob in Google Cloud Storage."""
    return f"gs://{bucket_name}/{blob_name}"

def download_from_gcs(client:storage.Client, bucket_name:str, source_blob_name:str, destination_file_name:str):
    """Downloads a file from Google Cloud Storage."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    return destination_file_name