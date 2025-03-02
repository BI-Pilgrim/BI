import os
from urllib.parse import urlparse
from google.oauth2 import service_account
from google.cloud import bigquery
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