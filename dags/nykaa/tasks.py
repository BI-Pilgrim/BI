import base64
from datetime import datetime
from io import BytesIO
import logging
from airflow.decorators import task, dag
from airflow.models import Variable
import pandas as pd
from utils.google_cloud import get_bq_client, get_gmail_client,  get_gsheets_client
import json

import logging
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from airflow.models import Variable
from nykaa.parser import ExtractReportData


SCHEMA = "pilgrim_bi_nykaa"
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify"
]


@dag("nykaa_email_report_sheet", schedule='0 10 * * *', start_date=datetime(year=2025,month=1,day=20), catchup=False, tags=["nykaa"])
def nykaa_email_report_sheet():

    @task.python
    def fetch_and_load_sheet():
        GMAIL_FB_APP_TESTING_TOKEN = json.loads(Variable.get("GMAIL_FB_APP_TESTING_TOKEN", "{}"))
        

        creds = Credentials.from_authorized_user_info(GMAIL_FB_APP_TESTING_TOKEN)
        creds.refresh(Request())
        service = build("gmail", "v1", credentials=creds)
        # results = service.users().labels().list(userId="me").execute()

        messages_client = service.users().messages()
        attachments_client = messages_client.attachments()

        mails = []
        get_mails_req = messages_client.list(userId="me", q="subject:Beauty EComm Control Tower - Weekly Report AND has:attachment AND is:unread", maxResults=10)
        get_mails_resp = get_mails_req.execute()

        while( isinstance(get_mails_resp, (dict,)) and get_mails_resp.get("nextPageToken") is not None):
            mails.extend(get_mails_resp['messages'])
            get_mails_resp = messages_client.list_next(get_mails_req, get_mails_resp).execute()

        mails.extend(get_mails_resp.get('messages', []))

        for mail in mails:
            email_data = messages_client.get(userId="me", id=mail["id"]).execute()
            mail_attachment = list(filter(lambda x:x['filename'].lower().find("xlsb")>=0, email_data['payload']['parts']))[0]
            logging.info(f"processing {mail_attachment['filename']}")
            attachment_data = attachments_client.get(userId="me", messageId=mail["id"], id=mail_attachment['body']['attachmentId']).execute()
            attachment_bytes = BytesIO(base64.urlsafe_b64decode(attachment_data['data'] + '=' * (4 - len(attachment_data['data']) % 4)))
            mail_date = datetime.fromtimestamp(int(email_data['internalDate'])//1000)
            email_headers = email_data['payload']['headers']
            mail_subject = [header['value'] for header in email_headers if header['name'] == 'Subject']
            mail_subject = mail_subject[0] if mail_subject else None

            extractor = ExtractReportData(attachment_bytes, mail_subject)
            extractor.sync(mail_date, int(datetime.now().strftime("%Y%m%d%H%M%S")), SCHEMA)
            messages_client.modify(userId="me", id=mail["id"], body=dict(removeLabelIds=["UNREAD"])).execute()
    
    load_sheet = fetch_and_load_sheet()

nykaa_email_report_sheet()