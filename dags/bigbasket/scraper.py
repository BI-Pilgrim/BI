from copy import deepcopy
from datetime import datetime
import json
import re
import base64
from typing import List, Optional
from requests import Session
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from airflow.models import Variable
from pydantic import BaseModel
from bigbasket.data_models import Transaction, TransactionsResponseData, TransactionsResponse

from utils.mail_utils import get_mail_body

class BigBasketScraper:
    BASE_URL = "https://nucleus.bigbasket.com"
    def __init__(self):
        self.session = Session()
        self.default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'content-type': 'application/json; charset=utf-8',
            'X-Tenant-ID': '1',
            'Origin': 'https://nucleus.bigbasket.com',
            'Connection': 'keep-alive',
            'Referer': 'https://nucleus.bigbasket.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'DNT': '1',
            'Sec-GPC': '1',
            'Priority': 'u=0',
            'Pragma': 'no-cache'
        }
        self.session.headers.update(self.default_headers)
        self.email = "mini@discoverpilgrim.com"
        self.login()

    def get_gmail_credentials(self) -> Credentials:
        GMAIL_FB_APP_TESTING_TOKEN = json.loads(Variable.get("GMAIL_FB_APP_TESTING_TOKEN", "{}"))
        creds = Credentials.from_authorized_user_info(GMAIL_FB_APP_TESTING_TOKEN)
        creds.refresh(Request())
        return creds

    def get_mails(self, messages_client) -> list:
        mails = []
        get_mails_req = messages_client.list(userId="me", q="subject:Vendor Dashboard OTP AND is:unread", maxResults=10)
        get_mails_resp = get_mails_req.execute()
        while isinstance(get_mails_resp, dict) and get_mails_resp.get("nextPageToken") is not None:
            mails.extend(get_mails_resp['messages'])
            get_mails_resp = messages_client.list_next(get_mails_req, get_mails_resp).execute()
        mails.extend(get_mails_resp.get('messages', []))
        return mails

    def extract_otp(self, mails: list, messages_client) -> str:
        for mail in mails:
            msg = messages_client.get(userId="me", id=mail['id']).execute()
            body = get_mail_body(msg)
            match = re.search(r'\b\d{6}\b', body)
            if match:
                messages_client.modify(userId="me", id=mail["id"], body=dict(removeLabelIds=["UNREAD"])).execute()
                return match.group(0)
        raise Exception("OTP not found in emails")

    def login(self):

        resp = self.session.post(f"{self.BASE_URL}/authz/v1/vendor/otp/", 
                                 data=json.dumps({"email":self.email}))
        if not resp.ok:
            raise Exception(f"Failed to send OTP to {self.email}\n" + resp.text)
        
        creds = self.get_gmail_credentials()
        service = build("gmail", "v1", credentials=creds)
        messages_client = service.users().messages()
        mails = self.get_mails(messages_client)
        otp = self.extract_otp(mails, messages_client)
        
        otp_payload = json.dumps({"email": self.email, "otp": int(otp)})
        resp = self.session.post(f"{self.BASE_URL}/authz/v1/vendor/login/", data=otp_payload)

        if not resp.ok:
            raise Exception(f"Failed to validate OTP to {self.email}, {otp}\n" + resp.text)

    def get_report_variables(self, report_slug="analytics_manufacturer_bbdaily-sales-report") -> dict:
        url = f"{self.BASE_URL}/stitch/vendor/report_variables/?report_slug={report_slug}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def trigger_analytics_manuf_bbdaily_sales_report(self, start_date: datetime, end_date: datetime, cities: List[int]=None) -> dict:
        report_slug = "analytics_manufacturer_bbdaily-sales-report"
        report_variables = self.get_report_variables(report_slug)
        
        variables = report_variables['data']['variables']
        field_ids = {var['name']: var['id'] for var in variables}
        
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        headers = deepcopy(self.default_headers)
        del headers['content-type']
        
        payload = [
            ('report-type', (None, report_slug)),
            (str(field_ids['start_date']), (None, start_date_str)),
            (str(field_ids['end_date']), (None, end_date_str)),
        ]
        
        if cities is None:
            city_var = None
            for var in variables:
                if var['name'] == 'city':
                    cities = [city['id'] for city in var['options']]
                    city_var = var
                    break
            if city_var is None: raise Exception("City variable not found")
            cities = [cval['value'] for cval in city_var['meta']['value']]

        for city in cities:
            payload.append(
                (str(field_ids['city']), (None, str(city)))
            )
        
        payload.append(('email', (None, '')))

        response = self.session.post(
            f"{self.BASE_URL}/stitch/vendor/generate/report/",
            headers=headers,
            data=payload
        )
        
        response.raise_for_status()
        return response.json()
    
    def get_reports(self) -> dict:
        pass

    def get_reports(self, page: int = 1, page_size: int = 10, group: int = 0) -> TransactionsResponse:
        url = f"{self.BASE_URL}/stitch/vendor/transactions/?page={page}&page_size={page_size}&group={group}"
        response = self.session.get(url)
        response.raise_for_status()
        return TransactionsResponse(**response.json())


# Example usage:
# scraper = BigBasketScraper()
# transactions = scraper.get_reports()
# print(transactions)
