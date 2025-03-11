import json
import re
import base64
from typing import Optional
from requests import Session
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from airflow.models import Variable

from utils.mail_utils import get_mail_body

class BigBasketScraper:
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
                return match.group(0)
        raise Exception("OTP not found in emails")

    def login(self):

        resp = self.session.post("https://nucleus.bigbasket.com/authz/v1/vendor/otp/", 
                                 data=json.dumps({"email":self.email}))
        if not resp.ok:
            raise Exception(f"Failed to send OTP to {self.email}\n" + resp.text)
        
        creds = self.get_gmail_credentials()
        service = build("gmail", "v1", credentials=creds)
        messages_client = service.users().messages()
        mails = self.get_mails(messages_client)
        otp = self.extract_otp(mails, messages_client)
        
        otp_payload = json.dumps({"email": self.email, "otp": int(otp)})
        resp = self.session.post("https://nucleus.bigbasket.com/authz/v1/vendor/login/", data=otp_payload)

        if not resp.ok:
            raise Exception(f"Failed to validate OTP to {self.email}, {otp}\n" + resp.text)

    def post_login_endpoint(self, url: str, data: Optional[dict] = None) -> dict:
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()

# Example usage:
# scraper = BigBasketScraper()
# response = scraper.post_login_endpoint("https://nucleus.bigbasket.com/api/some_endpoint", {"key": "value"})
# print(response)
