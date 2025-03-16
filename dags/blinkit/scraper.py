import base64
import io
import json
import os
import re
import time
from typing import Optional, Tuple
import pandas as pd
from requests import Session
from datetime import datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from playwright.sync_api import sync_playwright, Browser, Page, Playwright
from playwright_stealth import Stealth
from airflow.models import Variable

from utils.mail_utils import get_mail_body
from utils.google_cloud import get_gcs_client, upload_to_gcs

class BlinkItAdsScraper:
    BASE_URL = "https://brands.blinkit.com"
    def __init__(self):
        self.session = Session()
        self.gcs_client = get_gcs_client(Variable.get("GOOGLE_CLOUD_STORE_TOKEN"))
        self.default_headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json;charset=UTF-8',
            'origin': 'https://brands.blinkit.com',
            'priority': 'u=1, i',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        }
        self.session.headers.update(self.default_headers)
        self.token = self.login_and_get_token()
        self.session.headers.update({"firebase_user_token": self.token})
        print(self.gcs_client, "GCS CLIENT")

    def get_login_url(self) -> str:
        creds = self.get_gmail_credentials()
        service = build("gmail", "v1", credentials=creds)
        messages_client = service.users().messages()
        mails = self.get_mails(messages_client)
        login_url = self.extract_login_url(mails, messages_client)
        return login_url
        #return self.login_and_get_token(login_url)

    def get_gmail_credentials(self) -> Credentials:
        GMAIL_FB_APP_TESTING_TOKEN = json.loads(Variable.get("GMAIL_FB_APP_TESTING_TOKEN", "{}"))
        creds = Credentials.from_authorized_user_info(GMAIL_FB_APP_TESTING_TOKEN)
        creds.refresh(Request())
        return creds

    def get_mails(self, messages_client) -> list:
        mails = []
        get_mails_req = messages_client.list(userId="me", q="subject:Sign in to Blinkit Brand Central AND is:unread", maxResults=10)
        get_mails_resp = get_mails_req.execute()
        while isinstance(get_mails_resp, dict) and get_mails_resp.get("nextPageToken") is not None:
            mails.extend(get_mails_resp['messages'])
            get_mails_resp = messages_client.list_next(get_mails_req, get_mails_resp).execute()
        mails.extend(get_mails_resp.get('messages', []))
        return mails

    def extract_login_url(self, mails: list, messages_client) -> str:
        for mail in mails:

            msg = messages_client.get(userId="me", id=mail['id']).execute()
            body = get_mail_body(msg)
            body = base64.urlsafe_b64decode(msg['payload']['body']['data']).decode('utf-8')
            match = re.search(r'\"(https:\/\/[\w.]+sendgrid\.net\/[^"]+)', body)
            print(match, len(match.groups()), match.group(1))
            if match and len(match.groups()) >= 1:
                messages_client.modify(userId="me", id=mail["id"], body=dict(removeLabelIds=["UNREAD"])).execute()
                return match.group(1)
        raise Exception("Login URL not found in emails")

    def login_and_get_token(self) -> str:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            stealth = Stealth()
            context = browser.new_context()
            stealth.apply_stealth_sync(context)
            page = context.new_page()
            page.goto(self.BASE_URL)
            page.locator(".sc-dcJsrY").click()
            page.locator("#login_email").fill("meghna@discoverpilgrim.com") # Update
            page.locator("#login_persist").click()
            page.locator("#login > div:nth-child(4) > div > div > div > button").click()
            page.screenshot(path="login.ignore.png")
            self._upload_screenshot_to_gcs("login.ignore.png", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
            time.sleep(10)
            page.screenshot(path="login2.ignore.png")
            self._upload_screenshot_to_gcs("login2.ignore.png", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
            login_url = self.get_login_url()
            page.goto(login_url)
            time.sleep(10)
            ss = page.context.storage_state()
            state_str = [x for x in ss['origins'][0]['localStorage'] if x["name"] == "state"][0]["value"]
            token = json.loads(state_str)["login"]["token"]
            page.screenshot(path="token.ignore.png")
            self._upload_screenshot_to_gcs("token.ignore.png", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
            
            browser.close()
            return token

    def _upload_screenshot_to_gcs(self, file_path: str, run_datetime: str):
        bucket_name = "airflow-data-download"
        destination_blob_name = f"brands.blinkit.com/{run_datetime}/{os.path.basename(file_path)}"
        upload_to_gcs(self.gcs_client, bucket_name, file_path, destination_blob_name)

    def download_ad_summary(self, from_date: datetime, to_date: datetime, campaign_types: Optional[list] = None) -> dict:
        if campaign_types is None:
            campaign_types = [
                "PRODUCT_LISTING",
                "BANNER_LISTING",
                "PRODUCT_RECOMMENDATION",
                "SEARCH_SUGGESTION",
                "BRAND_BOOSTER"
            ]
        from_date_str = from_date.strftime("%m/%d/%Y")
        to_date_str = to_date.strftime("%m/%d/%Y")
        url = f'{self.BASE_URL}/adservice/v2/advertisers/campaigns/reports/download'
        data = {
            "from_date": from_date_str,
            "to_date": to_date_str,
            "campaign_types": campaign_types
        }
        response = self.session.post(url, headers=self.default_headers, json=data)
        response.raise_for_status()
        data = response.json()
        
        if not data["success"]:
            raise Exception("Failed to download ad summary")
        
        report_url = data["data"]["url"]
        return self._download_excel(report_url)
    
    def _download_excel(self, url: str) -> Tuple[pd.DataFrame]:
        """
        returns DFs product_listing, brand_booster, product_recommendation 
        """
        response = self.session.get(url, headers=self.default_headers)
        response.raise_for_status()
        data = io.BytesIO(response.content)
        dfs = pd.read_excel(data, sheet_name=None)
        return dfs

    def _process_dataframe(self, df: pd.DataFrame, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        df["pg_extracted_at"] = datetime.now()
        if "start_date" not in df.columns:
            df["start_date"] = start_date
        if "end_date" not in df.columns:
            df["end_date"] = end_date
        return df
    

# Example usage:
# scraper = BlinkItAdsScraper()
# response = scraper.post_login_endpoint("https://brands.blinkit.com/api/some_endpoint", {"key": "value"})
# print(response)
