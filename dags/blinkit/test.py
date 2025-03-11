import json
from typing import Optional
from requests import Session
import os
from datetime import date
from dataclasses import dataclass, asdict
import pandas as pd
from io import BytesIO
import logging
import time
from typing import List, Dict
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.cloud import bigquery
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from airflow.models import Variable
from nykaa.parser import ExtractReportData

if __name__ == "__main__":
    from playwright.sync_api import sync_playwright, Browser, Page, Playwright
    from playwright_stealth import Stealth
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=False)
    stealth = Stealth()
    context = browser.new_context()
    stealth.apply_stealth_sync(context)
    page = context.new_page()
    page.goto("https://brands.blinkit.com/")
    page.locator(".sc-dcJsrY").click()
    page.locator("#login_email").fill("meghna@discoverpilgrim.com")
    page.locator("#login_persist").click()
    page.locator("#login > div:nth-child(4) > div > div > div > button").click()

    default_headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://brands.blinkit.com',
        'priority': 'u=1, i',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    }
    GMAIL_FB_APP_TESTING_TOKEN = json.loads(Variable.get("GMAIL_FB_APP_TESTING_TOKEN", "{}"))
        

    creds = Credentials.from_authorized_user_info(GMAIL_FB_APP_TESTING_TOKEN)
    creds.refresh(Request())
    service = build("gmail", "v1", credentials=creds)
    # results = service.users().labels().list(userId="me").execute()

    messages_client = service.users().messages()
    attachments_client = messages_client.attachments()

    mails = []
    get_mails_req = messages_client.list(userId="me", q="subject:Sign in to Blinkit Brand Central AND is:unread", maxResults=10)
    get_mails_resp = get_mails_req.execute()

    while( isinstance(get_mails_resp, (dict,)) and get_mails_resp.get("nextPageToken") is not None):
        mails.extend(get_mails_resp['messages'])
        get_mails_resp = messages_client.list_next(get_mails_req, get_mails_resp).execute()

    mails.extend(get_mails_resp.get('messages', []))
    # Get Login url from email
    page.goto("https://u37106689.ct.sendgrid.net/ls/click?upn=u001.78zVw2SCK9ig9H-2B0xAk3hiMOkvh32KRewpgQ67SvxlAsTGlXlZ-2Frf3BII1qq8Q-2BsNEyEEg60VWSNU432GAAQVyvR-2B2TIPuHZjhULJzSGfdrmWY5ZpLoKN1vI5c0SXIkPYE-2FgtHeFVtv42n5vDUdSHpb5XpQRZ43wdizY0ovfIe3UTsXh-2BxV2-2BStr44zpzG-2BYEcz9B7lPMbiwGYuJfHqR-2BDjX9OS4AnYDxRHQctDLXMwb2rMhT-2F-2BBoXv6l9MgG3MAU5Fz_gHh7PaMDX0Q-2BeU0c3BE7ZmZkfY2eg7cLPd0NLDYqtWaftikNFpLtOdbanWu0PU7n5fYOnGquZWi7g-2FCJvDaZMtGQCk2WAe6zZuF4NrPa5gCAaKNP2cxAE4IQFO6xQQSgVk7rNomC7gGLPicpSFoA1XvmwaaB60mxZDkH0z-2B1vo8gyPJkP7bzyxL6k-2BNqHEnasymW4KKcklnW5lWeq0uRldKpCqUuAJs2nTtjh9RbM9vAnOskIa5ASTYCJziPelg8Cssas-2B-2BVg-2FuGm84LgHzNF6TnVO5oSRLrMxDuZdXHHK08ddjJUAn7GpcbLM8mk1hMB-2FpZuNC-2FhUbU1QRGa2D3aZLPi4qJJVpMUymGVnr8YW40-2FyeprLC6YiPsLSzLy4Rx0lFRmadbvs-2B72CSY6biZB3apjd-2FTIl8uCpNL9A405414qq6qaNf2D2xKedodHpm-2BhMj-2FYJN-2FGCb7vaa5zEgo3L2fzJg09ypsTUaiasC0EzkMMCdQbmbqoIZBWL8Dxv5GC0rfvCB-2BVvEVBLDc-2FEZO-2FKdHicjEh-2F0sv0fQ7SfDEUnGsKOfLj0Omii4w-2B7DS-2F3bsJjjXLvwDEE6Pd5atvrDUZusLN8ezhImOAO6FLm6O0CKkIGzT-2Bdq-2FcZuY2OpawcbFmXmKBNO5Ej7IN9NdD8vvw-3D-3D")
    ss = page.context.storage_state()
    state_str = [x for x in ss['origins'][0]['localStorage'] if x["name"]=="state"][0]["value"]
    token = json.loads(state_str)["login"]["token"]
    self.session.headers.update({"firebase_user_token": token})


