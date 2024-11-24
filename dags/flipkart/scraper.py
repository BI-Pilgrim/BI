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
from playwright.sync_api import sync_playwright, Browser, Page, Playwright
from typing import List, Dict
import re

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Content-Type': 'application/json',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://seller.flipkart.com',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'DNT': '1',
    'Sec-GPC': '1',
    'Priority': 'u=4',
}

# re
RE_FK_DOMAIN = re.compile(".*flipkart\.com")

FK_USERNAME = os.getenv("FK_USERNAME", "cloud@discoverpilgrim.com")
FK_PASSWORD = os.getenv("FK_PASSWORD")
FK_EMAIL = os.getenv("FK_EMAIL", "gagan@discoverpilgrim.com")
FK_SELLER_ID =  os.getenv("FK_SELLER_ID", "66ec1162ae694fbe")

# Playwright entries
LGN_BUTTON_SELECTOR = "#app > div > div.styles__MenuWrapperContainer-sc-1lklol6-46.jdvZIE > div > div > div.styles__Block-sc-a90kxg-13.styles__Flex-sc-a90kxg-14.styles__ActionItem-sc-1lklol6-38.etHNIN > button.styles__ButtonStyle-sekd9q-0.klTPPh.styles__PrimaryButton-sc-a90kxg-10.styles__LoginButton-sc-1lklol6-40.huhqUP"
LGN_EMAIL_FIELD_SELECTOR = "#app > div.styles__LoginModal-sc-fj5lxd-20.gPzQoh > div > section > section > div > div.styles__FormWrapper-sc-fj5lxd-6.hDEgDI > form > div:nth-child(1) > div > div.styles__InputContainer-cql555-0.iRkbXM.login-input-container > input"
LGN_TRIGGER_PWD_PROMPT_BTN = "#app > div.styles__LoginModal-sc-fj5lxd-20.gPzQoh > div > section > section > div > div.styles__Footer-sc-fj5lxd-9.khIPRD.login-modal-footer > div.styles__LoginFooterSection-sc-fj5lxd-10.fQWLdp > button"
LGN_PWD_FIELD_SELECTOR = "#app > div.styles__LoginModal-sc-fj5lxd-20.gPzQoh > div > section > section > div > div.styles__FormWrapper-sc-fj5lxd-6.hDEgDI > form > div:nth-child(2) > div > div.styles__InputContainer-cql555-0.iRkbXM.password-input-container > input"
LGN_LOGIN_BTN = "#app > div.styles__LoginModal-sc-fj5lxd-20.gPzQoh > div > section > section > div > div.styles__Footer-sc-fj5lxd-9.khIPRD.login-modal-footer > div.styles__LoginFooterSection-sc-fj5lxd-10.fQWLdp > button"


def get_login_cookie_strings(cookies:List[Dict]):
    cookie_maker = ["is_login=true"]
    for cookie in cookies:
        if  RE_FK_DOMAIN.match(cookie["domain"]) is not None and (
            (cookie["name"] == "connect.sid") or
            (cookie["name"] == "T") or
            (cookie["name"] == "sellerId")):
            cookie_maker.append(f"{cookie['name']}={cookie['value']}")
    return "; ".join(cookie_maker)

@dataclass
class LoginRespData:
    sellerId: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    displayName: Optional[str] = None
    businessName: Optional[str] = None
    city: Optional[str] = None
    pincode: Optional[int] = None
    state: Optional[str] = None
    redirectState: Optional[str] = None
    userId: Optional[str] = None
    task: Optional[str] = None
    newStatus: Optional[str] = None
    isOnBehalf: Optional[bool] = None
    onBehalfEmail: Optional[str] = None
    onBehalfRole: Optional[str] = None
    onBehalfHomePage: Optional[str] = None
    isRefurbSeller: Optional[bool] = None
    domain: Optional[str] = None
    context: Optional[str] = None
    refUrlForOnBehalf: Optional[str] = None
    staticHomepage: Optional[bool] = None

@dataclass
class LoginResp:
    code: str
    message: str
    data: Optional[LoginRespData]
    
@dataclass
class CheckReportRespDLLink:
    url: Optional[str]

@dataclass
class CheckReportRespDL:
    isCSV: Optional[bool]
    link: Optional[CheckReportRespDLLink]
@dataclass
class CheckReportResp:
    request_id:str
    created_at: int
    downloadLink: CheckReportRespDL

class GenerateReportRespReport:
    request_id: Optional[str]
    eta: Optional[str]

@dataclass
class GenerateReportResp:
    report: GenerateReportRespReport

class AuthException(Exception): 
    pass

class SellerPortal:
    
    def __init__(self):
        self.session = Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def _get_login_cookies(self, page:Page,  username:str, password:str):

        page.goto("https://seller.flipkart.com")
        login_trigger = page.locator(LGN_BUTTON_SELECTOR)
        login_trigger.click()
        time.sleep(3)

        user_email = page.locator(LGN_EMAIL_FIELD_SELECTOR)
        user_email.focus()
        page.fill(LGN_EMAIL_FIELD_SELECTOR, username)

        page.locator(LGN_TRIGGER_PWD_PROMPT_BTN).click()

        time.sleep(3)
        user_pwd = page.locator(LGN_PWD_FIELD_SELECTOR)
        user_pwd.focus()
        page.fill(LGN_PWD_FIELD_SELECTOR, password)

        time.sleep(1)
        login_button = page.locator(LGN_LOGIN_BTN)
        login_button.focus()
        login_button.click()

        time.sleep(10)
        return get_login_cookie_strings(page.context.cookies())
    
    def login(self, username:str, password:str):
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page()
            cookies = {'Cookie': self._get_login_cookies(page, username, password)}
            logging.info(cookies)
            self.session.headers.update(cookies)

    def get_seller_features(self):
        return self.session.get("https://seller.flipkart.com/getFeaturesForSeller")
    
    def generate_report(self, start: date, end: date, report_name:str="earn_more_report")->GenerateReportResp:
        resp = self.session.get("https://seller.flipkart.com/napi/metrics/bizReport/report/generateReport", 
            params={
                    "fileName":report_name,
                    "from_date":start.strftime("%Y-%m-%d"),
                    "to_date":end.strftime("%Y-%m-%d"),
                    "emailId":FK_EMAIL,
                    "sellerId":FK_SELLER_ID,
                }
            )
        if not resp.ok:
            logging.error(f"{resp.content}  \n Status_code:: {resp.status_code}")
            error_data = json.loads(resp.content)
            if error_data["error"]["statusCode"]==400:
                if error_data["error"]["error"]["errors"][0]["message"].find("the download link is still active")>0:
                    return asdict(GenerateReportResp(dict(report=dict(request_id="PREGENERATED", eta="0 mins"))))
            raise AuthException("Unable to login")
        return asdict(GenerateReportResp(**resp.json()))

    def check_report(self, start: date, end: date, report_name:str="earn_more_report"):
        resp = self.session.get("https://seller.flipkart.com/napi/metrics/bizReport/report/checkReports", 
            params={
                    "fileName":report_name,
                    "from_date":start.strftime("%Y-%m-%d"),
                    "to_date":end.strftime("%Y-%m-%d"),
                    "emailId":FK_EMAIL,
                    "sellerId":FK_SELLER_ID,
                }
            )
        if not resp.ok:
            logging.error(f"{resp.content}  \n Status_code:: {resp.status_code}")
            raise AuthException("Unable to login")
        return CheckReportResp(**resp.json())
    
    def download_report(self, report_id:str, report_name:str='earn_more_report')->pd.DataFrame:
        resp = self.session.get(f"https://seller.flipkart.com/napi/metrics/bizReport/downloadReport/{report_name}.xlsx", params={"token":report_id})
        if not resp.ok:
            logging.error(f"{resp.content}  \n Status_code:: {resp.status_code}")
            raise AuthException("Unable to login")
        print(f"https://seller.flipkart.com/napi/metrics/bizReport/downloadReport/{report_name}.xlsx?token={report_id}")
        data = BytesIO(resp.content)
        data.seek(0)
        return pd.read_excel(data) 
