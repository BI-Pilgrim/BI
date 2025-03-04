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

if __name__ == "__main__":
    from playwright.sync_api import sync_playwright, Browser, Page, Playwright
    from playwright_stealth import Stealth
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=False)
    stealth = Stealth()
    context = browser.new_context()
    stealth.apply_stealth_sync(context)
    page = context.new_page()
    page.goto("https://seller.nykaa.com/login")
    page.locator("#cm-name").fill("zaid@discoverpilgrim.com")
    page.locator("#login-page-wrapper > div > aside > form > div:nth-child(2) > div.action-section > div:nth-child(1) > div > button").click()

    page.goto("https://www.nykaa.com/skin/c/8377")
    cookies = {'Cookie': self._get_login_cookies(page, username, password)}
    logging.info(cookies)
    self.session.headers.update(cookies)