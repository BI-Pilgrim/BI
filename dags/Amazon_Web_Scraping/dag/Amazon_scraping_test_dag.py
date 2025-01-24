from datetime import date, datetime, timedelta
import logging
from typing import TYPE_CHECKING 
from playwright.sync_api import sync_playwright, Browser, Page, Playwright

from airflow.decorators import task, dag





@dag("amazon_scrap_test_dag", schedule='0 7 * * *', start_date=datetime(year=2024,month=12,day=1))
def amazon_scrap_test_dag():

    START = date.today()-timedelta(days=1)
    END = date.today()
    
    @task.bash
    def install_playwright_chromium():
        return "playwright install-deps && playwright install chromium"

    @task.python
    def trigger_login(): 
        with sync_playwright() as p:
            browser = p.firefox.launch(headless=True)
            page = browser.new_page() 
            print('code done')

        

        return 0


    install_playwright_chromium() >> trigger_login

dag = amazon_scrap_test_dag()
