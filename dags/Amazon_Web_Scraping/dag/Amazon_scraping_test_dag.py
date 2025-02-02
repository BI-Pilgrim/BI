from datetime import date, datetime, timedelta
import logging
from typing import TYPE_CHECKING 
from playwright.sync_api import sync_playwright, Browser, Page, Playwright
import subprocess
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from Amazon_Web_Scraping.python.AMZ_Review_Test import main



@dag("amazon_scrap_test_dag", schedule='0 7 * * *', start_date=datetime(year=2025,month=1,day=29))
def amazon_scrap_test_dag():

    START = date.today()-timedelta(days=1)
    END = date.today()
    
    @task.bash
    def install_playwright_chromium():
        return "playwright install-deps && playwright install firefox"

    @task.python
    def trigger_login1(a,b): 
        main(a,b)

    install_playwright_chromium() >> trigger_login1(0,5)

dag = amazon_scrap_test_dag() 
