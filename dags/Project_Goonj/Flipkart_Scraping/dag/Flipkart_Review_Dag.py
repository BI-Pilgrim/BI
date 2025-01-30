from datetime import date, datetime, timedelta
import logging
from typing import TYPE_CHECKING 
from playwright.sync_api import sync_playwright, Browser, Page, Playwright
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from Project_Goonj.Flipkart_Scraping.python.flipkart_Scrape import main


@dag("Flipkart_Review_scrape_dag", schedule='0 7 * * *', start_date=datetime(year=2025,month=1,day=26))
def Flipkart_Review_scrape_dag():

    @task.bash
    def install_playwright_chromium():
        return "playwright install-deps && playwright install firefox"

    @task.python
    def call_main_python():
        main()

    install_playwright_chromium() >> call_main_python()

dag = Flipkart_Review_scrape_dag() 
