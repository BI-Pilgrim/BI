from datetime import date, datetime, timedelta
import logging
from typing import TYPE_CHECKING
from flipkart.scraper import SellerPortal
from google.oauth2 import service_account
from google.cloud import bigquery
import json
import base64
from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

COL_NAME_REMAP = {"Product Id":"product_id",
"SKU ID":"sku_id",
"Category":"category",
"Brand":"brand",
"Vertical":"vertical",
"Order Date":"order_date",
"Fulfillment Type":"fulfillment_type",
"Location Id":"location_id",
"Gross Units":"gross_units",
"GMV":"gmv",
"Cancellation Units":"cancellation_units",
"Cancellation Amount":"cancellation_amount",
"Return Units":"return_units",
"Return Amount":"return_amount",
"Final Sale Units":"final_sale_units",
"Final Sale Amount":"final_sale_amount"
}

class ReportGeneratedSensor(BaseSensorOperator):
    def __init__(self, *, report_name:str, start:date, end:date, cookies:str, **kwargs):
        super().__init__(**kwargs)
        self.report_name = report_name
        self.start = start
        self.end = end
        self.cookies = cookies
        # import pdb; pdb.set_trace()
        
    
    def poke(self, context:'Context'):
        # logging.info({"Cookie":f"{self.cookies}"})
        logging.info(f"{context['ti'].xcom_pull(task_ids=self.cookies.operator.task_id, key=self.cookies.key)}")
        sp = SellerPortal()
        sp.session.headers.update({"Cookie":f"{context['ti'].xcom_pull(task_ids=self.cookies.operator.task_id, key=self.cookies.key)}"})
        resp = sp.check_report(self.start, self.end, self.report_name)
        return len(resp.downloadLink)>0


def get_bq_client(credentials_info:str)->bigquery.Client:
    credentials_info = base64.b64decode(credentials_info).decode("utf-8")
    credentials_info = json.loads(credentials_info)

    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = bigquery.Client(credentials=credentials, project="shopify-pubsub-project")
    return client

@dag("flipkart_seller_earn_more_report", schedule=None)
def flipkart_seller_earn_more_report():

    START = date.today()-timedelta(days=1)
    END = date.today()
    
    
    @task.bash
    def install_playwright_chromium():
        return "playwright install-deps && playwright install chromium"

    @task.python
    def trigger_login():
        username = Variable.get("FK_USERNAME", default_var="cloud@discoverpilgrim.com")
        password = Variable.get("FK_PASSWORD")
        sp = SellerPortal()
        sp.login(username, password)
        return sp.session.headers["Cookie"]
    
    @task.python
    def generate_report(cookies:str):
        sp = SellerPortal()
        sp.session.headers.update({"Cookie":cookies})
        data = sp.generate_report(START, END)
        return data

    @task.python
    def download_report(cookies:str):

        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        TABLE_NAME = "pilgrim_bi_flipkart_seller.earn_more_report"
        
        sp = SellerPortal()
        sp.session.headers.update({"Cookie":cookies})
        
        data = sp.check_report(START, END)
        df = sp.download_report(data.request_id)
        
        df2 = df.rename(COL_NAME_REMAP, axis=1)
        df2["runid"] = int(datetime.now().strftime("%Y%m%d%H%M%S"))
        
        client = get_bq_client(credentials_info)
        table = client.get_table(TABLE_NAME)
        client.insert_rows_from_dataframe(TABLE_NAME, df2, selected_fields=table.schema)
        
    login_cookies = trigger_login()
    generated_report = generate_report(login_cookies)
    wait_sensor = ReportGeneratedSensor(task_id="check_report_State", start=START, end=END, 
                                        timeout=timedelta(hours=3), poke_interval=timedelta(minutes=10), 
                                        cookies=login_cookies, report_name='earn_more_report', mode='reschedule')
    download_report = download_report(login_cookies)


    install_playwright_chromium() >> login_cookies >> generated_report >> wait_sensor >> download_report

dag = flipkart_seller_earn_more_report()