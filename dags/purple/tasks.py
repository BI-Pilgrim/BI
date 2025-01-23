from datetime import datetime
import logging
from airflow.decorators import task, dag
from airflow.models import Variable
import pandas as pd
from utils.google_cloud import get_bq_client,  get_gsheets_client
import json

TABLE_NAME = "pilgrim_bi_purple.claims_report"


COLUMN_MAP = {'date': "date",
 'category_name': "category_name",
 'ean': "ean",
 'sku': "sku",
 'orders': "orders",
 'nmv': "nmv",
 'mrp': "mrp",
 'quantity': "quantity",
 'price off %': "price_off_percentage",
 'absolute price off': "absolute_price_off",
 'off invoice': "off_invoice",
}

@dag("purple_email_claims", schedule='0 22 * * *', start_date=datetime(year=2025,month=1,day=20), tags=["purple"])
def purple_email_claims_sheet():

    @task.python
    def fetch_and_load_sheet():
        logging.info(f"Started Task")
        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        GMAIL_FB_APP_TESTING_TOKEN = json.loads(Variable.get("GSHEETS_TOKEN", "{}"))
        gsheets_client = get_gsheets_client(GMAIL_FB_APP_TESTING_TOKEN)
        bq_client = get_bq_client(credentials_info)
        sheet = gsheets_client.spreadsheets()

        result = (
        sheet.values()
        .get(spreadsheetId="1NbFMYeaJGxAivNm3enljbPvnefj4YJ4uI0fbsLzX4n8", range="Sheet1")
        .execute()
        )
        values = result.get("values", [])
        df = pd.DataFrame(values[1:], columns=[COLUMN_MAP[x.lower().strip()] for x in values[0]])

        df["runid"] = int(datetime.now().strftime("%Y%m%d%H%M%S"))
        df['date'] = pd.to_datetime(df['date'], dayfirst=True).dt.date
        df['price_off_percentage'] = pd.to_numeric(df['price_off_percentage'].str.replace("%", "").str.replace(",", ""))
        df['mrp'] = pd.to_numeric(df['mrp'].str.replace(",", ""))
        df['nmv'] = pd.to_numeric(df['nmv'].str.replace(",", ""))
        df['quantity'] = pd.to_numeric(df['quantity'].str.replace(",", ""))
        df['orders'] = pd.to_numeric(df['orders'].str.replace(",", ""))
        df['absolute_price_off'] = pd.to_numeric(df['absolute_price_off'].str.replace(",", ""))
        df['off_invoice'] = pd.to_numeric(df['off_invoice'].str.replace(",", ""))
        
        
        table = bq_client.get_table(TABLE_NAME)
        bq_client.query(f"TRUNCATE table {TABLE_NAME}").result()
        bq_client.insert_rows_from_dataframe(TABLE_NAME, df, selected_fields=table.schema)
    
    resp = fetch_and_load_sheet()

purple_email_claims_dag = purple_email_claims_sheet()