from datetime import datetime
import logging
from airflow.decorators import task, dag
from airflow.models import Variable
import pandas as pd
from utils.google_cloud import get_bq_client,  get_gsheets_client
import json

TABLE_NAME = "adhoc_data_asia.Product_SKU_mapping_D2C_static"
COLUMN_MAP = {
'ID':"ID",	
'Product_Title':"Product_Title",	
'Variant_ID':"Variant_ID",	
'Variant_SKU':"Variant_SKU",	
'Parent_SKU':"Parent_SKU",	
'Type_of_Product':"Type_of_Product",	
'Main_Category':"Main_Category",	
'Sub_Category':"Sub_Category",	
'Range':"Range",	
'Acc_Range':"Acc_Range",	
'Concern':"Concern",	
'MRP':"MRP",	
'MRP_OG':"MRP_OG",
'SH_Concern':"SH_Concern",

}

@dag("gsheet_to_gbq_product_sku_mapping", schedule='0 22 * * *', start_date=datetime(year=2025,month=1,day=20), tags=["purple"])
def shopify_gsheet_to_gbq():

    @task.python
    def fetch_and_load_sheet():
        logging.info(f"Started Task")
        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        D2C_product_sku_mapping =  json.loads(Variable.get("GSHEETS_TOKEN", "{}"))
        gsheets_client = get_gsheets_client(D2C_product_sku_mapping)
        bq_client = get_bq_client(credentials_info)
        sheet = gsheets_client.spreadsheets()

        result = (
        sheet.values()
        .get(spreadsheetId="1AdpbbBn2A2AFbBU1VYTW_4kSPm-eqWV7KWwZ9PGmd5g", range="Product_id_mapping")
        .execute()
        )
        values = result.get("values", [])
        # df = pd.DataFrame(values[1:], columns=[COLUMN_MAP[x.lower().strip()] for x in values[0]])
        df = pd.DataFrame(values)
        table = bq_client.get_table(TABLE_NAME)
        bq_client.query(f"TRUNCATE table {TABLE_NAME}").result()
        bq_client.insert_rows_from_dataframe(TABLE_NAME, df, selected_fields=table.schema)
    
    resp = fetch_and_load_sheet()

gsheet_to_gbq_product_sku_mapping = shopify_gsheet_to_gbq()
