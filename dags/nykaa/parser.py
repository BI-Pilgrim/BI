from datetime import datetime
import re
import time
from typing import Optional, Union
import pandas as pd
from google.cloud import bigquery
import pyxlsb
from airflow.models import Variable

from utils.google_cloud import get_bq_client

class ExtractReportData:
    def __init__(self, file: Union[str, bytes]):
        """
        file: str or bytes
        """
        # self.file_path = file_path
        self.workbook = pyxlsb.open_workbook(file)
        credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
        self.bigquery = get_bq_client(credentials_info)
        # self.workbook.close()

    def get_start_row(self, sheet: pyxlsb.Worksheet, value: str, start_col:int=0, end_col:Optional[int]=None) -> int:
        for i, row in enumerate(sheet.rows()):
            cells = list(row)
            if end_col is not None: cells = cells[start_col:end_col+1]
            else: cells = cells[start_col:]
            # print(cells)
            if cells and cells[0].v and cells[0].v.lower().strip() == value:
                return i
        return 0

    def sync(self, mail_date:datetime, runid:int, schema:str):
        df1 = self.parse_assortments()
        self.load_data_to_bigquery(f"{schema}.assortments", df1, mail_date, runid)
        print("parsed assortments")

        df2 = self.parse_brand_lvl_dashboard()
        self.load_data_to_bigquery(f"{schema}.brand_lvl_dashboard", df2, mail_date, runid)
        print("parsed brand_lvl_dashboard")

        df3 = self.parse_velocity_lvl_dashboard()
        self.load_data_to_bigquery(f"{schema}.velocity_lvl_dashboard", df3, mail_date, runid)
        print("parsed velocity_lvl_dashboard")

        df4 = self.parse_sku_lvl_dashboard()
        self.load_data_to_bigquery(f"{schema}.sku_lvl_dashboard", df4, mail_date, runid)
        print("parsed sku_lvl_dashboard")

        df5 = self.parse_inv_ageing()
        self.load_data_to_bigquery(f"{schema}.inv_ageing", df5, mail_date, runid)
        print("parsed inv_ageing")

        df6 = self.parse_sku_inv()
        self.load_data_to_bigquery(f"{schema}.sku_inv", df6, mail_date, runid)
        print("parsed sku_inv")

        df7 = self.parse_open_po_summary()
        self.load_data_to_bigquery(f"{schema}.open_po_summary", df7, mail_date, runid)
        print("parsed open_po_summary")

        df8 = self.parse_fill_summary()
        self.load_data_to_bigquery(f"{schema}.fill_summary", df8, mail_date, runid)
        print("parsed fill_summary")

        df9 = self.parse_sku_level_fill()
        self.load_data_to_bigquery(f"{schema}.sku_level_fill", df9, mail_date, runid)
        print("parsed sku_level_fill")

        df10 = self.parse_grn_details()
        self.load_data_to_bigquery(f"{schema}.grn_details", df10, mail_date, runid)
        print("parsed grn_details")

        df11_1, df11_2 = self.parse_appointment_adherence_summary()
        self.load_data_to_bigquery(f"{schema}.parse_appointment_adherence_summary_t1", df11_1, mail_date, runid)
        self.load_data_to_bigquery(f"{schema}.parse_appointment_adherence_summary_t2", df11_2, mail_date, runid)
        print("parsed appointment_adherence_summary")

        df12 = self.parse_appointment_adherence()
        self.load_data_to_bigquery(f"{schema}.appointment_adherence", df12, mail_date, runid)
        print("parsed appointment_adherence")

        df13 = self.parse_inward_discrepancy()
        self.load_data_to_bigquery(f"{schema}.inward_discrepancy", df13, mail_date, runid)
        print("parsed inward_discrepancy")

        df14 = self.parse_open_rtv()
        self.load_data_to_bigquery(f"{schema}.open_rtv", df14, mail_date, runid)
        print("parsed open_rtv")
        

    def load_data_to_bigquery(self, table_id, data, extracted_at,  _retry=0, max_retry=3):
        """Load the data into BigQuery."""
        print(f"Loading {table_id} data to BigQuery")
        data["pg_extracted_at"] = extracted_at
        # table = self.client.get_table(self.table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # schema=table.schema,
        )
        try:
            job = self.bigquery.load_table_from_dataframe(data, table_id, job_config=job_config)
            job.result()
        except Exception as e:
            if _retry+1<max_retry: 
                mint = 60
                print('Error:', str(e))
                print(f"SLEEPING :: Error inserting to BQ retrying in {mint} min")
                time.sleep(60*mint) # 15 min
                return self.load_data_to_bigquery(data, extracted_at, _retry=_retry+1, max_retry=max_retry)
            raise e
        
    @staticmethod
    def fix_col_name(col_name: Optional[str]):
        if(col_name is None):
            return None
        col_name = col_name.replace('%', 'per').lower()
        return re.compile(r'(\s+)|(-)|([\(\)])').sub(' ', col_name).strip().replace(' ', '_').lower()

    def get_non_empty_rows(self, sheet, start_row, start_col:int, col_len:int):
        i = 0
        rows = []
        for row in sheet.rows():
            if(i<start_row): 
                i+=1
                continue
            cells = list(row)
            cells = cells[start_col:start_col+col_len]
            if all([(y.v is None or y.v == '') for y in cells]): break
            rows.append([x.v for x in cells[:col_len]])
        return rows
    
    def get_non_null_cols(self, sheet, start_row, start_col:int=0, col_len:Optional[int]=None):
        i=0
        for row in sheet.rows():
            if col_len is not None: cells = row[start_col:start_col+col_len]
            else: cells = row[start_col:]
            if i == start_row:
                return [x.v for x in cells if x.v is not None]
            i+=1
            
    
    def parse_assortments(self, sheet_index: int = 2) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'brand name')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_brand_lvl_dashboard(self, sheet_index: int = 3) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'brand name')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_velocity_lvl_dashboard(self, sheet_index: int = 4) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'brand name')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_sku_lvl_dashboard(self, sheet_index: int = 5) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'sku status')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_inv_ageing(self, sheet_index: int = 6) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'brand name')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_sku_inv(self, sheet_index: int = 7) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'brand name')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_open_po_summary(self, sheet_index: int = 8) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'wh location')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        df = pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
        df.replace("(blank)", None, inplace=True)
        return df
    
    def parse_fill_summary(self, sheet_index: int = 9) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'wh location')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_sku_level_fill(self, sheet_index: int = 10) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'brand name')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_grn_details(self, sheet_index: int = 11) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'brand name')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_appointment_adherence_summary(self, sheet_index: int = 12) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'month', 0, 4)
        non_null_cols = self.get_non_null_cols(sheet, start_row, 0, 5)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        df1 =  pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )

        start_row = self.get_start_row(sheet, 'wh', 7, 11)
        non_null_cols = self.get_non_null_cols(sheet, start_row, 7, 4)
        rows = self.get_non_empty_rows(sheet, start_row+1, 7, len(non_null_cols))
        df2 = pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
        return (df1, df2)
    
    def parse_appointment_adherence(self, sheet_index: int = 13) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'parent')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_inward_discrepancy(self, sheet_index: int = 14) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'product sku')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
    def parse_open_rtv(self, sheet_index: int = 15) -> pd.DataFrame:
        sheet = self.workbook.get_sheet(self.workbook.sheets[sheet_index])
        start_row = self.get_start_row(sheet, 'rtv no')
        non_null_cols = self.get_non_null_cols(sheet, start_row)
        rows = self.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))
        return pd.DataFrame(rows).rename(
            columns={i: self.fix_col_name(col) for i, col in enumerate(non_null_cols)}
        )
    
# file_path = "./nykaa/pilgrim.xlsb"
# from nykaa.parser import ExtractReportData
# from datetime import datetime
# extractor = ExtractReportData(file_path)
# extractor.sync(datetime.now(), 1, "pilgrim_bi_nykaa")

# df = extractor.parse_assortments()
# print("parsed assortments")
# df2 = extractor.parse_brand_lvl_dashboard()
# print("parsed brand_lvl_dashboard")
# df3 = extractor.parse_velocity_lvl_dashboard()
# print("parsed velocity_lvl_dashboard")
# df4 = extractor.parse_sku_lvl_dashboard()
# print("parsed sku_lvl_dashboard")
# df5 = extractor.parse_inv_ageing()
# print("parsed inv_ageing")
# df6 = extractor.parse_sku_inv()
# print("parsed sku_inv")
# df7 = extractor.parse_open_po_summary()
# print("parsed open_po_summary")
# df8 = extractor.parse_fill_summary()
# print("parsed fill_summary")
# df9 = extractor.parse_sku_level_fill()
# print("parsed sku_level_fill")
# df10 = extractor.parse_grn_details()
# print("parsed grn_details")
# df11_1, df11_2 = extractor.parse_appointment_adherence_summary()
# print("parsed appointment_adherence_summary")
# df12 = extractor.parse_appointment_adherence()
# print("parsed appointment_adherence")
# df13 = extractor.parse_inward_discrepancy()
# print("parsed inward_discrepancy")
# df14 = extractor.parse_open_rtv()
# print("parsed open_rtv")



# workbook = pyxlsb.open_workbook(file_path)
# sheet = workbook.get_sheet(workbook.sheets[12])
# def get_start_row(sheet: pyxlsb.Worksheet, value: str, start_col:int=0, end_col:Optional[int]=None) -> int:
#     for i, row in enumerate(sheet.rows()):
#         cells = list(row)
#         if end_col is not None: cells = cells[start_col:end_col+1]
#         else: cells = cells[start_col:]
#         # print(cells)
#         if cells and cells[0].v and cells[0].v.lower().strip() == value:
#             return i
#     return 0

# def get_non_null_cols(sheet, start_row, start_col:int, col_len:int):
#     i=0
#     for row in sheet.rows():
#         if i == start_row:
#             return [x.v for x in row[start_col:start_col+col_len] if x.v is not None]
#         i+=1

# def get_non_empty_rows(sheet, start_row, start_col:int, col_len:int):
#         i = 0
#         rows = []
#         for row in sheet.rows():
#             # print(i)
#             if(i<start_row): 
#                 i+=1
#                 continue
#             cells = list(row)
#             cells = cells[start_col:start_col+col_len]
#             # print(cells)
#             if all([(y.v is None or y.v == '') for y in cells]): break
#             rows.append([x.v for x in cells[:col_len]])
#         return rows

# get_start_row(sheet, 'brand name')

# start_row = extractor.get_start_row(sheet, 'month', 0, 4)
# non_null_cols = extractor.get_non_null_cols(sheet, start_row, 0, 5)
# rows = extractor.get_non_empty_rows(sheet, start_row+1, 0, len(non_null_cols))

# start_row = extractor.get_start_row(sheet, 'wh', 7, 11)
# non_null_cols = extractor.get_non_null_cols(sheet, start_row, 7, 4)
# rows = extractor.get_non_empty_rows(sheet, start_row+1, 7, len(non_null_cols))
