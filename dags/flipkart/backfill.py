import sys, os
sys.path.append(os.getcwd())

import datetime
from tqdm import tqdm
from flipkart.tasks import COL_NAME_REMAP, get_bq_client
from flipkart.scraper import SellerPortal
from datetime import date, timedelta
from airflow.models import Variable
import time
import pickle


credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS")
username = Variable.get("FK_USERNAME", default_var="cloud@discoverpilgrim.com")
password = Variable.get("FK_PASSWORD")

DATE_STATUS_TRACKER_PKL = "flipkart/fk_earn_more_bkfill_tracker.ignore.pkl"
LOGIN_PKL = "./flipkart/login_cookie.ignore.pkl"
TABLE_NAME = "pilgrim_bi_flipkart_seller.earn_more_report"

sp = SellerPortal()

print("Logging In !! ")

if os.path.exists(LOGIN_PKL):
    with open(LOGIN_PKL, "rb") as f:
        sp.session.headers.update({"Cookie":pickle.load(f)})
else:    
    sp.login(username, password)
    with open(LOGIN_PKL, "wb") as f:
        pickle.dump(sp.session.headers["Cookie"], f)


start = date(year=2024, month=11, day=1)
end = date(year=2024, month=11, day=30)


HARD_STOP = date(2021, 1, 1)

range_pair = []

def has_report(start, end)->bool:
    resp = sp.check_report(start, end)
    print(f"Report Generated ! Moving to download. {resp}" if len(resp.downloadLink)>0 else f"Waiting for report: {resp}")
    return len(resp.downloadLink)>0


while(start>=HARD_STOP):
    range_pair.append((start, end))
    end = start
    start = start-timedelta(days=30)

last_saved = None

if not os.path.exists(DATE_STATUS_TRACKER_PKL):
    with open(DATE_STATUS_TRACKER_PKL, "wb") as f:
        last_saved = date.today()+timedelta(days=1)
        pickle.dump(last_saved, f)

with open(DATE_STATUS_TRACKER_PKL, "rb") as f:
        last_saved = pickle.load(f)


print(f"Using Last Saved: {last_saved}")

for pair in tqdm(range_pair):
    if(pair[0]>=last_saved): 
        print(f"Skipping {pair}")
        continue
    print(f"Attempting for download {pair}")
    genResp = sp.generate_report(*pair)
    while(not has_report(*pair)): time.sleep(10*60) # 10 min wait per check
    check_resp = sp.check_report(*pair)
    df = sp.download_report(check_resp.request_id)

    df2 = df.rename(COL_NAME_REMAP, axis=1)
    df2["runid"] = int(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

    client = get_bq_client(credentials_info)
    table = client.get_table(TABLE_NAME)
    client.insert_rows_from_dataframe(TABLE_NAME, df2, selected_fields=table.schema)
    last_saved = pair[0]
    with open(DATE_STATUS_TRACKER_PKL, "wb") as f:
        pickle.dump(last_saved, f)
    
for pair in tqdm(range_pair):
    if(pair[0]>=last_saved): 
        print(f"Skipping {pair}")
        continue
    print(f"downloading {pair}")