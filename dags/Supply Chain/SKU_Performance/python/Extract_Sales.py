from __future__ import print_function
import io
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import TimeoutGuard
from pandas_gbq import to_gbq



SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
creds = None
# creds = Credentials.from_authorized_user_file('token.json', SCOPES)
# if not creds or not creds.valid:
#     if creds and creds.expired and creds.refresh_token:
#         creds.refresh(Request())
#     else:
#         flow = InstalledAppFlow.from_client_secrets_file('dags/Supply Chain/SKU_Performance/python/credentials.json', SCOPES)
#         creds = flow.run_local_server(port=0)
#     with open('token.json', 'w') as token:
#         token.write(creds.to_json())

if not creds or not creds.valid:
    flow = InstalledAppFlow.from_client_secrets_file('dags/Supply Chain/SKU_Performance/python/credentials.json', SCOPES)
    creds = flow.run_local_server(port=0)


service = build('drive', 'v3', credentials=creds)
folder_id = "1wrsGiYkVMyKPXt_evmbRhdvcEspzCyzI"
results = service.files().list(pageSize=1, fields="files(modifiedTime,name,id)", orderBy="modifiedTime desc", q="'" + folder_id + "' in parents and mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
items = results.get('files', [])

if items:
    file_id = items[0]['id']
    file_name = items[0]['name']
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print('Download %d%%.' % int(status.progress() * 100))

    buffer.seek(0)
    df = pd.read_excel(buffer,engine= 'openpyxl',header = 0)


month = items[0]['name'].split("_")[1].split(" ")[0]
year = items[0]['name'].split("_")[1].split(" ")[1].split(".")[0]


date = month + " 01, 20" + year 
df['date'] = pd.to_datetime(date, format='%B %d, %Y')
df[['Qty', 'Taxable Value']] = df[['Qty', 'Taxable Value']].replace(['', '-'], 0)
df["Taxable Value"] = df["Taxable Value"].round(2)



project_id = 'shopify-pubsub-project'
destination_table = 'Shopify_staging.Sales_Master'
to_gbq(df, destination_table, project_id=project_id, if_exists='append')  
print("Done appending in the table")
