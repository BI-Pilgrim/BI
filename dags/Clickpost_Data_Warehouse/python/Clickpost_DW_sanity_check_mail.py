import smtplib
import sys
import pandas as pd 
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq as pgbq
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from airflow.models import Variable
from datetime import datetime, timedelta
import os 
import base64 
import json

def get_bq_client(credentials_info:str)->bigquery.Client:
    credentials_info = base64.b64decode(credentials_info).decode("utf-8")
    credentials_info = json.loads(credentials_info)

    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = bigquery.Client(credentials=credentials, project="shopify-pubsub-project")
    return client

def read_from_gbq(bq_client,p_id,t_id):
    df = bq_client.query(f"SELECT * FROM `{p_id}.{t_id}`").to_dataframe() 
    return df

def send_sanity_check_email():
    #query = """
    #   SELECT * FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.sanity`
    #"""  
    project_id = 'shopify-pubsub-project'
    sanity = 'Data_Warehouse_ClickPost_Staging.sanity' 
    credentials_info = Variable.get("GOOGLE_BIGQUERY_CREDENTIALS") 
    bq_client = get_bq_client(credentials_info) 
    df = read_from_gbq(bq_client,project_id, sanity)

    #df = pgbq.read_gbq(query, project_id="shopify-pubsub-project")
    
    df['Source_max_date'] = pd.to_datetime(df['Source_max_date'], errors='coerce')
    df['Staging_max_date'] = pd.to_datetime(df['Staging_max_date'], errors='coerce')
    df['Date1'] = pd.to_datetime(df['Date1'], errors='coerce')  

    filtered_df = df[(df['Source_max_date'] != df['Staging_max_date']) |
                 (df['Source_pk_count'] != df['Staging_pk_count'])  | 
                (df['Date1']  != df['Source_max_date'])]
    
    SENDER_EMAIL = "cloud@discoverpilgrim.com"
    RECIPIENT_EMAILS = "bi@discoverpilgrim.com"
    EMAIL_PASSWORD =  Variable.get("EMAIL_PASSWORD")  
    subject = "ClickPost DW Discrepancy !!!"

   
    if not filtered_df.empty:                    
        body = "Hi Team,<br><br>Please find the mismatch details attached.<br><br>Warm Regards,"
        attachments = []

        if not filtered_df.empty:
            filtered_df.to_csv("source_staging_max_date_&_pk_count.csv", index=False)
            attachments.append("source_staging_max_date_&_pk_count.csv")
        send_email(SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAILS, subject, body, attachments if attachments else None)

        
    else:
        body = "Hi Team,<br><br>No discrepancies found in the Clickpost data warehouse.<br><br>Warm Regards,"
        send_email(SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAILS, subject, body)


def send_email(sender_email, sender_password, recipient_email, subject, body, attachment_paths=None):
    try:
        if not body.strip():
            raise ValueError("Email body cannot be empty")
    
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        # Attach multiple files if provided
        if attachment_paths:  # ✅ Checks if attachments exist
            for file_path in attachment_paths:  # ✅ Loops over the list
                with open(file_path, "rb") as file:
                    attachment = MIMEBase("application", "octet-stream")
                    attachment.set_payload(file.read())
                    encoders.encode_base64(attachment)
                    attachment.add_header("Content-Disposition", f"attachment; filename={os.path.basename(file_path)}")
                    msg.attach(attachment)

        
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls() 
        print('mail is being sent')
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
        return True

    except Exception as e:
        print(f"Error sending email: {e}")
        return False

if __name__ == "__main__":
    send_sanity_check_email()
