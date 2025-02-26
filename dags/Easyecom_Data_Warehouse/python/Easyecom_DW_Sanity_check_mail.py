import smtplib
import sys
import pandas as pd
import pandas_gbq as pgbq
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from airflow.models import Variable
from datetime import datetime, timedelta
import os

def send_sanity_check_email():
    query = """
    SELECT 
        *
    FROM 
        `Data_Warehouse_Easyecom_Staging.Sanity_check`

        """
    df =  pgbq.read_gbq(query,"shopify-pubsub-project")
    df['Source_max_date'] = pd.to_datetime(df['Source_max_date'])
    df['Staging_max_date'] = pd.to_datetime(df['Staging_max_date'])
    df['Date1'] = pd.to_datetime(df['Date1'])

    filtered_df = df[(df['Source_max_date'] != df['Staging_max_date']) | 
                    (df['Date1'] - pd.to_timedelta('1 day') != df['Source_max_date']) | 
                    (df['Source_pk_count'] != df['Staging_pk_count'])]
    # return filtered_df

    # Email Configuration
    SENDER_EMAIL = "cloud@discoverpilgrim.com"
    RECIPIENT_EMAILS = "bi@discoverpilgrim.com"
    EMAIL_PASSWORD = Variable.get("EMAIL_PASSWORD")  # Secure password handling
    subject = "EasyEcom DW Discrepancy !!!"

    # Send email based on filtered data
    if not filtered_df.empty:
        filtered_df.to_csv("sanity_check_mismatch.csv", index=False)
        body = "Hi Team,<br><br>Please find the mismatch details attached.<br><br>Warm Regards,"
        send_email(SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAILS, subject, body, "sanity_check_mismatch.csv")
    else:
        # If filtered_df is empty, send a message indicating no issues
        body = "Hi Team,<br><br>No discrepancies found in the EasyEcom data warehouse.<br><br>Warm Regards,"
        send_email(SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAILS, subject, body)

def send_email(sender_email, sender_password, recipient_email, subject, body,attachment_path=None):
    try:
        if body is None or body.strip() == '':
            raise ValueError("Email body cannot be empty")
    
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        # Attach CSV file if exists
        if attachment_path:
            with open(attachment_path, "rb") as file:
                attachment = MIMEBase("application", "octet-stream")
                attachment.set_payload(file.read())
                encoders.encode_base64(attachment)
                attachment.add_header("Content-Disposition", f"attachment; filename={attachment_path}")
                msg.attach(attachment)

        # Set up the SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        print(sender_email)
        print(sender_password)
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
        return True

    except Exception as e:
        print(f"Error sending email: {e}")
        return False

if __name__ == "__main__":
    send_sanity_check_email()
