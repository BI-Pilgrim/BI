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
    # Query the data from BigQuery
    query = """
       SELECT * FROM `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.Sanity_check`
    """
    df = pgbq.read_gbq(query, project_id="shopify-pubsub-project")
    df = df.fillna({col: 0 for col in df.select_dtypes(include=['number']).columns})

    # Handle NULL values to prevent errors
    df['Source_max_date'] = pd.to_datetime(df['Source_max_date'], errors='coerce')
    df['Staging_max_date'] = pd.to_datetime(df['Staging_max_date'], errors='coerce')
    df['Date1'] = pd.to_datetime(df['Date1'], errors='coerce')  # Convert to datetime
    df['Latest_Valid_Date'] = pd.to_datetime(df['Latest_Valid_Date'], errors='coerce')  # Convert to datetime

    # Subtract 3 days
    Date1_minus_7 = df['Date1'] - timedelta(days=7)
    Date1_minus_3 = df['Date1'] - timedelta(days=3)

    # print(df[['Date1', 'Date1_minus_3']])


    # Apply filtering logic
    filtered_df = df[(df['Source_max_date'] != df['Staging_max_date']) |
                 (df['Source_max_date'] < Date1_minus_3) |
                 (df['Source_pk_count'] != df['Staging_pk_count'])]

    filtered_df1 = df[(df['Latest_Valid_Date'] < Date1_minus_7)]

    # return filtered_df1,filtered_df 

    SENDER_EMAIL = "cloud@discoverpilgrim.com"
    RECIPIENT_EMAILS = "bi@discoverpilgrim.com"
    EMAIL_PASSWORD = Variable.get("EMAIL_PASSWORD")  # Secure password handling
    subject = "Facebook DW Discrepancy !!!"

    # Send email based on filtered data  "source_staging_max_date_&_pk_count_currdate_minus3.csv"  "source_vs_latest_valid_date_minus7.csv"
    if not filtered_df.empty or not filtered_df1.empty:
        body = "Hi Team,<br><br>Please find the mismatch details attached.<br><br>Warm Regards,"
        attachments = []

        if not filtered_df.empty:
            filtered_df.to_csv("source_staging_max_date_&_pk_count_currdate_minus3.csv", index=False)
            attachments.append("source_staging_max_date_&_pk_count_currdate_minus3.csv")
        
        if not filtered_df1.empty:
            filtered_df1.to_csv("source_vs_latest_valid_date_minus7.csv", index=False)
            attachments.append("source_vs_latest_valid_date_minus7.csv")

        send_email(SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAILS, subject, body, attachments if attachments else None)

        
    else:
        # If filtered_df is empty, send a message indicating no issues
        body = "Hi Team,<br><br>No discrepancies found in the Facebook data warehouse.<br><br>Warm Regards,"
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

# ✅ Prevent automatic execution when the script is imported
if __name__ == "__main__":
    send_sanity_check_email()
