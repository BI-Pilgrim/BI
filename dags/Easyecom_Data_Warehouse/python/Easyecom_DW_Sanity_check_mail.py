import smtplib
import pandas as pd
from google.cloud import bigquery
from email.mime.base import MIMEBase
import pandas_gbq as pgbq
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import config

query = """
   SELECT 
    *
FROM 
    `Data_Warehouse_Easyecom_Staging.Sanity_check`

    """
df =  pgbq.read_gbq(query,"shopify-pubsub-project")
df['Source_max_date'] = pd.to_datetime(df['Source_max_date'])
df['Dest_max_date'] = pd.to_datetime(df['Dest_max_date'])
df['Date1'] = pd.to_datetime(df['Date1'])
filtered_df = df[(df['Source_max_date'] != df['Dest_max_date']) & 
                 (df['Date1'] - pd.to_timedelta('1 day') != df['Source_max_date']) & 
                 (df['Source_pk_count'] != df['Dest_pk_count'])]

def send_email(sender_email, sender_password, recipient_email, subject, body):
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
#         print("Email Sent Successfully!!!")
        return True

    except Exception as e:
        print(f"Error sending email: {e}")
        return False

# Example usage (replace with your credentials and message)
SENDER_EMAIL = config.SENDER_EMAIL
RECIPIENT_EMAILS = config.RECIPIENT_EMAILS
EMAIL_PASSWORD = config.EMAIL_PASSWORD
body = f"Hi Team,<br><br>Please find mismatch in the respective tables of Easyecom datawarehouse <br><br>{df.to_html(index=False)}<br><br>Warm Regards,"
subject = f"Easyecom DW Discrepancy !!! "

if (not(filtered_df.empty)):
    send_email(SENDER_EMAIL,EMAIL_PASSWORD,RECIPIENT_EMAILS,subject,body)


