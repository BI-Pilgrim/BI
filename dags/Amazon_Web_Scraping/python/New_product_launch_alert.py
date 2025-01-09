import smtplib
from google.cloud import bigquery
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import StringIO
from cryptography.fernet import Fernet
import pandas_gbq as pgbq

# Decrypt SMTP password
key = b"6xxliLxwuePV66P3kVVsxQunIPXrGpftn3wPHtQw-To="
encrypted_password = b"gAAAAABnOsoKL5aIs8AVIQBRfqX-Ba3x5mFGzVS1l8DR7fJQudmNK4qqRy3JBnFZWf7JQVF5iulcA_a-sLTOpEIlRj7rWTzh4A=="
cipher = Fernet(key)
smtp_password = cipher.decrypt(encrypted_password).decode("utf-8")

# Email Configuration
SENDER_EMAIL = "kajal.ray@discoverpilgrim.com"
RECIPIENT_EMAILS = [
    "konark.gaur@discoverpilgrim.com", "pooja.vadia@discoverpilgrim.com",
    "minu@discoverpilgrim.com", "swati.sachdeva@discoverpilgrim.com",
    "amrutha.renganathan@discoverpilgrim.com", "bi@discoverpilgrim.com"
]
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
GOOGLE_PROJECT_ID = "shopify-pubsub-project"

# Read SQL Query from File
def load_query(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Send Email with Attachment
def send_email_with_attachment(df, summary, current_month):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECIPIENT_EMAILS)
    msg['Subject'] = f"Competitor New Product Launch - {current_month}"

    html_table = summary.to_html(index=False, border=1)
    body = f"""
<html>
<body>
    <p>Hi Team,</p>
    <p>Here is the summary of the new product launches by target competitors:</p>
    {html_table}
    <br>Also, PFA the detailed report on the competitor new product launches.</p>
    <br><p>Regards,<br>Kajal Ray</p>
</body>
</html>
"""

    empty_body = """
<html>
<body>
    <p>Hi Team,</p>
    <p><b>There are no new product launches on Amazon this month.</b></p>
    <p>Regards,<br>Kajal Ray</p>
</body>
</html>
"""

    if summary.empty:
        msg.attach(MIMEText(empty_body, 'html'))
    else:
        msg.attach(MIMEText(body, 'html'))
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(csv_buffer.getvalue())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename=new_product_entries.csv")
        msg.attach(part)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, smtp_password)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())

# Main Function
if __name__ == "__main__":
    # Load SQL Queries
    main_query = load_query("new_product_launch.sql")
    date_query = "SELECT MAX(Date_MS) FROM `Amazon_Market_Sizing.AMZ_Current_month_MS`"

    # Query BigQuery
    df = pgbq.read_gbq(main_query, GOOGLE_PROJECT_ID)
    max_date = pgbq.read_gbq(date_query, GOOGLE_PROJECT_ID)
    current_month = pd.to_datetime(max_date['f0_'][0]).strftime("%B %Y")

    # Summarize Data
    summary = df.groupby('Brand_value')['ASIN'].nunique().reset_index(name='New Product Launch')

    # Send Email
    send_email_with_attachment(df, summary, current_month)
