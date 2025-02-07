import smtplib
import pandas as pd
import pandas_gbq as pgbq
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from airflow.models import Variable

def send_sanity_check_email():
    """Fetches sanity check data from BigQuery and sends an email if discrepancies are found."""
    
    try:
        # Query the data from BigQuery
        query = """
           SELECT * FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.Amazon_seller_Sanity_check`
        """
        df = pgbq.read_gbq(query, project_id="shopify-pubsub-project")

        # Handle NULL values to prevent errors
        df['Source_max_date'] = pd.to_datetime(df['Source_max_date'], errors='coerce')
        df['Dest_max_date'] = pd.to_datetime(df['Dest_max_date'], errors='coerce')

        # Ensure 'Latest_date' exists, otherwise create it
        if 'Latest_date' not in df.columns:
            print("'Latest_date' column is missing! Adding it.")
            df['Latest_date'] = pd.Timestamp.today()
        else:
            df['Latest_date'] = pd.to_datetime(df['Latest_date'], errors='coerce')

        # Fill missing values with today's date
        df = df.fillna(pd.Timestamp.today())

        # Apply filtering logic
        filtered_df = df[
            (df['Source_max_date'] != df['Dest_max_date']) & 
            (df['Latest_date'] - pd.to_timedelta('1 day') != df['Source_max_date']) & 
            (df['Source_pk_count'] != df['Dest_pk_count'])
        ]

        print("Filtered Data (First 5 rows):\n", filtered_df.head())

        # Email Configuration
        SENDER_EMAIL = "cloud@discoverpilgrim.com"
        RECIPIENT_EMAILS = "bi@discoverpilgrim.com"
        EMAIL_PASSWORD = Variable.get("EMAIL_PASSWORD")  # Secure password handling
        subject = "Amazon Seller DW Discrepancy !!!"

        # Send email based on filtered data
        if not filtered_df.empty:
            filtered_df.to_csv("sanity_check_mismatch.csv", index=False)
            body = "Hi Team,<br><br>Please find the mismatch details attached.<br><br>Warm Regards,"
            send_email(SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAILS, subject, body, "sanity_check_mismatch.csv")
        else:
            body = "Hi Team,<br><br>No discrepancies found in the Amazon Seller data warehouse.<br><br>Warm Regards."
            send_email(SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAILS, subject, body)

    except Exception as e:
        print(f"Error in send_sanity_check_email: {e}")

def send_email(sender_email, sender_password, recipient_email, subject, body, attachment_path=None):
    """Sends an email with or without an attachment."""
    try:
        if not body or body.strip() == '':
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
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
        print("Email sent successfully!")
        return True

    except Exception as e:
        print(f"Error sending email: {e}")
        return False

# ✅ Prevent automatic execution when the script is imported
if __name__ == "__main__":
    send_sanity_check_email()  # Now it runs **only when executed directly**
