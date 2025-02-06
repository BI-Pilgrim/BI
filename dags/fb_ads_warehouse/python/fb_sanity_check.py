import pandas as pd
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from datetime import datetime, timedelta
import json
import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from airflow import DAG, XComArg
from airflow.models import Variable
import smtplib
import pandas as pd
from google.cloud import bigquery
from email.mime.base import MIMEBase
import pandas_gbq as pgbq




def main():

    # # Define default arguments for the DAG
    # default_args = {
    #     'owner': 'omkar.sadawarte@discoverpilgrim.com',
    #     'depends_on_past': False,
    #     'email_on_failure': True,
    #     'email_on_retry': True,
    #     'retries': 1,
    #     'retry_delay': timedelta(minutes=5),
    #     'email': ['omkar.sadawarte@discoverpilgrim.com'],
    # }

    # # Define the DAG
    # with DAG(
    #     dag_id='fb_mailer_dag',
    #     default_args=default_args,
    #     description='Run Sanity Check and Send Mail',
    #     schedule_interval='0 9 * * *',  # 9:00 AM IST
    #     start_date=datetime(2024, 1, 19),
    #     catchup=False,
    #     max_active_runs=1,
    # ) as dag:

    #     # read from Sanity_check
    #     read_sanity_check_sql_path = "D:/Github/BI/dags/fb_ads_warehouse/sql/datawarehouse_sanity_check"
    #     with open(read_sanity_check_sql_path, 'r') as file:
    #         sql_query_1 = file.read()

    #     read_sanity_check = BigQueryInsertJobOperator(
    #         task_id='read_sanity_check',
    #         configuration={
    #             "query": {
    #                 "query": sql_query_92,
    #                 "useLegacySql": False,
    #             },
    #             "location": LOCATION,
    #         }
    #     )

    # df = read_sanity_check.to_dataframe()
    # df = df.fillna({col: 0 for col in df.select_dtypes(include=['number']).columns})
    # df[['Source_max_date', 'Staging_max_date', 'Latest_Valid_Date', 'Date1']] = df[
    #     ['Source_max_date', 'Staging_max_date', 'Latest_Valid_Date', 'Date1']
    # ].apply(pd.to_datetime)
    # df['Date1_minus_3'] = df['Date1'] - timedelta(days=3)
    
    # diff_df = df[(df['Source_max_date'] != df['Staging_max_date']) & (df['Staging_max_date'] != df['Latest_Valid_Date']) |
    #              (df['Source_max_date'] < df['Date1_minus_3']) |
    #              (df['Source_pk_count'] != df['Staging_pk_count'])]



    # credentials = Variable.get("google_service_acount_variables")

    # # Initialize BigQuery client with explicit credentials
    # bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials


    # # Step 2: Perform Sanity Check
    # query = """
    # SELECT * FROM `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.Sanity_check`
    # """
    # df = bq_client.query(query).to_dataframe()
    # df = df.fillna({col: 0 for col in df.select_dtypes(include=['number']).columns})
    # df[['Source_max_date', 'Staging_max_date', 'Latest_Valid_Date', 'Date1']] = df[
    #     ['Source_max_date', 'Staging_max_date', 'Latest_Valid_Date', 'Date1']
    # ].apply(pd.to_datetime)
    # df['Date1_minus_3'] = df['Date1'] - timedelta(days=3)
    
    # diff_df = df[(df['Source_max_date'] != df['Staging_max_date']) & (df['Staging_max_date'] != df['Latest_Valid_Date']) |
    #              (df['Source_max_date'] < df['Date1_minus_3']) |
    #              (df['Source_pk_count'] != df['Staging_pk_count'])]
    

    query = """
        select * from shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.Sanity_check
        """
    
    df =  pgbq.read_gbq(query,"shopify-pubsub-project")
    df

    
    # # Step 3: Send Email with Results
    # email = Variable.get('Mailer_Credentials_email')
    # password = Variable.get('Mailer_Credentials_pass')
    # if not email or not password:
    #     raise ValueError("Missing email credentials")

    # email_ids = ["omkar.sadawarte@discoverpilgrim.com"]
    # # email_ids = ["omkar.sadawarte@discoverpilgrim.com",
    # #             "shafiq@discoverpilgrim.com",
    # #             "rwitapa.mitra@discoverpilgrim.com"]
    # subject = "Data Discrepancy in Tables"
    # body = "Below are the details of tables with data discrepancies:<br><br>"
    # # email_body = body + (diff_df.to_html(index=False) if not diff_df.empty else "<p>No discrepancies found.</p>")
    
    # msg = MIMEMultipart()
    # msg['From'] = email
    # msg['Subject'] = subject
    # msg.attach(MIMEText(body, 'html'))
    
    # try:
    #     server = smtplib.SMTP('smtp.gmail.com', 587)
    #     server.starttls()
    #     server.login(email, password)
    #     for recipient in email_ids:
    #         msg['To'] = recipient
    #         server.sendmail(email, recipient, msg.as_string())
    #         print(f"Email sent successfully to {recipient}")
    # except Exception as e:
    #     print(f"Failed to send email: {e}")
    # finally:
    #     server.quit()

# Execute the main function
if __name__ == "__main__":
    main()








    # # Step 1: Initialize Clients and Authenticate
    # client = secretmanager.SecretManagerServiceClient()
    # secret_name = "projects/shopify-pubsub-project/secrets/my-gcp-key/versions/4"
    # response = client.access_secret_version(name=secret_name)
    # service_account_info = json.loads(response.payload.data.decode("UTF-8"))
    # credentials = service_account.Credentials.from_service_account_info(service_account_info)
    # bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    # # Step 2: Perform Sanity Check
    # query = """
    # SELECT * FROM `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.Sanity_check`
    # """
    # df = bq_client.query(query).to_dataframe()
    # df = df.fillna({col: 0 for col in df.select_dtypes(include=['number']).columns})
    # df[['Source_max_date', 'Staging_max_date', 'Latest_Valid_Date', 'Date1']] = df[
    #     ['Source_max_date', 'Staging_max_date', 'Latest_Valid_Date', 'Date1']
    # ].apply(pd.to_datetime)
    # df['Date1_minus_3'] = df['Date1'] - timedelta(days=3)
    
    # diff_df = df[(df['Source_max_date'] != df['Staging_max_date']) & (df['Staging_max_date'] != df['Latest_Valid_Date']) |
    #              (df['Source_max_date'] < df['Date1_minus_3']) |
    #              (df['Source_pk_count'] != df['Staging_pk_count'])]
    
    # # Step 3: Send Email with Results
    # email = service_account_info.get('email')
    # password = service_account_info.get('password')
    # if not email or not password:
    #     raise ValueError("Missing email credentials in the secret payload")

    # email_ids = ["omkar.sadawarte@discoverpilgrim.com"]
    # # email_ids = ["omkar.sadawarte@discoverpilgrim.com",
    # #             "shafiq@discoverpilgrim.com",
    # #             "rwitapa.mitra@discoverpilgrim.com"]
    # subject = "Data Discrepancy in Tables"
    # body = "Below are the details of tables with data discrepancies:<br><br>"
    # email_body = body + (diff_df.to_html(index=False) if not diff_df.empty else "<p>No discrepancies found.</p>")
    
    # msg = MIMEMultipart()
    # msg['From'] = email
    # msg['Subject'] = subject
    # msg.attach(MIMEText(email_body, 'html'))
    
    # try:
    #     server = smtplib.SMTP('smtp.gmail.com', 587)
    #     server.starttls()
    #     server.login(email, password)
    #     for recipient in email_ids:
    #         msg['To'] = recipient
    #         server.sendmail(email, recipient, msg.as_string())
    #         print(f"Email sent successfully to {recipient}")
    # except Exception as e:
    #     print(f"Failed to send email: {e}")
    # finally:
    #     server.quit()
