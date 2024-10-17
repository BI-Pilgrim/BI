import requests
import boto3
from airflow.models import Variable
import datetime
import logging
from datetime import date

# Setup logging
logging.basicConfig(filename='easyecom_log.txt', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


# Retrieve sensitive info from Airflow Variables
easy_ecom_email = Variable.get("easy_ecom_email")
easy_ecom_password = Variable.get("easy_ecom_password")
easy_ecom_location_key = Variable.get("easy_ecom_location_key")
aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")


# Function to handle the retrieval and storage of JWT token
def get_jwt_token():
    try:
        with open("jwt_token.txt", "r") as file:
            token = file.read().strip()
        if not token:
            logging.error("JWT token file is empty. Retrieving new token.")
            return retrieve_jwt_token()
        return token
    except FileNotFoundError:
        logging.error("JWT token file not found. Retrieving new token.")
        return retrieve_jwt_token()

def retrieve_jwt_token():
    url = "https://api.easyecom.io/access/token"
    headers = {"Content-Type": "application/json"}
    data = {
        "email": easy_ecom_email,
        "password": easy_ecom_password,
        "location_key": easy_ecom_location_key
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        token = response.json()['data']['token']['jwt_token']
        with open("jwt_token.txt", "w") as file:
            file.write(token)
        logging.info("New JWT token retrieved and stored.")
        return token
    else:
        logging.error(f"Failed to retrieve JWT token: {response.text}")
        return None
    

# Function to send data to Google Sheets
def send_to_google_sheets(invoice_id, order_id, invoice_number,reference_code, order_date, 
                          s3_link, order_type, order_type_key, marketplace, invoice_date,
                          awb_number, customer_name, company_name, date,inv_value,qty):
    url = 'https://script.google.com/macros/s/AKfycbw6DZLMue-vsCmhQV-5TLDjEMuTPuCZZ7w5OW9xo3TFk0Rowlln7FN_R5PwdW2D5S1tZw/exec'  # Replace with your Google Apps Script Web App URL
    data = {
        "invoice_id": "NA" if invoice_id==None else invoice_id,
        "order_id": "NA" if order_id==None else order_id ,
        "invoice_number": "NA" if invoice_number==None else invoice_number,
        "reference_code": "NA" if reference_code==None else reference_code,
        "order_date":"NA" if order_date==None else order_date,
        "s3_link": "NA" if s3_link==None else s3_link,
        "order_type": "NA" if order_type==None else order_type,
        "order_type_key" : "NA" if order_type_key==None else order_type_key,
        "marketplace": "NA" if marketplace==None else marketplace,
        "invoice_date":"NA" if invoice_date==None else invoice_date,
        "awb_number":"NA" if awb_number==None else awb_number,
        "customer_name": "NA" if customer_name==None else customer_name,
        "company_name": "NA" if company_name==None else company_name,
        "upload_date": "NA" if date==None else date,
        "total_amount": "NA" if inv_value==None else inv_value,
        "order_quantity": "NA" if qty==None else qty,
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)
    
    if response.ok:
        logging.info("Data sent to Google Sheets successfully.")
    else:
        logging.error(f"Failed to send data to Google Sheets: {response.text}")
    return response.json()

# Recursive function to get all orders from last day, handling pagination
def get_orders(token=None, url=None):
    if not url:
        today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        invoice_start_date = date.today()
        invoice_end_date = date.today()
        order_type = 1
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        url = f"https://api.easyecom.io/orders/V2/getAllOrders?limit=250&invoice_start_date={invoice_start_date}&invoice_end_date={invoice_end_date}&isalllocation=1"
    if not token:
        response = requests.get(url)
    else:
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(url, headers=headers)
    
    if response.status_code == 401:  # Token expired
        logging.info("JWT token expired. Refreshing token.")
        token = retrieve_jwt_token()
        return get_orders(token, url) if token else None
    elif response.status_code == 200:
        data = response.json()
        if 'nextUrl' in data['data'] and data['data']['nextUrl'] != None:
            next_url = 'https://api.easyecom.io'+ data['data']['nextUrl']
            next_response = get_orders(None,next_url)
            data['data']['orders'].extend(next_response['data']['orders'])
        return data
    else:
        logging.error(f"Failed to retrieve orders: HTTP {response.status_code} - {response.text}")
        return None

def upload_file_to_s3(file_content, bucket, object_name):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,  
        aws_secret_access_key=aws_secret_access_key
    )
    try:
        s3_client.put_object(Body=file_content, Bucket=bucket, Key=object_name)
        logging.info(f"File uploaded successfully: {object_name}")
        
        s3_url = f"https://{bucket}.s3.ap-south-1.amazonaws.com/{object_name}"
        return s3_url
    except Exception as e:
        logging.error(f"An error occurred during S3 upload: {e}")
        return None


# read from sheet from auto email code
# Main function to execute daily
# order['order_type']== 'B2B' :
#invoice_content = download_invoice(token, order['invoice_id'])
def main():
    token = get_jwt_token()
    counter = 1
    if token:
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        orders_data = get_orders(token)
        if orders_data and orders_data.get('data', {}).get('orders'):
            for order in orders_data['data']['orders']:
                counter += 1
                if order['invoice_id'] and order['order_type']== 'B2B' and order['documents']:
                    invoice_url = order['documents']['easyecom_invoice']
                    invoice_response = requests.get(invoice_url)
                    invoice_content = invoice_response.content
                    if invoice_content:
                        date_today = datetime.datetime.now().strftime("%Y-%m-%d")
                        file_name = f"{order['invoice_number']}.pdf"
                        s3_link=upload_file_to_s3(invoice_content, 'easyecombills', file_name)
                        print(s3_link)
                        push_date = datetime.datetime.now().strftime('%m/%d/%Y')
                        send_to_google_sheets(order['invoice_id'],order['order_id'],order['invoice_number'],order['reference_code'],order['order_date'],
                                              s3_link,order['order_type'],order['order_type_key'], order['marketplace'],order['invoice_date'],
                                              order['awb_number'],order['customer_name'],order['company_name'],date_today,order['total_amount'],order['order_quantity'])
                    else:
                        print('Invoice not present',order['invoice_id'],order['order_id'],order['Invoice_date'])
                else:
                    print("Either invoice_id is not present, or B2b order is not present, or Document is not present")
        else:
            logging.error("No orders retrieved or data structure was unexpected.")
    else:
        logging.error("Failed to retrieve or validate JWT token. Cannot proceed with retrieving orders.")
    print(counter)
    print("Done")
    return 0

if __name__ == "__main__":
    main()
