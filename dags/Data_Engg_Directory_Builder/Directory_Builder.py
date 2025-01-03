import os
import subprocess
from google.cloud import bigquery
import pandas as pd

# Function to install a package
def install_package(package_name):
    try:
        subprocess.check_call(["pip", "install", package_name])
    except subprocess.CalledProcessError as e:
        print(f"Error installing package {package_name}: {e}")
        raise

# Ensure the required library is installed
required_package = "db-dtypes"
try:
    import db_dtypes  # Attempt to import the package to check if it exists
except ImportError:
    print(f"{required_package} not found. Installing...")
    install_package(required_package)

# Set path to the JSON key file for authentication
key_file_path = "JSON_KEY.json"  # Replace with the actual path to your JSON key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path

# 1. Create a directory named 'dags' inside a folder 'BI'
base_path = "BI"
dags_path = os.path.join(base_path, "dags")
os.makedirs(dags_path, exist_ok=True)

# 2. Create another folder inside 'dags' named 'fb_ads_warehouse'
fb_ads_path = os.path.join(dags_path, "fb_ads_warehouse")
os.makedirs(fb_ads_path, exist_ok=True)

# 3. Inside 'fb_ads_warehouse', create two folders 'dag' and 'sql'
dag_folder = os.path.join(fb_ads_path, "dag")
sql_folder = os.path.join(fb_ads_path, "sql")
os.makedirs(dag_folder, exist_ok=True)
os.makedirs(sql_folder, exist_ok=True)

# 4. Inside the 'sql' folder, create three folders: 'data_model_master', 'datawarehouse_sanity_check', and 'fb_ads_to_bq'
big_query_folder = "fb_ads_to_bq"    # Give the name of the folder that will contain the create and append sql codes
folders = ["data_model_master", "datawarehouse_sanity_check", big_query_folder]
for folder in folders:
    os.makedirs(os.path.join(sql_folder, folder), exist_ok=True)

# 4.1 Create specific files as per the requirements
# Create an empty "dummy_dag.py" inside the 'dag' folder
dummy_dag_path = os.path.join(dag_folder, "dummy_dag.py")
with open(dummy_dag_path, "w") as f:
    pass

# Create an empty "dummy_sanity_check.sql" inside 'datawarehouse_sanity_check'
sanity_check_path = os.path.join(sql_folder, "datawarehouse_sanity_check", "dummy_sanity_check.sql")
with open(sanity_check_path, "w") as f:
    pass

# Create two empty SQL files inside 'data_model_master': "dummy_master_append.sql" and "dummy_master_create.sql"
data_model_master_path = os.path.join(sql_folder, "data_model_master")
for file_name in ["dummy_master_append.sql", "dummy_master_create.sql"]:
    file_path = os.path.join(data_model_master_path, file_name)
    with open(file_path, "w") as f:
        pass

# 5. Use BigQuery to get the table names
project_id = "shopify-pubsub-project"  # Replace with your Google Cloud Project ID
dataset_id = "Data_Warehouse_Facebook_Ads_Staging"  # Replace with your dataset ID
big_query_folder_path = os.path.join(sql_folder, big_query_folder)

try:
    # Initialize BigQuery client
    client = bigquery.Client()

    # Query to list table names in the dataset
    query = f"""
    SELECT table_name 
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    """
    df = client.query(query).to_dataframe()

    # Get table names from the dataframe
    table_names = df["table_name"].tolist()

    # 6. Create folders for each table name and add specific SQL files
    for table_name in table_names:
        table_folder_path = os.path.join(big_query_folder_path, table_name)
        os.makedirs(table_folder_path, exist_ok=True)

        # Create specific SQL files inside each folder
        create_file_path = os.path.join(table_folder_path, f"{table_name}_create.sql")
        append_file_path = os.path.join(table_folder_path, f"{table_name}_append.sql")
        for file_path in [create_file_path, append_file_path]:
            with open(file_path, "w") as f:
                pass

    print("Folder structure and required files created successfully!")

except Exception as e:
    print(f"An error occurred: {e}")
