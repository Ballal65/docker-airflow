import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta, time
import csv
import os
import boto3
import pandas as pd
from pytz import timezone
from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException 
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Define the timezone
local_tz = timezone("Asia/Kolkata")


# Define holidays
market_holidays = [
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-25"
]

def upload_to_s3():
    try:
        # AWS Credentials
        aws_access_key_id = os.getenv("aws_access_key_id")
        aws_secret_access_key = os.getenv("aws_secret_access_key")

        bucket_name = "nifty50-data-files"
        today = datetime.now(local_tz).strftime("%Y-%m-%d")
        s3_file_path = f"Nifty/nifty_data_{today}.csv"
        local_file_path = f"/opt/airflow/extracted_data/nifty_data/nifty_data_{today}.csv"

        # Initialize S3 Client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        # Check if the file already exists in S3
        try:
            s3_obj = s3_client.get_object(Bucket=bucket_name, Key=s3_file_path)
            existing_data = pd.read_csv(s3_obj['Body'])
            print(f"Existing file {s3_file_path} found in S3. Appending data.")
        except s3_client.exceptions.NoSuchKey:
            print(f"File {s3_file_path} does not exist in S3. Creating new file.")
            existing_data = pd.DataFrame(columns=['datetime', 'value'])

        # Read new data from the local CSV
        new_data = pd.read_csv(local_file_path)

        # Append new data to existing data
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        
        # Convert to CSV format
        csv_buffer = StringIO()
        combined_data.to_csv(csv_buffer, index=False)
        
        # Upload back to S3
        s3_client.put_object(Bucket=bucket_name, Key=s3_file_path, Body=csv_buffer.getvalue())

        print(f"Successfully updated {s3_file_path} in S3.")

    except Exception as e:
        print(f"Error during upload: {e}")
        raise AirflowFailException(f"\n \n \nCritical error in clean_and_upload_to_postgres_staging_sink: {e} \n \n \n")    


# Define default arguments for the DAG
default_args = {
    'owner': 'Ballal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# Define the DAG
with DAG(
    'Upload_Nifty_Data_To_S3',
    default_args=default_args,
    description='A simple DAG to extract Nifty 50 data and save it to a CSV file',
    schedule_interval='20 15 * * 1-5',
    start_date=datetime(2025, 1, 6, tzinfo=local_tz),
    catchup=False,
) as dag:

    upload_task = PythonOperator(
        task_id='upload_nifty_data_to_s3',
        python_callable=upload_to_s3,
    )


    upload_task  # Ensure extraction runs before upload