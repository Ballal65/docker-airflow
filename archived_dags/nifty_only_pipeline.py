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

# Define the extraction function
def extract_nifty_data(** kwargs):

    url = f'https://www.google.com/finance/quote/NIFTY_50:INDEXNSE'

    try:
        now = datetime.now(local_tz)
        print(f"Current time: {now}")

        # Check holidays
        if not now.strftime('%Y-%m-%d') in market_holidays:
            print("Today is a market holiday. Skipping task.")
            return

        # Check trading hours
        if not (time(9, 15) <= now.time() <= time(15, 30)):
            print("Outside trading hours. Skipping task.")
            #return        

        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            value_element = soup.find('div', class_='YMlKec fxKbKc')
            datetime_element = soup.find('div', class_='ygUjEc')

            if value_element and datetime_element:
                val = float(value_element.text.replace(',', ''))
                datetime_string = datetime_element.text.split(' · ')[0].split(' GMT')[0]
                current_year = datetime.now().year
                dt = datetime.strptime(f"{datetime_string} {current_year}", "%b %d, %I:%M:%S %p %Y")
                sql_datetime = dt.strftime("%Y-%m-%d %H:%M:%S")

                # Get today's date in SQL format
                today = datetime.now().strftime("%Y-%m-%d")
                file_name = f"/opt/airflow/extracted_data/nifty_data/nifty_data_{today}.csv"

                # Check if the extracted data is stale (i.e., from yesterday)
                if dt.date() < datetime.now().date():
                    print("Stale data detected. Skipping append.")
                    return

                # Save data to CSV
                file_exists = os.path.isfile(file_name)
                with open(file_name, 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    if not file_exists:
                        writer.writerow(['datetime', 'value'])  # Write header if file doesn't exist
                    writer.writerow([sql_datetime, val])
                    print(f"Data saved to {file_name}")

            # Push extracted values to Airflow's XCom
                kwargs['ti'].xcom_push(key='nifty_datetime', value=sql_datetime)
                kwargs['ti'].xcom_push(key='nifty_value', value=val)
            else:
                print("Value or datetime not found. The structure of the page may have changed.")

        else:
            print(f"Failed with status code {response.status_code}")

    except Exception as e:
        print(f"Error during extraction: {e}")


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


def generate_snowflake_sql(**kwargs):
    ti = kwargs['ti']
    nifty_datetime = ti.xcom_pull(task_ids="extract_nifty_data", key="nifty_datetime")
    nifty_value = ti.xcom_pull(task_ids="extract_nifty_data", key="nifty_value")

    if not nifty_datetime or not nifty_value:
        raise ValueError("XCom values missing!")

    # Return formatted SQL statement
    return f"""
        INSERT INTO NIFTY.PUBLIC.NIFTY_STAGING (datetime, nifty_value)
        VALUES ('{nifty_datetime}', {nifty_value});
    """

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
    'nifty_only_pipeline',
    default_args=default_args,
    description='A simple DAG to extract Nifty 50 data and save it to a CSV file',
    schedule_interval='*/1 9-15 * * 1-5',
    start_date=datetime(2025, 1, 6, tzinfo=local_tz),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_nifty_data',
        python_callable=extract_nifty_data,
    )

    # Generate SQL Query Task (Passes SQL as XCom)
    generate_sql_task = PythonOperator(
        task_id='generate_snowflake_sql',
        python_callable=generate_snowflake_sql,
        provide_context=True
    )

    # Insert Data Task (Pulls SQL Query from XCom)
    insert_task = SnowflakeOperator(
        task_id='insert_nifty_record_to_snowflake',
        snowflake_conn_id='snowflake_conn',
        sql="{{ ti.xcom_pull(task_ids='generate_snowflake_sql') }}"
    )

    upload_task = PythonOperator(
        task_id='upload_nifty_data_to_s3',
        python_callable=upload_to_s3,
    )


    extract_task >> generate_sql_task >> insert_task >> upload_task  # Ensure extraction runs before upload


# Set task dependencies
extract_task