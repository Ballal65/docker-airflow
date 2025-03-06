import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta, time
import csv
import os
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define timezone
local_tz = timezone("Asia/Kolkata")

# PostgreSQL Connection
DB_URL = "postgresql+psycopg2://postgres:Parijat789@postgres:5432/postgres"
engine = create_engine(DB_URL)

# Define market holidays
market_holidays = [
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-25"
]

# Task 1: Extract Data
def extract_nifty_data(**kwargs):
    url = f'https://www.google.com/finance/quote/NIFTY_50:INDEXNSE'
    
    try:
        now = datetime.now(local_tz)
        print(f"Current time: {now}")

        # Check if today is a holiday
        if now.strftime('%Y-%m-%d') in market_holidays:
            print("Today is a market holiday. Skipping task.")
            return
        
        # Check market hours
        if not (time(9, 15) <= now.time() <= time(15, 30)):
            print("Outside trading hours. Skipping task.")
            return        

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

                # Get today's date for file naming
                today = datetime.now().strftime("%Y-%m-%d")
                file_name = f"/opt/airflow/extracted_data/nifty_data_{today}.csv"

                # Avoid appending stale data
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
                print(f"\n \n \n First task values: {sql_datetime}, {val} \n \n \n")
                # Push values to XCom for the next task
                kwargs['ti'].xcom_push(key='nifty_datetime', value=sql_datetime)
                kwargs['ti'].xcom_push(key='nifty_value', value=val)
            else:
                print("Value or datetime not found. The structure of the page may have changed.")

        else:
            print(f"Failed with status code {response.status_code}")

    except Exception as e:
        print(f"Error during extraction: {e}")

# Task 2: Upload to PostgreSQL
def upload_to_postgres(**kwargs):
    ti = kwargs['ti']

    #print datetime and values
    sql_datetime = ti.xcom_pull(task_ids='extract_nifty_data', key='nifty_datetime')
    val = ti.xcom_pull(task_ids='extract_nifty_data', key='nifty_value')

    print(f"\n \n \n Second task values: {sql_datetime}, {val} \n \n \n")
    if not sql_datetime or not val:
        print("No data extracted. Skipping database insertion.")
        return

    try:
        query = f"""
    INSERT INTO NIFTY (datetime, value)
    VALUES ('{sql_datetime}', {val})  
    """
        with engine.connect() as connection:
            connection.execute(query)
        print("Data inserted into PostgreSQL")

        #df = pd.DataFrame([[sql_datetime, val]], columns=['datetime', 'value'])
        #df.to_sql('NIFTY', con=engine, if_exists='append', index=False)
    except Exception as e:
        print(f"Database insertion failed: {e}")

# Define DAG
default_args = {
    'owner': 'Ballal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'nifty_pipeline_postgres',
    default_args=default_args,
    description='Extract Nifty 50 data and upload to PostgreSQL',
    schedule_interval='*/1 9-15 * * 1-5',  # Run every minute during market hours
    start_date=datetime(2025, 1, 6, tzinfo=local_tz),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_nifty_data',
        python_callable=extract_nifty_data,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id='upload_to_postgres',
        python_callable=upload_to_postgres,
        provide_context=True,
    )

    # Define task dependencies
    extract_task >> upload_task
