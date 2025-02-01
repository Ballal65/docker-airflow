import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta, time
import csv
import os
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define the timezone
local_tz = timezone("Asia/Kolkata")

# Define expiry dates
expiry_dates = [
    "09-Jan-2025", "16-Jan-2025", "23-Jan-2025", 
    "30-Jan-2025", "06-Feb-2025"
]

# Define holidays
market_holidays = [
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-25"
]

# Define the extraction function
def extract_nifty_data():
    ticker = 'NIFTY_50'
    url = f'https://www.google.com/finance/quote/{ticker}:INDEXNSE'

    try:
        now = datetime.now(local_tz)
        print(f"Current time: {now}")

        # Check holidays
        if now.strftime('%Y-%m-%d') in market_holidays:
            print("Today is a market holiday. Skipping task.")
            return

        # Check trading hours
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

            else:
                print("Value or datetime not found. The structure of the page may have changed.")

        else:
            print(f"Failed with status code {response.status_code}")

    except Exception as e:
        print(f"Error during extraction: {e}")

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

# Set task dependencies
extract_task