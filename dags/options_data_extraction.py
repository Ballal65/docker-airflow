from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, time
import os

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

# Function to fetch and append data for multiple expiries
def fetch_and_append_data():
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

        current_time = datetime.now(local_tz).strftime('%Y-%m-%d %H:%M:%S')

        today = datetime.now(local_tz).strftime('%Y-%m-%d')
        csv_file = f"/opt/airflow/extracted_data/nifty_data/nifty_and_options_data_{today}.csv"
        file_exists = os.path.isfile(csv_file)

        for expiry in expiry_dates:
            # Modify URL dynamically
            expiry_formatted = expiry.replace('-', '-').replace(' ', '-')
            url = f'https://upstox.com/option-chain/nifty/?expiry={expiry_formatted}'
            response = requests.get(url, timeout=25)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Extract Nifty value
                nifty_div = soup.find('div', {'class': 'flex items-center gap-2'})
                nifty_value = nifty_div.find('div').get_text(strip=True) if nifty_div else "N/A"

                if nifty_value == "N/A":
                    print(f"Failed to extract Nifty value for expiry {expiry}. Skipping task.")
                    continue

                # Extract headers
                header_row = soup.find('thead').find_all('th')
                headers = [header.text.strip() for header in header_row if header.text.strip()]

                headers = headers[2:]
                headers_ce = [f"CE_{h}" for h in headers[:headers.index("Strike")]]
                headers_pe = [f"PE_{h}" for h in headers[headers.index("Strike")+1:]]
                final_headers = headers_ce + ["Strike"] + headers_pe + ["Nifty Value", "Timestamp", "Expiry"]

                # Extract rows
                rows = soup.find('tbody').find_all('tr')
                cleaned_rows = []
                for row in rows:
                    cells = row.find_all('td')
                    cell_data = [cell.text.strip() for cell in cells]

                    if len(cells) == 2 and cells[0].get('colspan') == '19':
                        continue

                    if cell_data[-1] == 'Login To Trade':
                        cell_data = cell_data[:-1]

                    if len(cell_data) < len(headers):
                        continue

                    ce_data = cell_data[:headers.index("Strike")]
                    strike_data = [cell_data[headers.index("Strike")]]
                    pe_data = cell_data[headers.index("Strike")+1:]
                    cleaned_rows.append(ce_data + strike_data + pe_data + [nifty_value, current_time, expiry])

                # Append data to the CSV file
                with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    if not file_exists:
                        writer.writerow(final_headers)
                        file_exists = True  # Avoid rewriting headers in subsequent loops
                    writer.writerows(cleaned_rows)

                print(f"Data for expiry {expiry} successfully appended to {csv_file}")
            else:
                print(f"Failed to fetch the page for expiry {expiry}. Status Code: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

# Define the DAG
default_args = {
    'owner': 'Ballal',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 6, tzinfo=local_tz),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG(
    'nifty_options_pipeline',
    default_args=default_args,
    description='Fetch and save Nifty option chain data',
    schedule_interval='*/5 9-16 * * 1-5', 
    catchup=False,
)

# Task
task = PythonOperator(
    task_id='fetch_and_append_data',
    python_callable=fetch_and_append_data,
    dag=dag,
)