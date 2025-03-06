import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta, time
import csv
import os
import pandas as pd
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
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
def extract_nifty_data(**kwargs):
    ti = kwargs['ti']
    url = 'https://www.google.com/finance/quote/NIFTY_50:INDEXNSE'
    now = datetime.now(local_tz)
    print(f"Current time: {now}")

    if now.strftime('%Y-%m-%d') in market_holidays:
        print("Today is a market holiday. Skipping task.")
        ti.xcom_push(key='nifty_datetime', value=None)
        ti.xcom_push(key='nifty_value', value=None)
        return

    if not (time(9, 15) <= now.time() <= time(15, 30)):
        print("Outside trading hours. Skipping task.")
        ti.xcom_push(key='nifty_datetime', value=None)
        ti.xcom_push(key='nifty_value', value=None)
        return

    try:
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

                if dt.date() < datetime.now().date():
                    print("Stale data detected. Skipping append.")
                    ti.xcom_push(key='nifty_datetime', value=None)
                    ti.xcom_push(key='nifty_value', value=None)
                    return

                ti.xcom_push(key='nifty_datetime', value=sql_datetime)
                ti.xcom_push(key='nifty_value', value=val)
                print(f"Extracted Data: {sql_datetime}, {val}")
            else:
                print("Value or datetime not found. Structure of the page may have changed.")
        else:
            print(f"Failed with status code {response.status_code}")
    except Exception as e:
        print(f"Error during extraction: {e}")


def generate_snowflake_sql(**kwargs):
    ti = kwargs['ti']
    nifty_datetime = ti.xcom_pull(task_ids='extract_nifty_data', key='nifty_datetime')
    nifty_value = ti.xcom_pull(task_ids='extract_nifty_data', key='nifty_value')

    if not nifty_datetime or not nifty_value:
        print("XCom values missing, retrying...")
        raise AirflowFailException("XCom values missing!")

    return f"""
        INSERT INTO NIFTY.PUBLIC.NIFTY_STAGING (datetime, nifty_value)
        VALUES ('{nifty_datetime}', {nifty_value});
    """

# Define default arguments
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
    'nifty_snowflake_pipeline',
    default_args=default_args,
    description='Extract Nifty 50 data and store in Snowflake',
    schedule_interval='*/1 9-15 * * 1-5',
    start_date=datetime(2025, 1, 6, tzinfo=local_tz),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_nifty_data',
        python_callable=extract_nifty_data,
        provide_context=True
    )

    generate_sql_task = PythonOperator(
        task_id='generate_snowflake_sql',
        python_callable=generate_snowflake_sql,
        provide_context=True
    )

    insert_task = SnowflakeOperator(
        task_id='insert_nifty_record_to_snowflake',
        snowflake_conn_id='snowflake_conn',
        sql="{{ ti.xcom_pull(task_ids='generate_snowflake_sql') }}"
    )

    extract_task >> generate_sql_task >> insert_task