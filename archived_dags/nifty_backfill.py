import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator

# PostgreSQL Connection
DB_URL = "postgresql+psycopg2://postgres:Parijat789@postgres:5432/postgres"
engine = create_engine(DB_URL)

# Folder containing CSV files
CSV_FOLDER_PATH = "/opt/airflow/extracted_data/nifty_data/"

# List of specific files to upload
FILES_TO_UPLOAD = [
    "nifty_data_2025-02-03.csv",
    "nifty_data_2025-02-04.csv",
    "nifty_data_2025-02-05.csv",
    "nifty_data_2025-02-06.csv",
    "nifty_data_2025-02-07.csv",
    "nifty_data_2025-02-10.csv",
    "nifty_data_2025-02-11.csv",
    "nifty_data_2025-02-12.csv",
    "nifty_data_2025-02-13.csv",
]

# Function to insert CSV data into PostgreSQL using SQL queries
def upload_selected_csvs():
    try:
        with engine.connect() as connection:
            for file in FILES_TO_UPLOAD:
                file_path = os.path.join(CSV_FOLDER_PATH, file)
                
                if not os.path.exists(file_path):
                    print(f"File not found: {file_path}")
                    continue

                print(f"Processing: {file_path}")

                # Read CSV into DataFrame
                df = pd.read_csv(file_path)

                # Ensure datetime is formatted correctly
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df.dropna(subset=["datetime", "value"], inplace=True)  # Drop invalid rows

                # Insert each row manually using SQL query
                for _, row in df.iterrows():
                    sql_datetime = row["datetime"].strftime("%Y-%m-%d %H:%M:%S")
                    sql_value = row["value"]

                    query = text(f"""
                        INSERT INTO NIFTY (datetime, value)
                        VALUES ('{sql_datetime}', {sql_value})
                    """)

                    connection.execute(query)

                print(f"Inserted {len(df)} rows from {file}")

    except Exception as e:
        print(f"Error processing CSV files: {e}")

# Define DAG
default_args = {
    "owner": "Ballal",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "upload_selected_nifty_csvs_sql",
    default_args=default_args,
    description="Upload selected NIFTY CSV files to PostgreSQL using raw SQL queries",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 6),
    catchup=False,
) as dag:

    upload_task = PythonOperator(
        task_id="upload_selected_csvs_sql",
        python_callable=upload_selected_csvs,
    )

upload_task
