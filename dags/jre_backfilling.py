from datetime import datetime, timedelta
from dotenv import load_dotenv
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from googleapiclient.discovery import build
import csv
import isodate  # To parse ISO 8601 duration format
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.exceptions import AirflowSkipException, AirflowFailException 
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

load_dotenv()

# Define timezone and default args
local_tz = timezone("Asia/Kolkata")


default_args = {
    'owner': 'Ballal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Define DAG
dag = DAG(
    'JRE_pipeline_backfill_with_transformation',
    default_args=default_args,
    description='Backfill JRE pipeline data with cleaning and transformation',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
)

"""
Function to get a sorted list of all files in the given directory.
"""
def get_sorted_files(directory="/opt/airflow/extracted_data/youtube_data"):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    files.sort() 
    return files

def get_latest_yesterday(engine, db_url, today):
    query = """
    SELECT MAX(extraction_date) 
    FROM jre_insights_table 
    WHERE extraction_date < :today;
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query), {"today": today}).fetchone()
            yesterday = result[0] if result[0] else (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"\n \n \n Using yesterday = {yesterday} for transformation. \n \n \n")
        return yesterday
    except Exception as e:
        print(f"\n \n \n Error fetching latest yesterday: {e}. Defaulting to today - 1 day. \n \n \n")
        return (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")  # Fallback to today - 1 day
    
def clean_and_upload_to_postgres_staging_sink(CSV_FILE):
    try:
        source_data = pd.read_csv(CSV_FILE)

        # Create a copy to avoid SettingWithCopyWarning
        required_columns = source_data[["Title", "Publish Date", "Duration", "View Counts", "Like Counts"]].copy()
        required_columns.rename(columns={"Title": "title","Publish Date": "publish_date","Duration": "duration_in_seconds","View Counts": "view_count","Like Counts": "like_count"},inplace=True)
        
        extraction_date = CSV_FILE.split('_')[-1].split('.')[0]

        # Set a specific extraction date
        required_columns["extraction_date"] = extraction_date

        # PostgreSQL connection details
        db_url = os.getenv("postgres_db_url")

        # Create an SQLAlchemy engine
        engine = create_engine(db_url)

        # Delete existing rows for the same extraction_date
        try:
            with engine.connect() as connection:
                delete_query = text("DELETE FROM jre_staging_table WHERE extraction_date = :extraction_date")
                connection.execute(delete_query, {"extraction_date": extraction_date})
                print(f"Deleted existing rows for extraction_date: {extraction_date}")
        except Exception as e:
            print(f"\n \n \nError in deleting duplicate rows. : {e} \n \n \n")
        
        required_columns.to_sql(
            name='jre_staging_table',  # Table name
            con=engine,            # SQLAlchemy engine
            if_exists='append',    # Append mode (can be 'replace' or 'fail')
            index=False            # Do not write DataFrame index as a column
        )
    except Exception as e:
        raise AirflowFailException(f"Critical error in clean_and_upload_to_postgres_staging_sink: {e}")
    
def staging_to_insights_transformation(CSV_FILE):
    db_url = os.getenv("postgres_db_url")
    engine = create_engine(db_url)

    # Extract "today" from the CSV file name
    today = os.path.basename(CSV_FILE).split('_')[-1].split('.')[0]
    #Get yesterday from postgres
    yesterday = get_latest_yesterday(engine, db_url, today)


    query = """
    INSERT INTO jre_insights_table
    WITH yesterday AS (
        SELECT title, guest_name, publish_date, duration_in_seconds, jre_episode, mma_episode, toon_episode, extraction_date, video_stats
        FROM jre_insights_table
        WHERE extraction_date = :yesterday
    ),
    -- For some reason staging table has 4 duplicate rows. Removing those with row_number before proceeding
    today AS (
        SELECT * FROM(
                SELECT  title, publish_date, duration_in_seconds, view_count, like_count, 
                extraction_date,
                ROW_NUMBER() OVER (PARTITION BY title, extraction_date ORDER BY publish_date) AS rn
            FROM jre_staging_table
            WHERE extraction_date = :today
        ) AS sub WHERE rn = 1
    )
    SELECT
    -- 	title TEXT,
    -- If the video doesn't exists in y, take title from t
        COALESCE(y.title, t.title) AS title,
    -- 	guest_name TEXT,
    -- If video exists yesterday then carry forward that else extract the guest name
        CASE 
            WHEN y.title IS NOT NULL THEN y.guest_name
            ELSE 
                CASE
                    WHEN (COALESCE(y.title, t.title) LIKE 'Joe Rogan Experience%') 
                        THEN TRIM(SPLIT_PART(COALESCE(y.title, t.title), '-', 2))
                    WHEN (COALESCE(y.title, t.title) LIKE 'JRE MMA Show%') 
                        THEN TRIM(SPLIT_PART(COALESCE(y.title, t.title), 'with', 2))
                    ELSE '-'	
                END
        END AS guest_name,
    -- 						publish_date DATE,
        COALESCE(y.publish_date::DATE, t.publish_date::DATE) AS publish_date,
    -- 						duration_in_seconds BIGINT,
        COALESCE(y.duration_in_seconds::BIGINT, t.duration_in_seconds::BIGINT) AS duration_in_seconds,
    -- 						jre_episode BOOLEAN,
        CASE WHEN y.title IS NOT NULL THEN y.jre_episode
        ELSE
            CASE 
                -- Most JRE episodes have this patten
                WHEN (t.title ILIKE 'Joe Rogan Experience #%') THEN true
                -- Typo in one video
                WHEN (t.title ILIKE 'Joe Rogan Experienced #%') THEN true
                -- #857 episode
                WHEN (t.title ILIKE '%#857%') THEN true
                ELSE false
            END 
        END AS jre_episode,
    -- 						mma_episode BOOLEAN,
        CASE 
            WHEN y.title IS NOT NULL THEN y.mma_episode
            ELSE
                CASE
                    -- Most MMA Show episodes state with JRE MMA Show
                    WHEN (t.title LIKE 'JRE MMA Show%') 
                        OR (t.title LIKE 'Joe Rogan Experience - Fight Companion%') 
                        OR (t.title LIKE 'JRE Fight Companion%')
                        OR (t.title LIKE '%Fight Companion%')THEN true
                    ELSE false
                END
            END AS mma_episode,
    -- 						toon_episode BOOLEAN,
        CASE WHEN y.title IS NOT NULL THEN y.toon_episode
            ELSE
            CASE 
                WHEN (t.title LIKE '%JRE Toon%') THEN true
                ELSE false
            END
        END AS toon_episode,
    -- 						extraction_date DATE,
        t.extraction_date::DATE AS extraction_date,
    -- 						video_stats video_stats[]
        CASE 
            -- WHEN yesterday is NULL
            WHEN y.video_stats IS NULL THEN ARRAY[ROW(t.extraction_date::DATE, t.view_count::BIGINT, t.like_count::BIGINT)::video_stats]
            ELSE y.video_stats || ARRAY[ROW(t.extraction_date::DATE, t.view_count::BIGINT, t.like_count::BIGINT)::video_stats]
        END AS video_stats
    FROM today t FULL OUTER JOIN yesterday y ON y.title = t.title
    ORDER BY t.publish_date ASC;
    """

    try:
        with engine.connect() as connection:
            # Use **named parameters** with the query
            connection.execute(
                text(query),
                {"yesterday": yesterday, "today": today}  # Parameter binding
            )
        print(f"\n \n \n Transformation query executed for yesterday ({yesterday}) and today ({today}). \n \n \n")
    except Exception as e:
        raise AirflowFailException(f"Critical error in clean_and_upload_to_postgres_staging_sink: {e}")  

# Dynamic Task Creation
sorted_files = get_sorted_files()
previous_task = None

for file_path in sorted_files:
    clean_task = PythonOperator(
        task_id=f"clean_{os.path.basename(file_path).replace('.', '_')}",
        python_callable=clean_and_upload_to_postgres_staging_sink,  
        op_args=[file_path], 
    )

    transform_task = PythonOperator(
        task_id=f"transform_{os.path.basename(file_path).replace('.', '_')}",
        python_callable=staging_to_insights_transformation,
        op_args=[file_path], 
        dag=dag,
    )

    # Set dependencies: clean -> transform -> next file
    if previous_task:
        previous_task >> clean_task
    clean_task >> transform_task
    previous_task = transform_task