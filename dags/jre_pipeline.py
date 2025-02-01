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

# Define the timezone
local_tz = timezone("Asia/Kolkata")

DATES = {}  # Initialize DATES as a dictionary

"""
Function to get the name of csv file for saving the extracted data. 
"""
def get_yesterday_and_today():
    global DATES  # Ensure the function updates the global DATES dictionary
    now = datetime.now(local_tz)
    today = now.strftime('%Y-%m-%d')
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    DATES['yesterday'] = yesterday
    DATES['today'] = today
    return yesterday, today

# Populate DATES immediately when the script is loaded
get_yesterday_and_today()

"""
Function to get the name of csv file for saving the extracted data. 
"""
def get_csv_file_location():
    now = datetime.now(local_tz)
    today = now.strftime('%Y-%m-%d')
    input_source = f'/opt/airflow/extracted_data/youtube_data/jre_data_{today}.csv'
    return input_source

"""
Function to get more details about the video based on video_id.
This function is called by fetch_channel_videos().
"""
def fetch_video_details(youtube, video_ids):
    try:
        video_details = []
        video_request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=','.join(video_ids)
        )
        video_response = video_request.execute()
        
        for item in video_response['items']:
            details = {
                "Title": item['snippet']['title'],
                "Publish Date": item['snippet']['publishedAt'],
                "Duration": isodate.parse_duration(item['contentDetails']['duration']).total_seconds(),
                "Tags": item['snippet'].get('tags', []),
                "Category": item['snippet']['categoryId'],
                "View Counts": int(item['statistics'].get('viewCount', 0)),
                "Like Counts": int(item['statistics'].get('likeCount', 0))
            }
            video_details.append(details)
        
        return video_details
    except Exception as e:
        raise AirflowFailException(f"\n \n \nCritical error in fetch_video_details: {e} \n \n \n")

"""
Function to fetch videos from youtube API.
This function is called by extract_jre_data()
"""
def fetch_channel_videos(api_key, channel_id):
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)

        videos = []
        next_page_token = None

        # Get the uploads playlist ID
        channel_request = youtube.channels().list(
            part='contentDetails',
            id=channel_id
        )
        channel_response = channel_request.execute()
        uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        # Fetch videos from the uploads playlist
        while True:
            playlist_request = youtube.playlistItems().list(
                part='snippet',
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            playlist_response = playlist_request.execute()
            
            # Collect video IDs for further details
            video_ids = [item['snippet']['resourceId']['videoId'] for item in playlist_response['items']]
            videos.extend(fetch_video_details(youtube, video_ids))
            
            # Check if there are more pages
            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                break

        return videos
    except Exception as e:
        raise AirflowFailException(f"\n \n \nCritical error in fetch_channel_videos: {e} \n \n \n")
    
"""
Function to save extracted data as csv with the date of extraction. It overwrites existing data. This is intentional.
This function is called by extract_jre_data()
"""
def save_to_csv(videos):
    try:
        if not videos:
            raise AirflowFailException("\n \n \nNo videos available to save. Skipping save_to_csv. \n \n \n")
        keys = videos[0].keys()

        # Ensure the directory exists
        directory = "/opt/airflow/extracted_data/youtube_data/"     
        os.makedirs(directory, exist_ok=True)

        CSV_FILE = get_csv_file_location()
        # Always overwrite the file for the day
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=keys)
            writer.writeheader()
            writer.writerows(videos)
        print(f"Data saved successfully to {CSV_FILE}")
    except Exception as e:
        raise AirflowFailException(f"\n \n \nCritical error in save_to_csv: {e} \n \n \n")
    
"""
Function to extract data from YouTube API. It only runs if time is between 11:00 - 11:05 PM. 
Manual triggering will not result to any extraction. This is intentional. 
"""
def extract_jre_data():
    # Define timezone
    local_tz = timezone("Asia/Kolkata")
    now = datetime.now(local_tz)

    # Get the current hour and minute
    current_hour = now.hour
    current_minute = now.minute

    # Only allow execution between 11:00 PM and 11:05 PM
    if (current_hour == 23 and 0 <= current_minute <= 5):
        print(f" \n \n \n Skipping execution: Current time is outside the 11:00 PM - 11:05 PM window. Current Time: {current_hour}:{current_minute} \n \n \n")
        # Skip execution if outside the allowed window
        raise AirflowSkipException("\n \n \n Current time is outside the allowed execution window.Skipping the entire chain. \n \n \n")
    
    try:
        youtube_api_key = os.getenv("youtube_api_key")
        joe_rogan_experience_channel_id =os.getenv("joe_rogan_experience_channel_id")
        videos = fetch_channel_videos(youtube_api_key, joe_rogan_experience_channel_id)

        if not videos:
            # If no videos are returned, raise a failure
            print(f" \n \n \n No videos were fetched for the channel ID: {joe_rogan_experience_channel_id}. \n \n \n")
            raise AirflowFailException("No videos fetched. Check API or channel settings.")

        save_to_csv(videos)
        print(f" \n \n \n Saved {len(videos)} videos to CSV. \n \n \n")
    except Exception as e:
        # Log the error and fail the task
        print(f" \n \n \n An error occurred while extracting JRE data: {e} \n \n \n")
        raise AirflowFailException(f"Extraction failed due to: {str(e)}")

"""
Function to remove unwanted columns and rename them.
"""
def clean_and_upload_to_postgres_staging_sink():
    try:
        # Get the input source and load the data
        CSV_FILE = get_csv_file_location()
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
        raise AirflowFailException(f"\n \n \nCritical error in clean_and_upload_to_postgres_staging_sink: {e} \n \n \n")    


def upload_to_s3():
    try:
        # AWS Credentials
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key")

        # Fix tuple issue by converting to string if needed
        #For some reason access key is a tuple.
        if isinstance(aws_access_key_id, tuple):
            aws_access_key_id = aws_access_key_id[0]
        if isinstance(aws_secret_access_key, tuple):
            aws_secret_access_key = aws_secret_access_key[0]

        print(aws_access_key_id, aws_secret_access_key)
        # S3 Bucket and File Details
        bucket_name = "youtube-extracted-jre-data"
        local_file = f'/opt/airflow/extracted_data/youtube_data/jre_data_{DATES["today"]}.csv'
        s3_file = f'youtube_data/jre_data_{DATES["today"]}.csv'

        print(f"AWS Access Key ID: {aws_access_key_id} (type: {type(aws_access_key_id)})")
        print(f"AWS Secret Access Key: {aws_secret_access_key} (type: {type(aws_secret_access_key)})")

        print(f"Local file: {local_file} (type: {type(local_file)})")
        print(f"Bucket name: {bucket_name} (type: {type(bucket_name)})")
        print(f"S3 file path: {s3_file} (type: {type(s3_file)})")

        # Initialize S3 Client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        # Upload File to S3
        s3_client.upload_file(local_file, bucket_name, s3_file)
        print(f"Successfully uploaded {local_file} to {bucket_name}/{s3_file}")

    except FileNotFoundError:
        print(f"Error: The file {local_file} was not found.")
        raise
    except NoCredentialsError:
        print("Error: AWS credentials not provided.")
        raise
    except PartialCredentialsError as e:
        print(f"Error: Incomplete AWS credentials provided: {e}")
        raise
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        raise

"""
Function to find the most recent extraction_date in jre_insights_table before today.
If no such date exists, it defaults to today - 1 day.
"""  
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


def staging_to_insights_transformation():
    db_url = os.getenv("postgres_db_url")
    engine = create_engine(db_url)

    now = datetime.now(local_tz)
    today = now.strftime('%Y-%m-%d')
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
        raise AirflowFailException(f"Transformation failed: {e}")


# Default arguments for Dag
default_args = {
    'owner': 'Ballal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

#Simple dag running every night at 11 pm. 
dag = DAG(
    'JRE_pipeline',
    default_args=default_args,
    description='Fetch and save Joe Rogan Experience YouTube channel data daily at 11 PM',
    schedule_interval='0 23 * * *',  # Run daily at 11:00 PM
    start_date=datetime(2025, 1, 6, tzinfo=local_tz),
    catchup=False,
)

# First task to extract data
# extracted_jre_data 
extract_task = PythonOperator(
    task_id='extract_jre_data_from_youtube_api',
    python_callable=extract_jre_data,
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_and_upload_to_postgres_staging_sink',
    python_callable=clean_and_upload_to_postgres_staging_sink
)

"""
upload_jre_data_to_s3 = LocalFilesystemToS3Operator(
    task_id='upload_jre_data_to_s3',
    filename=f'/opt/airflow/extracted_data/youtube_data/jre_data_{DATES["today"]}.csv',
    dest_key=f'extracted_data/jre_data_{DATES["today"]}.csv',
    dest_bucket='youtube-extracted-jre-data',
    #aws_conn_id='aws_default',
    aws_conn_id=None,
    #aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),  # Fetch from environment
    #aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),  # Fetch from environment
    replace=True,
    dag=dag
)
"""

upload_jre_data_to_s3 = PythonOperator(
    task_id='upload_jre_data_to_s3',
    python_callable=upload_to_s3,
    dag=dag
)

# Task to test the connection
transform_data_from_staging_to_insights_table = PythonOperator(
    task_id="transform_Postgres_data_from_staging_to_insights_table",
    python_callable=staging_to_insights_transformation
)

# Set task dependencies
extract_task >> clean_task
extract_task >> upload_jre_data_to_s3
clean_task >> transform_data_from_staging_to_insights_table