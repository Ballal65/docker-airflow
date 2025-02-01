from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Define default args for DAG
default_args = {
    'owner': 'Ballal',
    'start_date': datetime(2025, 1, 29),
    'catchup': False
}

# Create DAG
with DAG(
    'insert_nifty_data',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually
    catchup=False
) as dag:

    insert_data = SnowflakeOperator(
        task_id='insert_nifty_record',
        snowflake_conn_id='snowflake_conn',
        sql="""
            INSERT INTO NIFTY.NIFTY50 (datetime, value)
            VALUES (CURRENT_TIMESTAMP, 20000.00);
        """
    )

    insert_data
