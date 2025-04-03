from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner":"Ballal Pathare",
    "depends_on_past":False,
    "retry": 5,
    "retry_delay": timedelta(minutes=1),
    "email_on_retry": False,
    "email_on_failure":False
}

dag_params = {
    "default_args" : default_args, 
    "schedule_interval" :timedelta(days=1), 
    "start_date":datetime(2025,3,31), 
    "catchup":False, 
    "description" = "Simple dag to print hello"
}

@dag(**dag_params)
def hello_work_dag():
    @task
    def print_hello():
        print("Hello world")
    
    @task
    def print_day():
        print(datetime.now())

    hello = print_hello()
    day = print_day()
    hello >> day

dag = hello_work_dag()
