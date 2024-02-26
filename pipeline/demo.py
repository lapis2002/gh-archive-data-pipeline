import requests
import gzip
import os
import shutil
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

@dag(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *", catchup=False, tags=['load'])
def run_demo():
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow")

    # Set dependencies between tasks
    hello >> airflow()

run_demo()