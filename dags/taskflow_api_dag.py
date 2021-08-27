from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag

from datetime import datetime, timedelta

@task()
def extract():
    partner_name = 'netflix'
    partner_path = '/partners/netflix'
    return partner_name

@task()
def process(partner_name):
    print(partner_name)

@dag(description='DAG in charge of processing customer data',
    start_date=datetime(2021, 1, 1), schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10), tags=['data_science', 'customers'],
    catchup=False, max_active_runs=1)

def taskflow_api_dag():

    process(extract())

taskflow_api_dag()