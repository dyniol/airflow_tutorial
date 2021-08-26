from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag

from datetime import datetime, timedelta

@task(task_id='extract_partners', do_xcom_push=False, multiple_outputs=True)    
def extract():
    partner_name = 'netflix'
    partner_path = '/partners/netflix'
    return {'partner_name': partner_name, 'partner_path': partner_path}

@task()
def process(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@dag(
    dag_id='other_dag',
    description='DAG in charge of processing customer data',
    start_date=datetime(2021, 1, 1), schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['data_science', 'customers'],
    catchup=False,
    max_active_runs=1)

def my_dag():

    partner_settings = extract()
    process(partner_settings['partner_name'], partner_settings['partner_path'])

dag = my_dag()