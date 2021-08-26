from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

def _extract(partner_name):                             
    print(partner_name)
    # to hide value of variable, we can use "_secret" to hide it's value on airflow ui
    # we get the variable by built-in model "Variable" and assign it to partner 
    # deserialize_json to read variable value from json format   

# to set variables:
# go to airflow ui, Admin -> Variables -> Add new record
# key: my_dag_partner
# val: partner_a
# desc: Variable for partners
with DAG(
    'variable_dag',
    description='DAG in charge of processing customer data',
    start_date=datetime(2021, 1, 1), schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['data_science', 'customers'],
    catchup=False, max_active_runs=1) as dag:
    
        extract = PythonOperator(
            task_id='extract',
            python_callable=_extract,
            op_args=["{{ var.json.my_dag_partner.name }}"]  # to get 'name' out of the variable, we can use only this line instead of line underneath
            # Variable.get('my_dag_partner', deserialize_json=True)['name']
        )
