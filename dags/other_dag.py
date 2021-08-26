
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

# class CustomPostgresOperator(PostgresOperator):       # modifying built-in operators and adding additional values to existing classes

#     template_fields = ('sql', 'parameters')

def _extract(ti):   # ti = task instance object
    partner_name = "netflix"
    partner_path = '/partners/netflix'
    # ti.xcom_push(key="partner_name", value=partner_name)
    # can be written by simply
    return {"partner_name":partner_name, "partner_path":partner_path}   # way to push multiple arguments

def _process(ti):
    partner_settings = ti.xcom_pull(
        # key="partner_name",   no need of puting key here cause its returned by _extract func
        task_ids="extract")
    print(partner_settings['partner_name'])  # accesing the partner_name from task instance xcom pull

with DAG(
    dag_id= 'my_dag',
    # default_args=default_args,
    description= 'A simple tutorial DAG',
    start_date= datetime(2021, 1, 1),
    schedule_interval= "@daily",
    dagrun_timeout= timedelta(minutes=10),
    tags= ["data_science", "customers"],
    catchup= False, max_active_runs=1
        ) as dag:

    extract = PythonOperator(
        task_id= 'extract',
        python_callable= _extract, # no () after calling function !!!!!!!!!!!!!!
        # op_args= ['{{ var.json.my_dag_partner.name }}']
    )


    process = PythonOperator(
        task_id= 'process',
        python_callable= _process, # no () after calling function !!!!!!!!!!!!!!
        # op_args= ['{{ var.json.my_dag_partner.name }}']
    )

    # fetch_data = CustomPostgresOperator(
    #     task_id= "fetching_data",
    #     sql= "sql/MY_REQUEST.sql",    # ds means today's day
    #     parameters={
    #         'next_ds': ' {{ next_ds }}',
    #         'prev_ds': ' {{ prev_ds }}',
    #         'partner': '{{ var.json.my_dag_partner.name }}',
    #     }
    # )



#    BashOperator(
#         task_id="creating_folders",
#         bash_command="mkdir my_folder",
#         # env: Optional[Dict[str, str]] = None,
#         # output_encoding: str = 'utf-8',
#         # skip_exit_code: int = 99,
#     )

extract >> process    