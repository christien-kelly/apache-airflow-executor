from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args: dict = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 27),
    'email': ['christien.kelly@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retires': 5,
    'retry_delay': timedelta(seconds=30),
    'schedule_interval': '*/15 * * * *'
}

with DAG(    
    dag_id="simple_bash_dag",
    default_args=default_args,
    schedule_interval=None,
    tags=["my_dag"],
    ) as dag:

    t1: BashOperator = BashOperator(
        bash_command="touch ~/my_bash_file.txt", 
        task_id="create_file"
    )
    t2: BashOperator = BashOperator(
        bash_command="mv ~/my_bash_file.txt ~/my_bash_file_changed.txt", 
        task_id="change_file_name"
    )
    t1 >> t2


