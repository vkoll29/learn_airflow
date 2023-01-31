from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'vkoll29',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
        dag_id='catchup_v2',
        description='First dag written',
        start_date=datetime(2022, 9, 20),
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False
) as dag:
    # initialize task
    t1 = BashOperator(
        task_id='hello_world1',
        bash_command="echo This is a catchup dag"
    )

    t2 = BashOperator(
        task_id='second_task',
        bash_command='echo This is task 2 that runs only after task 1'
    )

    t1 >> t2
