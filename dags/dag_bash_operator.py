from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'vkoll29',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
        dag_id='first_dag_v5',
        description='First dag written',
        start_date=datetime(2022, 8, 26),
        schedule_interval='@daily',
        default_args=default_args
) as dag:
    # initialize task
    t1 = BashOperator(
        task_id='hello_world1',
        bash_command="echo Hello world this is the first task"
    )

    t2 = BashOperator(
        task_id='second_task',
        bash_command='echo This is task 2 that runs only after task 1'
    )

    t3 = BashOperator(
        task_id='third_task',
        bash_command='echo Task 3 runs after task 1 but concurrent with task2'
    )

    t4 = BashOperator(
        task_id='fourth_task',
        bash_command='date'
    )

    # t1.set_downstream(t2)
    # t1.set_downstream(t3)
    t1 >> [t2, t3] >> t4
