from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'vkoll29',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


def get_name(ti):
    ti.xcom_push(key='name', value='Noname Junior')
    ti.xcom_push(key='age', value='21')


def greet(ti):
    name = ti.xcom_pull(task_ids='get_name', key='name')
    age = ti.xcom_pull(task_ids='get_name', key='age')
    print(f'Hello {name}. I am {age} old')


with DAG(
    dag_id='python_dag_v5',
    description='First python operator dag',
    default_args=default_args,
    start_date=datetime(2022, 9, 22),
    schedule_interval='@daily'

) as dag:
    t1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        #op_kwargs={'name': 'Noname'}
    )
    t2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    t2 >> t1

