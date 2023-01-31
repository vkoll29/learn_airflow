from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'owner': 'vkoll29',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='taskflow_dag_v4',
    default_args=default_args,
    start_date=datetime(2022, 9, 22),
    schedule_interval='@daily'
)
def test_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {'fname': 'Noname',
                'lname': 'General'}
    @task
    def get_age():
        return 21
    @task
    def greet(fname, lname, age):
        print(f'Name is {fname} {lname} and {age} old')

    name = get_name()
    greet(name['fname'], name['lname'], get_age())


greet_dag = test_etl()
