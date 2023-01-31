from datetime import datetime, timedelta
import csv
import logging

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'vkoll29',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


@dag(
    dag_id='postgres_minio_v1',
    default_args=default_args,
    start_date=datetime(2022, 8, 22),
    schedule_interval='30 6 * * 6',
)
def etl():
    curr = '{{ ds_nodash }}'
    nxt = "{{ next_ds_nofdash }}"

    @task(task_id='logg')
    def task2():
        print(curr, nxt)

    @task(task_id='read_from_postgres')
    def postgres_to_s3():

        print(curr, nxt)
        # 1. query data from postgres and save to .txt
        hook = PostgresHook(postgres_conn_id='postgres_local')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM customer_retention where order_date between {curr} and {nxt} "
        )

        with open(f"dags/retention_{curr}.txt", 'w') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(i[0] for i in cursor.description)
            csv_writer.writerows(cursor)
        cursor.close()
        conn.close()
        logging.info(f"Saved customer retention information in retention_{curr}.txt")

        # 2. upload .txt to s3 (minio)

    #postgres_to_s3('{{ ds_nodash }}', '{{ next_ds_nodash }}')
    #postgres_to_s3()
    task2()


pg_minio_dag = etl()
