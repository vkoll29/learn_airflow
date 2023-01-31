from datetime import datetime, timedelta
import csv
import logging
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'vkoll29',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


def postgres_to_s3(ds_nodash, next_ds_nodash):
    # 1. query data from postgres and save to .txt
    phook = PostgresHook(postgres_conn_id='postgres_local')
    conn = phook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT * FROM revenue where delivery_date >= '{ds_nodash}' and delivery_date <'{next_ds_nodash}'"
    )
    with NamedTemporaryFile(mode='w', suffix=f'{ds_nodash}') as file:
    #with open(f"dags/revenue{ds_nodash}.txt", 'w') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(i[0] for i in cursor.description)
        csv_writer.writerows(cursor)
        file.flush()
        cursor.close()
        conn.close()
        logging.info(f"Saved daily revenue information in revenue{ds_nodash}.txt")

        #2 upload text file to s3
        s3_hook = S3Hook(aws_conn_id='minio_conn')
        s3_hook.load_file(
            filename=file.name,
            bucket_name='airflow',
            key=f"revenue/revenue{ds_nodash}.txt",
            replace=True
        )
        logging.info(f'Revenue data file {file.name} has been pushed to s3')


with DAG(
    dag_id='pg_minio_v5',
    description='Postgres with minion s3 bucket',
    default_args=default_args,
    start_date=datetime(2022, 7, 22),
    schedule_interval='30 6 * * 6'

) as dag:
    t1 = PythonOperator(
        task_id='read_from_pg',
        python_callable=postgres_to_s3
    )

    t1
