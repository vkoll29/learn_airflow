from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'vkoll29',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='minio_dagv3',
        schedule_interval='40 3 * * *',
        start_date=datetime(2022, 9, 24),
        default_args=default_args,
        catchup=False
) as dag:
    t1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn',
        mode='poke',
        poke_interval=5,
        timeout=45

    )
