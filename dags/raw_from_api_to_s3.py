from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pendulum

import logging
import requests

OWNER = "rysel0"
DAG_ID = "raw_from_api_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"

args = {
    "owner": OWNER,
    "retries": 3,
    "start_date": pendulum.datetime(2026, 9, 24, tz="Europe/Moscow"),
    "retry_delay": pendulum.duration(hours=1),
}

# Берет данные из метабазы
def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date

def get_and_transfer_api_data_to_s3(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load via Airflow Hook for dates: {start_date}/{end_date}")

    # 1. Скачиваем данные из API
    url = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}'
    response = requests.get(url)
    response.raise_for_status()

    # 2. Инициализируем встроенный Хук Airflow, передавая ему ID подключения
    # Все ключи доступа и эндпоинт 'minio:9000' зашиты внутри этого ID соединения
    s3_hook = S3Hook(aws_conn_id='my_minio_conn')

    # 3. Формируем путь в бакете
    s3_key = f"{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.csv"

    # 4. Метод хука сам умеет принимать строку текста и отправлять её в S3/MinIO
    s3_hook.load_string(
        string_data=response.text,
        key=s3_key,
        bucket_name='prod',
        replace=True # Перезаписать, если файл уже существует
    )

    logging.info(f"✅ Download via Hook success: {start_date}")

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    tags = ["s3"],
    schedule="0 5 * * *",
    catchup=False,
) as dag:
    
    start_task = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3_task = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end_task = EmptyOperator(
        task_id="end"
    )

    start_task >> get_and_transfer_api_data_to_s3_task >> end_task