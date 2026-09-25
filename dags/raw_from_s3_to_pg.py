import io
import logging

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "rysel0"
DAG_ID = "raw_from_s3_to_pg"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 9, 18, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_and_transfer_raw_data_to_ods_pg(**context) -> None:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    s3_key = f"{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.csv"
    logging.info("Load s3://prod/%s for interval starting %s", s3_key, start_date)

    csv_text = S3Hook(aws_conn_id="my_minio_conn").read_key(
        key=s3_key,
        bucket_name="prod",
    )

    conn = PostgresHook(postgres_conn_id="postgres_dwh_conn").get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE stg_earthquake_usgs (
                    time text, latitude text, longitude text, depth text, mag text,
                    "magType" text, nst text, gap text, dmi text, rms text, net text,
                    id text, updated text, place text, type text,
                    "horizontalError" text, "depthError" text, "magError" text,
                    "magNst" text, status text, "locationSource" text, "magSource" text
                ) ON COMMIT DROP
                """
            )
            cur.copy_expert(
                "COPY stg_earthquake_usgs FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')",
                io.StringIO(csv_text),
            )
            cur.execute(
                f"""
                INSERT INTO {SCHEMA}.{TARGET_TABLE} (
                    time, latitude, longitude, depth, mag, mag_type, nst, gap, dmin,
                    rms, net, id, updated, place, type,
                    horizontal_error, depth_error, mag_error, mag_nst, status,
                    location_source, mag_source
                )
                SELECT
                    time, latitude, longitude, depth, mag, "magType", nst, gap, dmi,
                    rms, net, id, updated, place, type,
                    "horizontalError", "depthError", "magError", "magNst", status,
                    "locationSource", "magSource"
                FROM stg_earthquake_usgs
                """
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logging.info("Done for %s", start_date)


with DAG(
    dag_id=DAG_ID,
    schedule="0 5 * * *",
    default_args=args,
    tags=["s3", "ods", "pg"],
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    start = EmptyOperator(task_id="start")

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
    )

    load_raw_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(task_id="end")

    start >> sensor_on_raw_layer >> load_raw_to_ods_pg >> end
