from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum

OWNER = "rysel0"
DAG_ID = "fct_count_day_earthquake"

SCHEMA = "dm"
TARGET_TABLE = "fct_count_day_earthquake"

PG_CONNECT = "postgres_dwh_conn"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 9, 18, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    description="DAG for counting the number of earthquakes per day",
    default_args=args,
    tags=["dm"],
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    start = EmptyOperator(task_id="start")

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_s3_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
    )

    init_target_infrastructure = SQLExecuteQueryOperator(
        task_id="init_target_infrastructure",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TARGET_TABLE} (
            date DATE,
            count INT
        );
        """,
    )

    drop_stg_table_before = SQLExecuteQueryOperator(
        task_id="drop_stg_table_before",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{data_interval_start.format("YYYY-MM-DD")}}}}"
        """,
    )

    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{data_interval_start.format("YYYY-MM-DD")}}}}" AS 
        SELECT
            time::date as date,
            COUNT(*)
        FROM ods.fct_earthquake
        WHERE time::date = '{{{{data_interval_start.format("YYYY-MM-DD")}}}}'
        GROUP BY 1
        """,
    )
    drop_from_target_table = SQLExecuteQueryOperator(
        task_id="drop_from_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date = '{{{{data_interval_start.format("YYYY-MM-DD")}}}}'
        """,
    )
    insert_into_target_table = SQLExecuteQueryOperator(
        task_id = "insert_into_target_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql = f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{data_interval_start.format("YYYY-MM-DD")}}}}"
        """,
    )
    
    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id="drop_stg_table_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{data_interval_start.format("YYYY-MM-DD")}}}}"
        """,
    )
    end = EmptyOperator(task_id="end")

    (
        start >> 
        sensor_on_raw_layer >> 
        init_target_infrastructure >>
        drop_stg_table_before >> 
        create_stg_table >>
        drop_from_target_table >> 
        insert_into_target_table >> 
        drop_stg_table_after >> 
        end
    )