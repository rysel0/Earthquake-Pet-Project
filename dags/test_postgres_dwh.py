from airflow.providers.postgres.hooks.postgres import PostgresHook

def check_my_connection():
    print("Начинаем проверку подключения postgres_dwh_conn...")
    try:
        hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")

        records = hook.get_records("SELECT version();")
        print(f"Успех! PostgreSQL: {records[0][0]}")

    except Exception as e:
        print(
            "Ошибка подключения! Проверь настройки в Airflow Connections. "
            f"Текст ошибки: {e}"
        )


if __name__ == "__main__":
    check_my_connection()
