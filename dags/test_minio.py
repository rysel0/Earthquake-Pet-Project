from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def check_my_connection():
    print("Начинаем проверку подключения my_minio_conn...")
    try:
        # Инициализируем хук, используя твое имя подключения
        hook = S3Hook(aws_conn_id='my_minio_conn')
        
        # Вызываем базовый метод S3 API, чтобы получить список бакетов
        buckets = hook.check_for_bucket('prod') # Проверит, есть ли бакет 'prod'
        
        print(f"Успех! Бакет 'prod' существует? Ответ: {buckets}")
        
        # Дополнительно выведем все бакеты, которые видит хук
        s3_client = hook.get_conn()
        response = s3_client.list_buckets()
        print("Список всех доступных бакетов в MinIO:")
        for bucket in response['Buckets']:
            print(f" - {bucket['Name']}")
            
    except Exception as e:
        print(f"Ошибка подключения! Проверь настройки в Airflow Connections. Текст ошибки: {e}")

# Этот блок позволяет запустить функцию напрямую, если вызвать файл через python
if __name__ == "__main__":
    check_my_connection()