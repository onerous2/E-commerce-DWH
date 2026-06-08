from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

#Базовые настройки по умолчанию
default_args = {
    'owner' : 'com_developer',
    'depends_on_past' : False,
    'start_date' : datetime(2026, 1, 1),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=2),
}

DATA_MAPPING = {
    'olist_customers_dataset.csv': 'staging.stg_customers',
    'olist_geolocation_dataset.csv': 'staging.stg_geolocation',
    'olist_order_items_dataset.csv': 'staging.stg_order_items',
    'olist_order_payments_dataset.csv': 'staging.stg_order_payments',
    'olist_order_reviews_dataset.csv': 'staging.stg_order_reviews',
    'olist_orders_dataset.csv': 'staging.stg_orders',
    'olist_products_dataset.csv': 'staging.stg_products',
    'olist_sellers_dataset.csv': 'staging.stg_sellers',
    'product_category_name_translation.csv': 'staging.stg_product_category_translation'
}

def load_csv_to_staging(csv_file: str, table_name: str):
    """Высокоскоростная очистка и загрузка CSV в Postgres через COPY expert"""
    # Имя connection, которое мы прописали в docker-compose.yml
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    
    # 1. Очищаем таблицу перед загрузкой (обеспечиваем идемпотентность)
    pg_hook.run(f"TRUNCATE TABLE {table_name};")
    
    # Внутри контейнера Airflow папка ./data/raw примонтирована к /opt/airflow/data/raw
    file_path = f"/opt/airflow/data/raw/{csv_file}"
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл {csv_file} не найден в Data Lake по пути {file_path}")
        
    # 2. Используем нативный COPY для моментальной загрузки
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    copy_sql = f"""
        COPY {table_name} 
        FROM STDIN 
        WITH CSV HEADER DELIMITER ',' QUOTE '"';
    """
    
    with open(file_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert(sql=copy_sql, file=f)
        
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Успешно загружено: {csv_file} -> {table_name}")


with DAG(
    'olist_load_staging',
    default_args=default_args,
    description='ЭТАП 1: Извлечение данных из Data Lake и загрузка в Staging слои DWH',
    schedule_interval=None, # Запускаем вручную для тестов
    catchup=False,
    tags=['olist', 'staging'],
) as dag:

    # Динамически генерируем таски для каждого файла
    for csv_file, table_name in DATA_MAPPING.items():
        # Заменяем точки и дефисы в ID задачи, чтобы Airflow не ругался
        task_id = f"load_{csv_file.replace('.', '_').replace('-', '_')}"
        
        PythonOperator(
            task_id=task_id,
            python_callable=load_csv_to_staging,
            op_kwargs={'csv_file': csv_file, table_name: table_name},
        )