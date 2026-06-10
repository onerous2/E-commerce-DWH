from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

#Настройки DAG
default_args = {
    'owner' : 'com_developer',
    'depends_on_past' : False,
    'start_date' : datetime(2026, 6, 1),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    'transform_core_layer',
    default_args=default_args,
    description='Трансформация данных из staging в core (Star Schema)',
    schedule_interval='@daily', #Запуск раз в день
    catchup=False,
    tags=['core', 'dwh', 'transformation']
) as dag:
    
    #Задача №1: Клиентская база
    dim_customers = PostgresOperator(
        task_id='load_dim_customers',
        postgres_conn_id='postgres_dwh',
        sql="""
            INSERT INTO core.dim_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
            SELECT TRIM(customer_id), TRIM(customer_unique_id), TRIM(customer_zip_code_prefix), LOWER(TRIM(customer_city)), UPPER(TRIM(customer_state))
            FROM staging.stg_customers
            ON CONFLICT (customer_id) DO NOTHING;
        """
    )    
    # 2. Задача: Измерение продавцов
    dim_sellers = PostgresOperator(
        task_id='load_dim_sellers',
        postgres_conn_id='postgres_dwh',
        sql="""
            INSERT INTO core.dim_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
            SELECT TRIM(seller_id), TRIM(seller_zip_code_prefix), LOWER(TRIM(seller_city)), UPPER(TRIM(seller_state))
            FROM staging.stg_sellers
            ON CONFLICT (seller_id) DO NOTHING;
        """
    )

    #3. Задача: Измерение товарооборота
    dim_products = PostgresOperator(
        task_id='load_dim_products',
        postgres_conn_id='postgres_dwh',
        sql="""
            INSERT INTO core.dim_products (product_id, product_category_name_english, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
            SELECT 
                TRIM(p.product_id), COALESCE(TRIM(t.product_category_name_english), 'unknown'),
                CAST(NULLIF(TRIM(p.product_weight_g), '') AS INT), CAST(NULLIF(TRIM(p.product_length_cm), '') AS INT),
                CAST(NULLIF(TRIM(p.product_height_cm), '') AS INT), CAST(NULLIF(TRIM(p.product_width_cm), '') AS INT)
            FROM staging.stg_products p
            LEFT JOIN staging.stg_product_category_translation t ON TRIM(p.product_category_name) = TRIM(t.product_category_name)
            ON CONFLICT (product_id) DO NOTHING;
        """
    )
    # 4. Задача: Календарь
    dim_date = PostgresOperator(
        task_id='load_dim_date',
        postgres_conn_id='postgres_dwh',
        sql="""
            INSERT INTO core.dim_date (date_dim, year_id, month_id, month_name, day_id, day_of_week, day_name, is_weekend)
            SELECT 
                datum AS date_dim, EXTRACT(YEAR FROM datum) AS year_id, EXTRACT(MONTH FROM datum) AS month_id,
                TO_CHAR(datum, 'TMMonth') AS month_name, EXTRACT(DAY FROM datum) AS day_id,
                EXTRACT(ISODOW FROM datum) AS day_of_week, TO_CHAR(datum, 'TMDay') AS day_name,
                CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
            FROM generate_series('2016-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) datum
            ON CONFLICT (date_dim) DO NOTHING;
        """
    )

    # 5. Задача: Таблица Фактов
    fact_order_items = PostgresOperator(
        task_id='load_fact_order_items',
        postgres_conn_id='postgres_dwh',
        sql="""
            INSERT INTO core.fact_order_items (
                order_id, order_item_id, customer_id, product_id, seller_id, order_status,
                order_purchase_timestamp, order_approved_at, order_delivered_carrier_date,
                order_delivered_customer_date, order_estimated_delivery_date, shipping_limit_date, price, freight_value
            )
            SELECT
                TRIM(oi.order_id), CAST(TRIM(oi.order_item_id) AS INT), TRIM(o.customer_id), TRIM(oi.product_id), TRIM(oi.seller_id), TRIM(o.order_status),
                CAST(NULLIF(TRIM(o.order_purchase_timestamp), '') AS TIMESTAMP), CAST(NULLIF(TRIM(o.order_approved_at), '') AS TIMESTAMP),
                CAST(NULLIF(TRIM(o.order_delivered_carrier_date), '') AS TIMESTAMP), CAST(NULLIF(TRIM(o.order_delivered_customer_date), '') AS TIMESTAMP),
                CAST(NULLIF(TRIM(o.order_estimated_delivery_date), '') AS TIMESTAMP), CAST(NULLIF(TRIM(oi.shipping_limit_date), '') AS TIMESTAMP),
                CAST(NULLIF(TRIM(oi.price), '') AS NUMERIC(10, 2)), CAST(NULLIF(TRIM(oi.freight_value), '') AS NUMERIC(10, 2))
            FROM staging.stg_order_items oi
            INNER JOIN staging.stg_orders o ON TRIM(oi.order_id) = TRIM(o.order_id)
            ON CONFLICT (order_id, order_item_id) DO NOTHING;
        """
    )

    # ---------------------------------------------------------
    # НАСТРОЙКА ЗАВИСИМОСТЕЙ (Оркестрация)
    # ---------------------------------------------------------
    # Измерения не зависят друг от друга, их можно грузить параллельно.
    # Но таблица фактов ДОЛЖНА загружаться только ПОСЛЕ того, как загружены все измерения.
    
    [dim_customers, dim_sellers, dim_products, dim_date] >> fact_order_items
        
    