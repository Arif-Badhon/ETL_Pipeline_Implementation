from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import requests

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='etl_stock_data',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    create_table = PostgresOperator(
    task_id="create_stock_table",
    postgres_conn_id="postgres_default",
    sql="""
    CREATE TABLE IF NOT EXISTS stock_data (
        date DATE PRIMARY KEY,
        open DECIMAL(10,2),
        high DECIMAL(10,2),
        low DECIMAL(10,2),
        close DECIMAL(10,2)
    );
    """,
    )
    @task
    def transform(data):
        return {
            'date': data['t'],
            'open': data['o'],
            'high': data['h'],
            'low': data['l'],
            'close': data['c']
        }

    @task
    def load(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = """
        INSERT INTO stock_data (date, open, high, low, close)
        VALUES (%(date)s, %(open)s, %(high)s, %(low)s, %(close)s)
        """
        pg_hook.run(sql, parameters=transformed_data)

