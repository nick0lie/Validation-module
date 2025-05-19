from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'nick',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

# Создание DAG
with DAG(
    'call_update_function',
    default_args=default_args,
    schedule_interval='@once',  # Выполнять один раз
) as dag:

    # Задача для вызова функции update_function в PostgreSQL
    call_update_function = PostgresOperator(
        task_id='call_update_function',
        postgres_conn_id='my_postgres_conn',  # ID соединения с базой данных
        sql='SELECT public.update_function();',  # SQL для вызова функции
    )

    call_update_function
