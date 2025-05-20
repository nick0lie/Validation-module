from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import logging

Base = declarative_base()

def quote_full_table_name(full_name: str) -> str:
    """Преобразует 'schema.table' → '"schema"."table"'"""
    if "." not in full_name:
        raise ValueError("Ожидался формат schema.table")
    schema, table = full_name.split(".")
    return f'"{schema}"."{table}"'

def run_null_check(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    db_url = conf.get("db_url")
    result_id = conf.get("result_id")
    table = conf.get("table_params", {}).get("source_table")

    if not all([db_url, result_id, table]):
        raise ValueError("Необходимы db_url, result_id и source_table")

    schema, table_name = table.strip('"').split(".")
    quoted_table = quote_full_table_name(table)

    # Подключение к источнику (внешняя БД)
    engine_source = create_engine(db_url)

    # Подключение к БД результатов
    engine_result = create_engine("postgresql+psycopg2://val_user:val_pass@validation-db:5432/validation_results")
    SessionLocal = sessionmaker(bind=engine_result)
    session = SessionLocal()

    try:
        with engine_source.connect() as conn:
            # Получаем все колонки
            cols = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :table
            """), {"schema": schema, "table": table_name}).fetchall()

            col_names = [col[0] for col in cols]
            if not col_names:
                raise Exception("Не удалось получить список колонок")

            # Строим SQL с проверкой на NULL
            condition = " OR ".join([f'"{col}" IS NULL' for col in col_names])
            query = f"SELECT * FROM {quoted_table} WHERE {condition}"

            rows = conn.execute(text(query)).fetchall()
            headers = rows[0].keys() if rows else col_names
            error_count = len(rows)

        # Обновляем validation_result
        session.execute(text("""
            UPDATE validation.validation_result
            SET status = :status,
                error_count = :error_count,
                check_date = now()
            WHERE result_id = :result_id
        """), {
            "status": error_count == 0,
            "error_count": error_count,
            "result_id": result_id
        })

        # Добавляем ошибки
        if error_count:
            for row in rows:
                session.execute(text("""
                    INSERT INTO validation.validation_error
                        (result_id, table_name, rule_name, record_reference, error_description)
                    VALUES
                        (:result_id, :table_name, :rule_name, :reference, :description)
                """), {
                    "result_id": result_id,
                    "table_name": table,
                    "rule_name": "null_check",
                    "reference": str(dict(row._mapping)),
                    "description": "Обнаружены NULL значения"
                })

        session.commit()

    except Exception as e:
        session.rollback()
        logging.error("Ошибка выполнения null_check", exc_info=e)
        raise
    finally:
        session.close()

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="null_check",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Проверка на NULL значения в таблице"
) as dag:
    task = PythonOperator(
        task_id="run_null_check",
        python_callable=run_null_check,
        provide_context=True
    )
