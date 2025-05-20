from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json
import logging

def quote_full_table_name(full_name: str) -> str:
    schema, table = full_name.split(".")
    return f'"{schema}"."{table}"'

def run_duplicate_rows_check(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    result_id = conf.get("result_id")
    db_url = conf.get("db_url")
    table_params = conf.get("table_params", {})

    source_table = table_params.get("source_table")
    key_columns_raw = table_params.get("key_columns")

    if not all([result_id, db_url, source_table, key_columns_raw]):
        raise ValueError("Не указаны обязательные параметры: source_table, key_columns")

    quoted_table = quote_full_table_name(source_table)
    key_columns = [f'"{col.strip()}"' for col in key_columns_raw.split(",")]
    key_expr = ", ".join(key_columns)

    engine_source = create_engine(db_url)
    engine_result = create_engine("postgresql+psycopg2://val_user:val_pass@validation-db:5432/validation_results")
    SessionLocal = sessionmaker(bind=engine_result)
    session = SessionLocal()

    try:
        with engine_source.connect() as conn:
            sql = f"""
                SELECT *, '{source_table}' AS table_name
                FROM {quoted_table}
                WHERE ({key_expr}) IN (
                    SELECT {key_expr}
                    FROM {quoted_table}
                    GROUP BY {key_expr}
                    HAVING COUNT(*) > 1
                )
            """
            rows = conn.execute(text(sql)).fetchall()
            error_count = len(rows)

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

        if error_count > 0:
            for row in rows:
                session.execute(text("""
                    INSERT INTO validation.validation_error
                        (result_id, table_name, rule_name, record_reference, error_description)
                    VALUES
                        (:result_id, :table_name, :rule_name, :reference, :description)
                """), {
                    "result_id": result_id,
                    "table_name": source_table,
                    "rule_name": "duplicate_rows_check",
                    "reference": json.dumps(dict(row._mapping), default=str, ensure_ascii=False),
                    "description": f"Найдены дубли по полям: {key_columns_raw}"
                })

        session.commit()

    except Exception as e:
        session.rollback()
        logging.error("Ошибка выполнения duplicate_rows_check", exc_info=e)
        raise
    finally:
        session.close()

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="duplicate_check",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Проверка на дубли по ключевым полям"
) as dag:

    task = PythonOperator(
        task_id="run_duplicate_rows_check",
        python_callable=run_duplicate_rows_check,
        provide_context=True
    )
