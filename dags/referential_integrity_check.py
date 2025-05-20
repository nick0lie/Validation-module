from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json
import logging

def quote_full_table_name(full_name: str) -> str:
    """Разделяет 'schema.table' → '"schema"."table"'"""
    if "." not in full_name:
        raise ValueError("Ожидался формат schema.table")
    schema, table = full_name.split(".")
    return f'"{schema}"."{table}"'

def serialize_value(val):
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return val

def run_check_and_save_to_db(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    result_id = conf.get("result_id")
    db_url = conf.get("db_url")  # источник данных (внешняя БД)
    table_params = conf.get("table_params", {})

    source_table = table_params.get("source_table")
    target_table = table_params.get("target_table")
    source_column = table_params.get("source_column")
    target_column = table_params.get("target_column")

    if not all([result_id, db_url, source_table, target_table, source_column, target_column]):
        raise ValueError("Не все параметры переданы в DAG")

    # Подключение к внешней БД
    engine_source = create_engine(db_url)

    # Подключение к внутренней БД (для записи результатов)
    engine_result = create_engine("postgresql+psycopg2://val_user:val_pass@validation-db:5432/validation_results")
    SessionLocal = sessionmaker(bind=engine_result)
    session = SessionLocal()

    quoted_source = quote_full_table_name(source_table)
    quoted_target = quote_full_table_name(target_table)

    sql = f"""
        SELECT *, '{source_table}' AS table_name
        FROM {quoted_source}
        WHERE "{source_column}" IS NOT NULL
        AND "{source_column}" NOT IN (
            SELECT "{target_column}" FROM {quoted_target}
        );
    """

    try:
        with engine_source.connect() as conn:
            rows = conn.execute(text(sql)).fetchall()
            error_count = len(rows)

        # Обновление записи результата
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

        # Вставка ошибок
        if error_count > 0:
            for row in rows:
                row_dict = {k: serialize_value(v) for k, v in row._mapping.items()}
                session.execute(text("""
                    INSERT INTO validation.validation_error
                        (result_id, table_name, rule_name, record_reference, error_description)
                    VALUES
                        (:result_id, :table_name, :rule_name, :reference, :description)
                """), {
                    "result_id": result_id,
                    "table_name": source_table,
                    "rule_name": "referential_integrity_check",
                    "reference": json.dumps(row_dict, ensure_ascii=False),
                    "description": f"Значение {source_column} отсутствует в {target_table}.{target_column}"
                })

        session.commit()

    except Exception as e:
        session.rollback()
        logging.error("Ошибка в DAG referential_integrity_check:", exc_info=e)
        raise
    finally:
        session.close()

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="referential_integrity_check",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["validation"],
    description="Проверка ссылочной целостности между таблицами"
) as dag:

    run_check = PythonOperator(
        task_id="run_check",
        python_callable=run_check_and_save_to_db,
        provide_context=True,
    )
