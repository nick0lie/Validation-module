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

def run_negative_flows_check(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    result_id = conf.get("result_id")
    db_url = conf.get("db_url")
    table_params = conf.get("table_params", {})

    source_table = table_params.get("source_table")

    if not all([result_id, db_url, source_table]):
        raise ValueError("Не указана таблица или результат")

    quoted_table = quote_full_table_name(source_table)

    engine = create_engine(db_url)
    engine_result = create_engine("postgresql+psycopg2://val_user:val_pass@validation-db:5432/validation_results")
    SessionLocal = sessionmaker(bind=engine_result)
    session = SessionLocal()

    try:
        with engine.connect() as conn:
            type_query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{source_table.split('.')[0].strip('"')}'
                  AND table_name = '{source_table.split('.')[1].strip('"')}'
                  AND data_type IN ('numeric', 'integer', 'bigint', 'double precision', 'real', 'decimal', 'smallint')
            """
            type_result = conn.execute(text(type_query)).fetchall()
            numeric_columns = [row[0] for row in type_result]

            if not numeric_columns:
                raise ValueError("В таблице нет числовых колонок")

            where_clause = " OR ".join([f'"{col}" < 0' for col in numeric_columns])
            query = f"""
                SELECT *, '{source_table}' AS table_name
                FROM {quoted_table}
                WHERE {where_clause}
            """

            rows = conn.execute(text(query)).fetchall()
            error_count = len(rows)

        # Запись результатов
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

        for row in rows:
            row_dict = dict(row._mapping)
            # ищем первое поле с отрицательным значением
            negative_field = None
            negative_value = None
            for field in numeric_columns:
                val = row_dict.get(field)
                if isinstance(val, (int, float)) and val < 0:
                    negative_field = field
                    negative_value = val
                    break

            description = (
                f"Отрицательное значение {negative_value} в поле '{negative_field}'"
                if negative_field else
                "Обнаружено отрицательное значение"
            )

            session.execute(text("""
                INSERT INTO validation.validation_error
                    (result_id, table_name, rule_name, record_reference, error_description)
                VALUES
                    (:result_id, :table_name, :rule_name, :reference, :description)
            """), {
                "result_id": result_id,
                "table_name": source_table,
                "rule_name": "negative_flows_check",
                "reference": json.dumps(row_dict, default=str, ensure_ascii=False),
                "description": description
            })


        session.commit()

    except Exception as e:
        session.rollback()
        logging.error("Ошибка выполнения negative_flows_check", exc_info=e)
        raise
    finally:
        session.close()

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="negative_flows_check",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Проверка на отрицательные значения в таблице"
) as dag:

    task = PythonOperator(
        task_id="run_negative_flows_check",
        python_callable=run_negative_flows_check,
        provide_context=True
    )
