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

def run_attribute_reference_check(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    result_id = conf.get("result_id")
    db_url = conf.get("db_url")
    table_params = conf.get("table_params", {})

    source_table = table_params.get("source_table")
    reference_table = table_params.get("reference_table")

    if not all([result_id, db_url, source_table, reference_table]):
        raise ValueError("Не указаны обязательные параметры (source_table, reference_table, result_id)")

    quoted_table = quote_full_table_name(source_table)
    quoted_ref = quote_full_table_name(reference_table)

    engine = create_engine(db_url)
    engine_result = create_engine("postgresql+psycopg2://val_user:val_pass@validation-db:5432/validation_results")
    SessionLocal = sessionmaker(bind=engine_result)
    session = SessionLocal()

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT * FROM {quoted_table}")).fetchall()
            columns = rows[0]._fields if rows else []

            ref_rows = conn.execute(text(f"SELECT field_name, valid_value FROM {quoted_ref}")).fetchall()
            ref_dict = {}
            for field, val in ref_rows:
                val_normalized = val.strip() if isinstance(val, str) else val
                ref_dict.setdefault(field, set()).add(val_normalized)

            invalid_rows = []
            for row in rows:
                for field in columns:
                    if field in ref_dict:
                        value = getattr(row, field)
                        norm_value = value.strip() if isinstance(value, str) else value
                        if norm_value not in ref_dict[field]:
                            invalid_rows.append({
                                **dict(row._mapping),
                                "invalid_field": field,
                                "invalid_value": value,
                                "table_name": source_table
                            })
                            break

        error_count = len(invalid_rows)

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

        for row in invalid_rows:
            session.execute(text("""
                INSERT INTO validation.validation_error
                    (result_id, table_name, rule_name, record_reference, error_description)
                VALUES
                    (:result_id, :table_name, :rule_name, :reference, :description)
            """), {
                "result_id": result_id,
                "table_name": source_table,
                "rule_name": "attribute_reference_check",
                "reference": json.dumps(row, default=str, ensure_ascii=False),
                "description": f"Недопустимое значение '{row['invalid_value']}' в поле '{row['invalid_field']}'"
            })

        session.commit()

    except Exception as e:
        session.rollback()
        logging.error("Ошибка выполнения attribute_reference_check", exc_info=e)
        raise
    finally:
        session.close()

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="attribute_reference_check",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Проверка значений полей по справочнику"
) as dag:

    task = PythonOperator(
        task_id="run_attribute_reference_check",
        python_callable=run_attribute_reference_check,
        provide_context=True
    )
