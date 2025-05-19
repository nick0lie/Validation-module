from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import os

def quote_pg_identifier(qualified_name: str) -> str:
    if "." in qualified_name:
        schema, table = qualified_name.split(".", 1)
        return f'"{schema}"."{table}"'
    return f'"{qualified_name}"'

def run_referential_check(**kwargs):
    conf = kwargs["dag_run"].conf or {}

    db_url = conf.get("db_url")
    source_table = conf.get("source_table")
    target_table = conf.get("target_table")
    source_column = conf.get("source_column")
    target_column = conf.get("target_column")

    if not db_url:
        raise ValueError("Параметр 'db_url' обязателен")

    if all([source_table, target_table, source_column, target_column]):
        source_table_quoted = quote_pg_identifier(source_table)
        target_table_quoted = quote_pg_identifier(target_table)

        sql = f"""
            SELECT *
            FROM {source_table_quoted}
            WHERE "{source_column}" IS NOT NULL
            AND "{source_column}" NOT IN (
                SELECT "{target_column}"
                FROM {target_table_quoted}
            );
        """
    else:
        raise ValueError("Недостаточно параметров для проверки ссылочной целостности")

    engine = create_engine(db_url)
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
        error_count = len(rows)

        if error_count == 0:
            print("Проверка пройдена. Нарушений не найдено.")
        else:
            print(f"Найдено {error_count} нарушений ссылочной целостности.")
            df = pd.DataFrame(rows)
            output_path = f"/opt/airflow/reports/referential_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_path, index=False)
            print(f"Ошибки сохранены в {output_path}")

        return {
            "status": "passed" if error_count == 0 else "failed",
            "error_count": error_count,
            "report_path": output_path if error_count > 0 else None
        }


default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="referential_integrity_check",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["validation"],
    description="Проверка ссылочной целостности между таблицами",
) as dag:

    check_task = PythonOperator(
        task_id="run_check",
        python_callable=run_referential_check,
        provide_context=True,
    )
