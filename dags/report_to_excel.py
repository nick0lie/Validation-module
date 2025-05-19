from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import os

def export_to_excel(**kwargs):
    conf = kwargs["dag_run"].conf or {}
    result_id = conf.get("result_id")
    db_url = conf.get("db_url")

    if not result_id or not db_url:
        raise ValueError("Нужно указать result_id и db_url")

    engine = create_engine(db_url)
    with engine.connect() as conn:
        result_query = text("""
            SELECT vr.result_id, vr.check_date, vr.status, vr.error_count,
                   vr.tables_used, vr.error_log,
                   r.rule_name, r.rule_description
            FROM validation.validation_result vr
            JOIN validation.validation_rule r ON r.rule_id = vr.rule_id
            WHERE vr.result_id = :result_id
        """)
        df = pd.read_sql(result_query, conn, params={"result_id": result_id})

        if df.empty:
            raise ValueError(f"Результат с ID {result_id} не найден")

        filename = f"report_result_{result_id}.xlsx"
        file_path = f"/opt/airflow/reports/{filename}"

        os.makedirs("/opt/airflow/reports", exist_ok=True)
        df.to_excel(file_path, index=False)
        print(f"✅ Отчет сохранен: {file_path}")

        return {"status": "success", "file": file_path}

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="report_to_excel",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["export"],
    description="Экспорт результата проверки в Excel по result_id",
) as dag:

    task = PythonOperator(
        task_id="export_excel",
        python_callable=export_to_excel,
        provide_context=True,
    )
