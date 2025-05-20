from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import ast
import json

def export_to_excel(**kwargs):
    conf = kwargs["dag_run"].conf
    result_id = conf["result_id"]

    engine = create_engine("postgresql+psycopg2://val_user:val_pass@validation-db:5432/validation_results")

    with engine.connect() as conn:
        df = pd.read_sql(f"""
            SELECT table_name, rule_name, record_reference, error_description
            FROM validation.validation_error
            WHERE result_id = {result_id}
        """, conn)

        if df.empty:
            table_name = "NoErrors"
            details_df = pd.DataFrame()
        else:
            table_name = df.iloc[0]["table_name"]
            parsed = []

            for record in df["record_reference"]:
                try:
                    parsed.append(ast.literal_eval(record))
                except Exception:
                    parsed.append({})

            details_df = pd.DataFrame(parsed)

    path = f"/opt/airflow/reports/report_result_{result_id}.xlsx"

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Check_Result", index=False)
        details_df.to_excel(writer, sheet_name=table_name[:31], index=False)  # Excel ограничение 31 символ

    print(f"Файл сохранён: {path}")

# Airflow DAG
default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="export_result_to_excel",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Экспорт результатов проверок в Excel"
) as dag:
    task = PythonOperator(
        task_id="export_errors",
        python_callable=export_to_excel,
        provide_context=True,
    )
