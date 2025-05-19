from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import psycopg2
import os

def export_results_by_period(**context):
    conf = context.get("dag_run").conf or {}
    start_date = conf.get("start_date")
    end_date = conf.get("end_date")

    conn = psycopg2.connect(
        dbname="validation_results",
        user="val_user",
        password="val_pass",
        host="validation-db",
        port="5432"
    )
    cursor = conn.cursor()

    # Получаем все результаты за период
    cursor.execute(f"""
        SELECT vr.result_id, vr.check_date, vr.status, vr.error_count,
               vr.errors, r.rule_name, ct.table_name
        FROM validation.validation_result vr
        LEFT JOIN validation.validation_rule r ON vr.rule_id = r.rule_id
        LEFT JOIN validation.check_table ct ON vr.table_id = ct.table_id
        WHERE vr.check_date BETWEEN %s AND %s
        ORDER BY r.rule_name, vr.check_date;
    """, (start_date, end_date))

    results = cursor.fetchall()
    if not results:
        raise Exception("Нет данных за указанный период")

    columns = ["result_id", "check_date", "status", "error_count", "errors", "rule_name", "table_name"]
    df = pd.DataFrame(results, columns=columns)

    grouped = df.groupby("rule_name")

    output_path = f"/opt/airflow/reports/results_by_period_{start_date}_to_{end_date}.xlsx"
    with pd.ExcelWriter(output_path) as writer:
        for rule, group_df in grouped:
            sheet_name = rule[:31]
            group_df.to_excel(writer, sheet_name=sheet_name, index=False)

    cursor.close()
    conn.close()
    print(f"Файл экспортирован: {output_path}")

with DAG(
    dag_id="export_results_by_period",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["export", "dynamic"]
) as dag:

    export_by_period = PythonOperator(
        task_id="export_results_for_period",
        python_callable=export_results_by_period,
        provide_context=True,
        params={
            "start_date": "2024-01-01", 
            "end_date": "2024-12-31"
        }
    )
