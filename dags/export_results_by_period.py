from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import json
import os

def export_results_by_period(**kwargs):
    conf = kwargs["dag_run"].conf
    start_date = conf.get("start_date")
    end_date = conf.get("end_date")
    rule_names = conf.get("rule_names", [])

    if not start_date or not end_date:
        raise ValueError("start_date и end_date обязательны")

    engine = create_engine("postgresql+psycopg2://val_user:val_pass@validation-db:5432/validation_results")

    with engine.connect() as conn:
        # Фильтр по rule_names
        rule_filter = ""
        params = {"start": start_date, "end": end_date}
        if rule_names:
            rule_filter = "AND r.rule_name = ANY(:rules)"
            params["rules"] = rule_names

        # Получаем summary
        query_summary = f"""
            SELECT vr.result_id, vr.check_date, vr.status, vr.error_count,
                   r.rule_name, t.table_name
            FROM validation.validation_result vr
            JOIN validation.validation_rule r ON r.rule_id = vr.rule_id
            JOIN validation.check_table t ON t.table_id = vr.table_id
            WHERE vr.check_date BETWEEN :start AND :end
              {rule_filter}
            ORDER BY vr.check_date;
        """
        rows = conn.execute(text(query_summary), params).fetchall()
        if not rows:
            raise ValueError("Нет результатов за указанный период")

        df_summary = pd.DataFrame(rows, columns=[
            "result_id", "check_date", "status", "error_count", "rule_name", "table_name"
        ])

        result_ids = df_summary["result_id"].tolist()

        # Получаем ошибки
        errors = conn.execute(text("""
            SELECT result_id, table_name, rule_name, record_reference, error_description
            FROM validation.validation_error
            WHERE result_id = ANY(:ids)
        """), {"ids": result_ids}).fetchall()

        df_errors = pd.DataFrame(errors, columns=[
            "result_id", "table_name", "rule_name", "record_reference", "error_description"
        ])

    # Создаём Excel
    os.makedirs("/opt/airflow/reports", exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = f"/opt/airflow/reports/results_{start_date}_to_{end_date}_{timestamp}.xlsx"


    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Первый лист: tables
        df_summary.to_excel(writer, sheet_name="tables", index=False)

        if not df_errors.empty:
            for table_name, group in df_errors.groupby("table_name"):
                # Парсим record_reference
                parsed = []
                for record in group["record_reference"]:
                    try:
                        parsed.append(json.loads(record))
                    except Exception:
                        parsed.append({})

                df_parsed = pd.DataFrame(parsed)
                result_df = pd.concat([group[["rule_name", "error_description"]].reset_index(drop=True), df_parsed], axis=1)

                sheet_name = table_name.replace(".", "_")[:31]
                result_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"[✓] Финальный отчёт создан: {output_path}")

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="export_results_by_period",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Экспорт отчётов за период (по таблицам)"
) as dag:

    task = PythonOperator(
        task_id="export_results_task",
        python_callable=export_results_by_period,
        provide_context=True
    )
