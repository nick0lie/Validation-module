from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
from datetime import datetime
from pydantic import BaseModel  
from database import get_db
from models.database_models import CheckTable, ValidationResult, ValidationRule, DBConnection
from utils.connection_utils import test_postgres_connection
from models.schemas import DBConnectionCreate, DBConnectionOut, ValidationRuleCreate, ValidationRuleOut, RunCheckRequest
import json
from fastapi.responses import FileResponse
import os
import requests
import psycopg2
import uuid
import pandas as pd
import glob

router = APIRouter()

@router.post("/trigger_dag")
def trigger_dag(payload: RunCheckRequest, db: Session = Depends(get_db)):
    conn = db.query(DBConnection).filter_by(name=payload.connection_name).first()
    if not conn:
        raise HTTPException(status_code=404, detail="Подключение не найдено")

    rule = db.query(ValidationRule).filter_by(rule_name=payload.rule_name).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Правило не найдено")

    db_url = f"postgresql://{conn.user}:{conn.password}@{conn.host}:{conn.port}/{conn.dbname}"

    dag_conf = {
        "connection_name": conn.name,
        "db_url": db_url,
        **payload.table_params
    }

    airflow_url = f"http://airflow-web:8080/api/v1/dags/{payload.rule_name}/dagRuns"
    dag_run_id = f"{payload.rule_name}__{datetime.utcnow().isoformat()}"

    response = requests.post(
        airflow_url,
        auth=("airflow", "airflow"),
        json={
            "dag_run_id": dag_run_id,
            "conf": dag_conf
        },
        timeout=10
    )

    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Airflow error: {response.text}")

    return {
        "dag_run_id": dag_run_id,
        "dag_id": payload.rule_name,
        "status": "triggered"
    }

@router.get("/report/last")
def get_last_csv_report():
    folder = "/opt/airflow/reports"
    files = glob.glob(f"{folder}/*.csv")

    if not files:
        raise HTTPException(status_code=404, detail="Нет отчетов")

    latest_file = max(files, key=os.path.getctime)
    try:
        df = pd.read_csv(latest_file)
        return {
            "filename": os.path.basename(latest_file),
            "rows": df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка чтения отчета: {e}")

@router.get("/dag_status")
def get_dag_status(
    dag_id: str = Query(...),
    dag_run_id: str = Query(...)
):
    airflow_url = f"http://airflow-web:8080/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"

    try:
        response = requests.get(airflow_url, auth=("airflow", "airflow"))
        if response.status_code != 200:
            return {"state": "unknown", "message": f"Ошибка запроса: {response.text}"}

        dag_run = response.json()
        state = dag_run["state"]

        if state in ("queued", "running"):
            return {"state": "running", "message": "Проверка выполняется..."}

        folder = "/opt/airflow/reports"
        matched = [f for f in os.listdir(folder) if dag_id in f and dag_run_id[:10].replace("T", "_") in f and f.endswith(".csv")]

        if not matched:
            return {
                "state": state,
                "message": "Файл отчета не найден",
                "errors": []
            }

        path = os.path.join(folder, matched[0])
        df = pd.read_csv(path)
        errors = df.to_dict(orient="records")

        return {
            "state": state,
            "message": f"Найдено {len(errors)} ошибок" if errors else "Ошибок не найдено",
            "errors": errors
        }

    except Exception as e:
        return {"state": "error", "message": f"Ошибка: {str(e)}"}
    

@router.get("/connections/test")
def test_connection_route(
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str
):
    return test_postgres_connection(host, port, dbname, user, password)

@router.get("/connections/", response_model=list[DBConnectionOut])
def get_connections(db: Session = Depends(get_db)):
    return db.query(DBConnection).all()

@router.post("/connections/", response_model=DBConnectionOut)
def create_connection(conn_data: DBConnectionCreate, db: Session = Depends(get_db)):
    name_exists = db.query(DBConnection).filter_by(name=conn_data.name).first()
    if name_exists:
        raise HTTPException(status_code=400, detail="Подключение с таким именем уже существует")

    duplicate_conn = db.query(DBConnection).filter_by(
        host=conn_data.host,
        port=conn_data.port,
        dbname=conn_data.dbname
    ).first()
    if duplicate_conn:
        raise HTTPException(status_code=400, detail="Подключение к этой базе данных уже существует")

    test_result = test_postgres_connection(
        conn_data.host,
        conn_data.port,
        conn_data.dbname,
        conn_data.user,
        conn_data.password
    )

    if not test_result["success"]:
        raise HTTPException(status_code=400, detail=f"Ошибка подключения: {test_result['message']}")

    new_conn = DBConnection(**conn_data.dict())
    db.add(new_conn)
    db.commit()
    db.refresh(new_conn)
    return new_conn

@router.delete("/connections/{conn_id}")
def delete_connection(conn_id: int, db: Session = Depends(get_db)):
    conn = db.query(DBConnection).get(conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    db.delete(conn)
    db.commit()
    return {"detail": "Deleted"}

@router.get("/rules/", response_model=list[ValidationRuleOut])
def get_rules(db: Session = Depends(get_db)):
    return db.query(ValidationRule).all()

@router.post("/rules/", response_model=ValidationRuleOut)
def create_rule(rule_data: ValidationRuleCreate, db: Session = Depends(get_db)):
    existing = db.query(ValidationRule).filter_by(rule_name=rule_data.rule_name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Rule with this name already exists")
    new_rule = ValidationRule(**rule_data.dict())
    db.add(new_rule)
    db.commit()
    db.refresh(new_rule)
    return new_rule

@router.delete("/rules/{rule_id}")
def delete_rule(rule_id: int, db: Session = Depends(get_db)):
    rule = db.query(ValidationRule).get(rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    db.delete(rule)
    db.commit()
    return {"detail": "Rule deleted"}

@router.get("/download_report/{result_id}")
def download_report(result_id: int):
    path = f"reports/result_{result_id}.xlsx"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Файл не найден")
    return FileResponse(path, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=f"result_{result_id}.xlsx")

@router.get("/tables")
def get_tables(connection_name: str, db: Session = Depends(get_db)):
    conn_obj = db.query(DBConnection).filter_by(name=connection_name).first()
    if not conn_obj:
        raise HTTPException(status_code=404, detail="Connection not found")

    try:
        conn = psycopg2.connect(
            host=conn_obj.host,
            port=conn_obj.port,
            dbname=conn_obj.dbname,
            user=conn_obj.user,
            password=conn_obj.password,
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_schema || '.' || table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_type='BASE TABLE';
        """)
        result = [row[0] for row in cursor.fetchall()]
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка подключения: {e}")
    finally:
        cursor.close()
        conn.close()

@router.get("/columns")
def get_columns(connection_name: str, table: str, db: Session = Depends(get_db)):
    conn_obj = db.query(DBConnection).filter_by(name=connection_name).first()
    if not conn_obj:
        raise HTTPException(status_code=404, detail="Connection not found")

    try:
        conn = psycopg2.connect(
            host=conn_obj.host,
            port=conn_obj.port,
            dbname=conn_obj.dbname,
            user=conn_obj.user,
            password=conn_obj.password,
        )
        cursor = conn.cursor()

        if "." not in table:
            raise HTTPException(status_code=400, detail="Table name must include schema (e.g., public.my_table)")

        schema, tablename = table.split(".")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s;
        """, (schema, tablename))
        result = [row[0] for row in cursor.fetchall()]
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении столбцов: {e}")
    finally:
        cursor.close()
        conn.close()

AIRFLOW_BASE_URL = "http://airflow-web:8080" 
AIRFLOW_DAG_ID = "export_results_by_period"
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"

@router.post("/export/period")
def trigger_export_by_period(start_date: str, end_date: str):
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{AIRFLOW_DAG_ID}/dagRuns"
    payload = {
        "conf": {
            "start_date": start_date,
            "end_date": end_date
        }
    }

    response = requests.post(url, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD), json=payload)

    if response.status_code == status.HTTP_200_OK:
        return {"message": "Экспорт запущен успешно"}
    else:
        return {
            "message": "Ошибка запуска DAG",
            "status": response.status_code,
            "details": response.json()
        }
    
AIRFLOW_API_URL = "http://airflow-web:8080/api/v1/dags/export_result_to_excel/dagRuns"

@router.post("/export/result/")
def export_result_to_excel(result_id: int):
    response = requests.post(
        AIRFLOW_API_URL,
        json={"conf": {"result_id": result_id}},
        auth=("airflow", "airflow") 
    )
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Ошибка запуска DAG: {response.text}")
    return {"detail": "Экспорт запущен", "dag_run_id": response.json().get("dag_run_id")}

@router.get("/export/result/download/{result_id}")
def download_result_excel(result_id: int):
    file_path = f"/opt/airflow/reports/report_result_{result_id}.xlsx"
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Файл еще не создан или не найден")
    return FileResponse(path=file_path, filename=f"report_result_{result_id}.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
