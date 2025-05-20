from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from api.routes_init import router as api_router
from database import engine, SessionLocal
from models.database_models import Base, ValidationRule
from sqlalchemy.exc import OperationalError
from pydantic import BaseModel
import time
import httpx
import json
import os
from sqlalchemy.orm import Session

#Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Validation Module",
    description="Модуль для запуска и управления проверками качества данных",
    version="0.1.0"
)

# Роуты API 
app.include_router(api_router, prefix="/api")

# Настройки CORS, чтобы можно было подключать frontend 
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Корневой эндпоинт для проверки запуска
@app.get("/")
def read_root():
    return {"message": "Добро пожаловать в Validation Module API!"}



for i in range(10):
    try:
        Base.metadata.create_all(bind=engine)
        print("Таблицы успешно созданы")
        break
    except OperationalError as e:
        print(f"Ожидание базы данных ({i+1}/10)...")
        time.sleep(2)
else:
    print("Не удалось подключиться к БД после 10 попыток")
    raise RuntimeError("Database connection failed")

# данные авторизации в Airflow
AIRFLOW_URL = "http://airflow-web:8080"
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"

class DagRunRequest(BaseModel):
    dag_id: str
    conf: dict = {}

@app.post("/trigger_dag")
async def trigger_dag_run(req: DagRunRequest):
    endpoint = f"{AIRFLOW_URL}/api/v1/dags/{req.dag_id}/dagRuns"
    auth = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                endpoint,
                auth=auth,
                json={"conf": req.conf}
            )
            if response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail=response.text)
            return response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def load_rules():
    session: Session = SessionLocal()
    if session.query(ValidationRule).count() > 0:
        session.close()
        return

    file_path = os.path.join(os.path.dirname(__file__), "rules.json")
    if not os.path.exists(file_path):
        print("Файл rules.json не найден, пропускаем загрузку")
        session.close()
        return

    with open(file_path, encoding="utf-8") as f:
        rules = json.load(f)

    for rule in rules:
        existing = session.query(ValidationRule).filter_by(rule_name=rule["rule_name"]).first()
        if not existing:
            session.add(ValidationRule(**rule))

    session.commit()
    session.close()
    print(f"Загружено правил: {len(rules)}")

load_rules()