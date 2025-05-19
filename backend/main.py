from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes_init import router as api_router
from database import engine
from models.database_models import Base
from sqlalchemy.exc import OperationalError
import time

#Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Validation Module",
    description="Модуль для запуска и управления проверками качества данных",
    version="0.1.0"
)

# Настройки CORS, чтобы можно было подключать frontend (например, React)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Можно ограничить по домену
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Корневой эндпоинт для проверки запуска
@app.get("/")
def read_root():
    return {"message": "Добро пожаловать в Validation Module API!"}

# Роуты API 
app.include_router(api_router, prefix="/api")

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