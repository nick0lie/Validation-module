from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Подключение к БД через docker-compose
DATABASE_URL = "postgresql://val_user:val_pass@validation-db:5432/validation_results"

# Создание движка SQLAlchemy
engine = create_engine(DATABASE_URL)

# Создание фабрики сессий
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Базовый класс для ORM-моделей
Base = declarative_base()

# Dependency для FastAPI (можно использовать в маршрутах)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
