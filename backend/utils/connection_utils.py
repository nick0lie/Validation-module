from sqlalchemy import create_engine, text  
from sqlalchemy.exc import SQLAlchemyError

def test_postgres_connection(host: str, port: int, dbname: str, user: str, password: str):
    try:
        url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"success": True, "message": "Подключение успешно"}
    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}
