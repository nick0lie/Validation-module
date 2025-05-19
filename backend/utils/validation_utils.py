from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database_models import DBConnection, ValidationResult
from database import SQLALCHEMY_DATABASE_URL
from datetime import datetime

def get_connection_url(connection_name: str) -> str:
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    conn = session.query(DBConnection).filter_by(name=connection_name).first()
    if not conn:
        raise ValueError("Connection not found")
    
    return f"postgresql://{conn.user}:{conn.password}@{conn.host}:{conn.port}/{conn.dbname}"

def save_validation_result(rule_id: int, error_count: int, rows: list):
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    result = ValidationResult(
        rule_id=rule_id,
        table_id=None,
        check_date=datetime.now(),
        status=(error_count == 0),
        error_count=error_count
    )
    session.add(result)
    session.commit()
