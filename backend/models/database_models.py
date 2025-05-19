from sqlalchemy import Column, Integer, String, Boolean, Text, ForeignKey, TIMESTAMP, DateTime
from sqlalchemy.orm import relationship
from database import Base
from sqlalchemy.dialects.postgresql import ARRAY


class CheckTable(Base):
    __tablename__ = "check_table"
    __table_args__ = {"schema": "validation"}

    table_id = Column(Integer, primary_key=True, index=True)
    table_name = Column(String(200), unique=True, nullable=False)
    table_description = Column(Text)
    last_validation_date = Column(TIMESTAMP)

    results = relationship("ValidationResult", back_populates="table")


class ValidationRule(Base):
    __tablename__ = "validation_rule"
    __table_args__ = {"schema": "validation"}

    rule_id = Column(Integer, primary_key=True, index=True)
    rule_name = Column(String(100), unique=True, nullable=False)
    rule_description = Column(Text)
    rule_type = Column(String, nullable=False)
    sql_text = Column(Text, nullable=False)

    results = relationship("ValidationResult", back_populates="rule")


class ValidationResult(Base):
    __tablename__ = "validation_result"
    __table_args__ = {"schema": "validation"}

    result_id = Column(Integer, primary_key=True, index=True)
    rule_id = Column(Integer, ForeignKey("validation.validation_rule.rule_id", ondelete="CASCADE"), nullable=False)
    table_id = Column(Integer, ForeignKey("validation.check_table.table_id", ondelete="CASCADE"), nullable=False)
    check_date = Column(TIMESTAMP, nullable=False)
    status = Column(Boolean, nullable=False)
    error_count = Column(Integer, default=0)
    tables_used = Column(ARRAY(String))  
    error_log = Column(Text, nullable=True)  

    rule = relationship("ValidationRule", back_populates="results")
    table = relationship("CheckTable", back_populates="results")
    errors = relationship("ValidationError", back_populates="result")


class ValidationError(Base):
    __tablename__ = "validation_error"
    __table_args__ = {"schema": "validation"}

    error_id = Column(Integer, primary_key=True, index=True)
    result_id = Column(Integer, ForeignKey("validation.validation_result.result_id", ondelete="CASCADE"), nullable=False)
    table_name = Column(String(200))
    rule_name = Column(String(100))
    record_reference = Column(Text)
    error_description = Column(Text)

    result = relationship("ValidationResult", back_populates="errors")

class DBConnection(Base):
    __tablename__ = "db_connection"
    __table_args__ = {"schema": "validation"}

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    host = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    dbname = Column(String, nullable=False)
    user = Column(String, nullable=False)
    password = Column(String, nullable=False)