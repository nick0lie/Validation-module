from pydantic import BaseModel



class DBConnectionBase(BaseModel):
    name: str
    host: str
    port: int
    dbname: str
    user: str
    password: str

class DBConnectionCreate(DBConnectionBase):
    pass

class DBConnectionOut(DBConnectionBase):
    id: int

    class Config:
        orm_mode = True

class ValidationRuleBase(BaseModel):
    rule_name: str
    rule_type: str  
    sql_text: str
    rule_description: str | None = None

class ValidationRuleCreate(ValidationRuleBase):
    pass

class ValidationRuleOut(ValidationRuleBase):
    rule_id: int

    class Config:
        orm_mode = True

class RunCheckRequest(BaseModel):
    rule_name: str
    connection_name: str
    table_params: dict[str, str]

class ExportRequest(BaseModel):
    result_id: int