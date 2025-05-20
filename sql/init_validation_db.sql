-- Создание схемы
CREATE SCHEMA IF NOT EXISTS validation;

-- Справочник проверяемых таблиц
CREATE TABLE IF NOT EXISTS validation.check_table (
    table_id SERIAL PRIMARY KEY,
    table_name VARCHAR(200) NOT NULL UNIQUE,
    table_description TEXT,
    last_validation_date TIMESTAMP
);

-- Каталог правил проверки
CREATE TABLE IF NOT EXISTS validation.validation_rule (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL UNIQUE,
    rule_description TEXT,
    rule_type VARCHAR(50),
    sql_text TEXT NOT NULL
);

-- Результаты выполнения проверок
CREATE TABLE IF NOT EXISTS validation.validation_result (
    result_id SERIAL PRIMARY KEY,
    rule_id INTEGER NOT NULL REFERENCES validation.validation_rule(rule_id) ON DELETE CASCADE,
    table_id INTEGER NOT NULL REFERENCES validation.check_table(table_id) ON DELETE CASCADE,
    check_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status BOOLEAN NOT NULL,
    error_count INTEGER NOT NULL DEFAULT 0
    tables_used TEXT[], -- новое поле
    error_log TEXT       -- новое поле
);

-- Подробности ошибок, выявленных при проверке
CREATE TABLE IF NOT EXISTS validation.validation_error (
    error_id SERIAL PRIMARY KEY,
    result_id INTEGER NOT NULL REFERENCES validation.validation_result(result_id) ON DELETE CASCADE,
    table_name VARCHAR(200),
    rule_name VARCHAR(100),
    record_reference TEXT,
    error_description TEXT
);

-- Подлкючния к бд для проверок
CREATE TABLE IF NOT EXISTS validation.db_connection (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    host VARCHAR NOT NULL,
    port INTEGER NOT NULL,
    dbname VARCHAR NOT NULL,
    user VARCHAR NOT NULL,
    password VARCHAR NOT NULL
);
