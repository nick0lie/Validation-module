from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import psycopg2
import pytz
import json

moscow_tz = pytz.timezone('Europe/Moscow')

def log_check_result(conn_params, check_type, check_table, check_output, check_initiator):
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        cursor.execute('SELECT COALESCE(MAX(check_id), 0) + 1 FROM "etl_sys"."Result_short";')
        check_id = cursor.fetchone()[0]

        insert_query = """
        INSERT INTO "etl_sys"."Result_short" ("check_id", "check_type", "check_table", 
                                                 "check_result", "check_output", 
                                                 "check_initiator", "check_date")
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        check_result = 1 if check_output > 0 else 0
        cursor.execute(insert_query, (
            check_id,
            check_type,
            check_table,
            check_result,
            check_output,
            check_initiator,
            datetime.now(moscow_tz)
        ))

        conn.commit()
        return check_id

    except Exception as e:
        print(f"Ошибка при записи в таблицу Result_short: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def fetch_negative_rows(cursor, table_name):
    # Проверяем отрицательные значения только в колонке 'payment_amt'
    negative_rows_query = f"""
    SELECT * FROM {table_name} WHERE payment_amt < 0
    """
    print(f"Выполняемый запрос: {negative_rows_query}")
    
    cursor.execute(negative_rows_query)
    
    negative_rows_data = cursor.fetchall()
    print(f"Количество найденных строк с отрицательными значениями: {len(negative_rows_data)}")
    
    result = []
    column_names = [desc[0] for desc in cursor.description]
    for row in negative_rows_data:
        row_dict = {column_names[i]: row[i] for i in range(len(column_names))}
        result.append(row_dict)

    return result

def serialize_value(value):
    """
    Преобразование значения в JSON-совместимый формат.
    """
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [serialize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    return str(value)  # Преобразуем неизвестные типы в строку

def log_negative_rows(conn_params, negative_rows, check_id, table_name, check_initiator, check_type):
    if not negative_rows:
        return  

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Получаем максимальный row_id
        cursor.execute('SELECT COALESCE(MAX(row_id), 0) + 1 FROM "etl_sys"."Result_full";')
        max_row_id = cursor.fetchone()[0]

        for negative_row in negative_rows:
            # Используем функцию serialize_value для преобразования значений в JSON-совместимый формат
            serialized_row = {k: serialize_value(v) for k, v in negative_row.items()}
            negative_row_json = json.dumps(serialized_row)

            print(f"Inserting row: check_id={check_id}, table_name={table_name}, check_initiator={check_initiator}, check_type={check_type}, row_data={negative_row_json}")

            insert_query = """
            INSERT INTO "etl_sys"."Result_full" (row_id, check_id, check_type, check_table, row_data, check_initiator, check_date) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (max_row_id, check_id, check_type, table_name, negative_row_json, check_initiator, datetime.now(moscow_tz)))

            max_row_id += 1

        conn.commit()
        print("Данные успешно вставлены.")

    except Exception as e:
        print(f"Ошибка при вставке данных: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

def find_negative_rows(conn_params, table_name, check_initiator):
    conn = None
    cursor = None
    negative_rows = []
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        negative_rows = fetch_negative_rows(cursor, table_name)

        count_negative_rows = len(negative_rows)
        print(f"Найдено строк с отрицательными значениями: {count_negative_rows}")

        check_id = log_check_result(conn_params, 'NEGATIVE_check', table_name, count_negative_rows, check_initiator)

        if check_id is None:
            raise ValueError("Не удалось получить check_id!")

        log_negative_rows(conn_params, negative_rows, check_id, table_name, check_initiator, 'NEGATIVE_check')

        return count_negative_rows, negative_rows

    except Exception as e:
        print(f"Ошибка при проверке таблицы {table_name}: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def check_table_for_negatives(table_name, check_initiator):
    conn_id = 'my_postgres_conn'
    conn = BaseHook.get_connection(conn_id)

    conn_params = {
        'host': conn.host,
        'dbname': conn.schema,
        'user': conn.login,
        'password': conn.password
    }

    find_negative_rows(conn_params, table_name, check_initiator)

default_args = {
    'owner': 'Nick',
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
}

with DAG('check_negative_values_dag', default_args=default_args, schedule_interval='@daily') as dag:
    check_table_task = PythonOperator(
        task_id='check_negative_values',
        python_callable=check_table_for_negatives,
        op_kwargs={'table_name': '"Practic"."cash_flow_fact_claim_x_ins_risk"', 'check_initiator': 'Nick'},
    )

    check_table_task
