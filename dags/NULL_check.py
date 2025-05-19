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

def get_column_names(cursor, table_name):
    schema_name, table_name = table_name.split('.')
    table_name = table_name.strip('"')
    schema_name = schema_name.strip('"')
    query = f"""
    SELECT column_name FROM information_schema.columns 
    WHERE table_name='{table_name}' AND table_schema='{schema_name}';"""
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]

def fetch_null_rows(cursor, table_name):
    column_names = get_column_names(cursor, table_name)
    
    
    null_rows_query = f"SELECT * FROM {table_name} WHERE " + " OR ".join([f"{col} IS NULL" for col in column_names])
    cursor.execute(null_rows_query)
    
    
    null_rows_data = cursor.fetchall()
    
    
    result = []
    for row in null_rows_data:
        row_dict = {column_names[i]: row[i] for i in range(len(column_names))}
        result.append(row_dict)

    return result

def log_null_rows(conn_params, null_rows, check_id, table_name, check_initiator, check_type):
    if not null_rows:
        return  

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Получаем максимальный row_id
        cursor.execute('SELECT COALESCE(MAX(row_id), 0) + 1 FROM "etl_sys"."Result_full";')
        max_row_id = cursor.fetchone()[0]

        for null_row in null_rows:
            null_row_json = json.dumps(null_row)

            print(f"Inserting row: check_id={check_id}, table_name={table_name}, check_initiator={check_initiator}, check_type={check_type}, row_data={null_row_json}")

            
            insert_query = """
            INSERT INTO "etl_sys"."Result_full" (row_id, check_id, check_type, check_table, row_data, check_initiator, check_date) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (max_row_id, check_id, check_type, table_name, null_row_json, check_initiator, datetime.now(moscow_tz)))

            
            max_row_id += 1

        conn.commit()
        print("Данные успешно вставлены.")

    except Exception as e:
        print(f"Ошибка при вставке данных: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

def find_null_rows(conn_params, table_name, check_initiator):
    conn = None
    cursor = None
    null_rows = []
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        null_rows = fetch_null_rows(cursor, table_name)

        count_null_rows = len(null_rows)
        print(f"Найдено строк с NULL: {count_null_rows}")

        check_id = log_check_result(conn_params, 'NULL_check', table_name, count_null_rows, check_initiator)

        if check_id is None:
            raise ValueError("Не удалось получить check_id!")

        log_null_rows(conn_params, null_rows, check_id, table_name, check_initiator, 'NULL_check')

        return count_null_rows, null_rows

    except Exception as e:
        print(f"Ошибка при проверке таблицы {table_name}: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def check_table_for_nulls(table_name, check_initiator):
    conn_id = 'my_postgres_conn'
    conn = BaseHook.get_connection(conn_id)

    conn_params = {
        'host': conn.host,
        'dbname': conn.schema,
        'user': conn.login,
        'password': conn.password
    }

    find_null_rows(conn_params, table_name, check_initiator)

default_args = {
    'owner': 'Nick',
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
}

with DAG('check_null_values_dag', default_args=default_args, schedule_interval='@daily') as dag:
    check_table_task = PythonOperator(
        task_id='check_null_values',
        python_callable=check_table_for_nulls,
        op_kwargs={'table_name': '"Practic"."agent_contract"', 'check_initiator': 'Nick'},
    )

    check_table_task
