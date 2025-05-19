from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, date
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

def fetch_duplicates(cursor, table_name, key_columns):
    key_columns_str = ', '.join(key_columns)
    query = f"""
    SELECT {key_columns_str}, COUNT(*) 
    FROM {table_name} 
    GROUP BY {key_columns_str}
    HAVING COUNT(*) > 1;
    """
    cursor.execute(query)
    return cursor.fetchall()

def serialize_data(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value

def log_duplicate_rows(conn_params, duplicates, check_id, table_name, check_initiator, check_type, key_columns):
    if not duplicates:
        return

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        cursor.execute('SELECT COALESCE(MAX(row_id), 0) + 1 FROM "etl_sys"."Result_full";')
        max_row_id = cursor.fetchone()[0]

        for duplicate in duplicates:
            duplicate_count = duplicate[-1]
            duplicate_data = {key_columns[i]: serialize_data(duplicate[i]) for i in range(len(key_columns))}
            duplicate_data['duplicate_count'] = duplicate_count

            duplicate_json = json.dumps(duplicate_data)

            insert_query = """
            INSERT INTO "etl_sys"."Result_full" (row_id, check_id, check_type, check_table, row_data, check_initiator, check_date) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (max_row_id, check_id, check_type, table_name, duplicate_json, check_initiator, datetime.now(moscow_tz)))
            max_row_id += 1

        conn.commit()
        print("Данные успешно вставлены.")

    except Exception as e:
        print(f"Ошибка при вставке данных: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

def find_duplicates(conn_params, table_name, key_columns, check_initiator):
    conn = None
    cursor = None
    duplicates = []
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        duplicates = fetch_duplicates(cursor, table_name, key_columns)

        count_duplicates = len(duplicates)
        print(f"Найдено дубликатов: {count_duplicates}")

        check_id = log_check_result(conn_params, 'DUPLICATE_check', table_name, count_duplicates, check_initiator)

        if check_id is None:
            raise ValueError("Не удалось получить check_id!")

        log_duplicate_rows(conn_params, duplicates, check_id, table_name, check_initiator, 'DUPLICATE_check', key_columns)

        return count_duplicates, duplicates

    except Exception as e:
        print(f"Ошибка при проверке таблицы {table_name}: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def check_table_for_duplicates(table_name, check_initiator):
    conn_id = 'my_postgres_conn'
    conn = BaseHook.get_connection(conn_id)

    conn_params = {
        'host': conn.host,
        'dbname': conn.schema,
        'user': conn.login,
        'password': conn.password
    }

    ref_conn = psycopg2.connect(**conn_params)
    ref_cursor = ref_conn.cursor()
    ref_cursor.execute('SELECT pk_fields FROM "etl_sys"."etl_table_delta_cfg" WHERE table_nm=%s;', (table_name,))
    result = ref_cursor.fetchone()
    ref_cursor.close()
    ref_conn.close()

    if result is None:
        print(f"No entry found for table {table_name} in etl_table_delta_cfg.")
        return

    key_columns = result[0].split(", ")
    find_duplicates(conn_params, table_name, key_columns, check_initiator)

default_args = {
    'owner': 'Nick',
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
}

with DAG('check_duplicate_values_dag', default_args=default_args, schedule_interval='@daily') as dag:
    check_table_task = PythonOperator(
        task_id='check_duplicate_values',
        python_callable=check_table_for_duplicates,
        op_kwargs={'table_name': '"Practic"."cash_flow_fact_claim_x_ins_risk"', 'check_initiator': 'Nick'},
    )

    check_table_task
