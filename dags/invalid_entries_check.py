from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, date, time
import decimal
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

def get_reference_values(cursor):
    cursor.execute('SELECT field_name, valid_value FROM "etl_sys"."Reference_table";')
    reference_data = cursor.fetchall()
    
    ref_dict = {}
    for field_name, valid_value in reference_data:
        if field_name not in ref_dict:
            ref_dict[field_name] = set()
        ref_dict[field_name].add(valid_value)
    return ref_dict

def fetch_invalid_rows(cursor, table_name, ref_dict):
    column_names = get_column_names(cursor, table_name)

    select_query = f'SELECT * FROM {table_name};'
    cursor.execute(select_query)
    rows = cursor.fetchall()

    invalid_rows = []
    for row in rows:
        for field_name, valid_values in ref_dict.items():
            if field_name in column_names:
                field_index = column_names.index(field_name)
                if row[field_index] not in valid_values:
                    invalid_row = dict(zip(column_names, row))
                    invalid_row['invalid_value'] = row[field_index] 
                    invalid_rows.append(invalid_row)
                    break  

    return invalid_rows

def convert_to_serializable(data):
    if isinstance(data, datetime):
        return data.isoformat()
    elif isinstance(data, (date, time)):
        return data.isoformat()
    elif isinstance(data, decimal.Decimal):
        return float(data)
    return data

def log_invalid_rows(conn_params, invalid_rows, check_id, table_name, check_initiator, check_type):
    if not invalid_rows:
        return  

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        cursor.execute('SELECT COALESCE(MAX(row_id), 0) + 1 FROM "etl_sys"."Result_full";')
        max_row_id = cursor.fetchone()[0]

        for invalid_row in invalid_rows:
            
            invalid_row_serializable = {k: convert_to_serializable(v) for k, v in invalid_row.items()}
            invalid_row_json = json.dumps(invalid_row_serializable)

            print(f"Inserting row: check_id={check_id}, table_name={table_name}, check_initiator={check_initiator}, check_type={check_type}, row_data={invalid_row_json}")

            insert_query = """
            INSERT INTO "etl_sys"."Result_full" (row_id, check_id, check_type, check_table, row_data, check_initiator, check_date) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (max_row_id, check_id, check_type, table_name, invalid_row_json, check_initiator, datetime.now(moscow_tz)))

            max_row_id += 1

        conn.commit()
        print("Данные успешно вставлены.")

    except Exception as e:
        print(f"Ошибка при вставке данных: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

def check_attribute_values(conn_params, table_name, check_initiator):
    conn = None
    cursor = None
    invalid_rows = []
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        ref_dict = get_reference_values(cursor)

        invalid_rows = fetch_invalid_rows(cursor, table_name, ref_dict)

        count_invalid_rows = len(invalid_rows)
        print(f"Найдено строк с некорректными значениями атрибутов: {count_invalid_rows}")

        check_id = log_check_result(conn_params, 'ATTRIBUTE_VALUE_CHECK', table_name, count_invalid_rows, check_initiator)

        if check_id is None:
            raise ValueError("Не удалось получить check_id!")

        log_invalid_rows(conn_params, invalid_rows, check_id, table_name, check_initiator, 'ATTRIBUTE_VALUE_CHECK')

        return count_invalid_rows, invalid_rows

    except Exception as e:
        print(f"Ошибка при проверке значений атрибутов в таблице {table_name}: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def check_table_for_attribute_values(table_name, check_initiator):
    conn_id = 'my_postgres_conn'
    conn = BaseHook.get_connection(conn_id)

    conn_params = {
        'host': conn.host,
        'dbname': conn.schema,
        'user': conn.login,
        'password': conn.password
    }

    check_attribute_values(conn_params, table_name, check_initiator)

default_args = {
    'owner': 'Nick',
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
}

with DAG('check_attribute_values_dag', default_args=default_args, schedule_interval='@daily') as dag:
    check_table_task = PythonOperator(
        task_id='check_attribute_values',
        python_callable=check_table_for_attribute_values,
        op_kwargs={'table_name': '"Practic"."cash_flow_fact_ins_contract"', 'check_initiator': 'Nick'},
    )

    check_table_task
