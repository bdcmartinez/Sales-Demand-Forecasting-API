from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.db_connection import get_db_connection

from pendulum import timezone

mex_tz = timezone("America/Mexico_City")

def call_function_one():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT SP_DELETE_NULL_SALES_QT();")
    conn.commit()
    cur.close()
    conn.close()

def call_function_two():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT SP_DELETE_DUPLICATES_SALES();")
    conn.commit()
    cur.close()
    conn.close()
    
def call_function_three():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT SP_DELETE_OUTLIERS_SALES();")
    conn.commit()
    cur.close()
    conn.close()

    

with DAG(
    "CLEAN_DATA",
    schedule_interval="35 7 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=mex_tz),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="DELETING_NULL_VALUES_IN_SALES",
        python_callable=call_function_one
    )

    task2 = PythonOperator(
        task_id="DELETING_DUPLICATE_VALUES",
        python_callable=call_function_two
    )
    
    task3 = PythonOperator(
    task_id="DELETING_OUTLIERS_VALUES",
    python_callable=call_function_three
    )

    task1 >> task2 >> task3   # Runs function_two only after function_one finishes
