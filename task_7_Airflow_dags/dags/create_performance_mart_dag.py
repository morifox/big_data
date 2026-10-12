# dags/create_performance_mart_dag.py
"""
DAG пересоздает витрину dmr.analytics_student_performance (задание 6).

Порядок задач:
    drop_mart -> create_schema -> create_table -> fill_mart -> check_mart

1. drop_mart      - удаляет витрину, если она существует;
2. create_schema  - создает схему dmr (если ее нет);
3. create_table   - создает таблицу витрины;
4. fill_mart      - заполняет витрину агрегатами из public.user_logs;
5. check_mart     - проверяет результат и выводит статистику в лог.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь к папке scripts в PYTHONPATH, чтобы импорт работал
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))

from build_performance_mart import (
    drop_mart,
    create_schema,
    create_table,
    fill_mart,
    check_mart,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'create_analytics_student_performance_mart',
    default_args=default_args,
    description='Пересоздание витрины dmr.analytics_student_performance',
    schedule='0 3 * * *',   # каждый день в 3:00
    catchup=False,
    tags=['mart', 'student_performance'],
) as dag:

    drop_mart_task = PythonOperator(
        task_id='drop_mart',
        python_callable=drop_mart,
    )

    create_schema_task = PythonOperator(
        task_id='create_schema',
        python_callable=create_schema,
    )

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    fill_mart_task = PythonOperator(
        task_id='fill_mart',
        python_callable=fill_mart,
    )

    check_mart_task = PythonOperator(
        task_id='check_mart',
        python_callable=check_mart,
    )

    drop_mart_task >> create_schema_task >> create_table_task >> fill_mart_task >> check_mart_task
