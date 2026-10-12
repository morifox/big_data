# dags/student_performance_dag.py
"""
DAG: витрины успеваемости студентов (вариант 4).

    generate_data -> load_raw -> transform -> create_mart

generate_data - генерация групп, студентов, предметов, преподавателей, оценок (CSV);
load_raw      - загрузка CSV в схему raw;
transform     - средний балл, задолженности, право на стипендию, агрегация по группам (схема stg);
create_mart   - витрины mart.student_performance и mart.group_stats.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from student_performance_pipeline import generate_data, load_raw, transform, create_mart

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "student_performance_mart",
    default_args=default_args,
    description="Витрины mart.student_performance и mart.group_stats",
    schedule="0 4 * * *",           # каждый день в 4:00
    catchup=False,
    tags=["mart", "students", "variant_4"],
) as dag:

    generate_data_task = PythonOperator(task_id="generate_data", python_callable=generate_data)
    load_raw_task = PythonOperator(task_id="load_raw", python_callable=load_raw)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    create_mart_task = PythonOperator(task_id="create_mart", python_callable=create_mart)

    generate_data_task >> load_raw_task >> transform_task >> create_mart_task
