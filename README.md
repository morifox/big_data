# Большие данные — лабораторные работы

Чикишев Степан Максимович, группа АПИб-23-1, МГТУ им. Г.И. Носова.

В каждой папке лежат файлы работы и краткий отчет в Word о том, как она выполнялась.

| Папка | Работа | Основные файлы |
|---|---|---|
| `task1`, `task_2_Docker` | PostgreSQL в Docker, загрузка логов портала | `docker-compose.yml`, `init.sh`, `datasets/` |
| `task_4_DBeaver_jupiter` | Анализ логов портала SQL-запросами из Jupyter | `postGre_portal_logs.ipynb` |
| `task_5_Visual_analysis` | Визуализация логов в Plotly | `Visualization_logs.ipynb` |
| `task_6_Data_marts` | Витрина `dmr.analytics_student_performance` | `main.py` |
| `task_7_Airflow_dags` | Airflow: DAG для пересоздания витрины | `dags/create_performance_mart_dag.py`, `dags/scripts/build_performance_mart.py` |
| `task_8_Ontology` | Онтология специалистов ML, задания 1–4 | `Ontology_mindmap.ipynb`, `ml_specialist_v1…v5.owl` |
| `task_9_Sintetic_data` | Синтетические данные (Faker), онтология и онтограф, вариант 4 | `generate.ipynb`, CSV, `student_performance.owl`, онтографы |
| `task_10_Student_performance_mart` | Витрины успеваемости студентов в Airflow, вариант 4, визуализация | `docker-compose.yaml`, `dags/`, `visualization.ipynb` |
