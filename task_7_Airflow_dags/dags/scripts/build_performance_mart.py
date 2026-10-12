"""
Функции построения витрины dmr.analytics_student_performance для Airflow.

Это тот же алгоритм, что и в task_6_Data_marts/main.py, разбитый на отдельные
функции. Каждая функция - отдельная задача (task) в DAG
create_analytics_student_performance_mart:

    drop_mart -> create_schema -> create_table -> fill_mart -> check_mart

Параметры подключения берутся из переменных окружения контейнера Airflow
(они задаются в docker-compose.yaml из файла .env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

Как считаются показатели (в public.user_logs одна строка = одна неделя семестра):
  * total_events, total_course_views, total_quiz_views, total_module_views,
    total_submissions - суммы по неделям;
  * avg_weekly_events - среднее s_all по неделям;
  * peak_activity_week - неделя с максимальным s_all (при равенстве - более ранняя);
  * consistency_score = 1 - CV / sqrt(n - 1), где CV = стандартное отклонение /
    среднее по неделям, n - число недель (от 0 до 1: 1 - активность равномерна,
    0 - вся активность в одну неделю);
  * activity_category: меньше ACTIVITY_LOW_MAX - «низкая», от ACTIVITY_LOW_MAX до
    ACTIVITY_HIGH_MIN - «средняя», от ACTIVITY_HIGH_MIN и выше - «высокая»;
  * education_level: 1 - бакалавриат, 2 - магистратура, 3 - специалитет,
    4 - аспирантура; education_base: 1 - бюджет, 2 - контракт.
"""

import os

import psycopg2

SCHEMA_NAME = "dmr"
TABLE_NAME = "dmr.analytics_student_performance"

# Пороги категорий активности (среднее число событий в неделю)
ACTIVITY_LOW_MAX = 2.0    # меньше -> «низкая»
ACTIVITY_HIGH_MIN = 8.0   # от этого значения и выше -> «высокая»


# ------------------------------------------------------- подключение к БД

def get_db_config():
    """Читает параметры подключения из переменных окружения."""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", "user_logs_db"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
    }


def get_connection():
    """Открывает соединение с БД (пароль в лог не выводится)."""
    config = get_db_config()
    print(f"Подключение: {config['user']}@{config['host']}:{config['port']}/{config['database']}")
    conn = psycopg2.connect(**config)
    conn.autocommit = False
    return conn


def _run(callback):
    """Выполняет callback(conn) в отдельном соединении; при ошибке откатывает транзакцию."""
    conn = get_connection()
    try:
        result = callback(conn)
        conn.commit()
        return result
    except Exception as e:
        conn.rollback()
        print(f"Ошибка: {e}")
        raise
    finally:
        conn.close()


# ---------------------------------------------------- шаги (задачи DAG)

def drop_mart():
    """Шаг 1. Удаляет витрину, если она существует (перед каждым пересозданием)."""
    def work(conn):
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
        print(f"Витрина {TABLE_NAME} удалена (если существовала).")
    _run(work)


def create_schema():
    """Шаг 2. Создает схему dmr, если ее еще нет."""
    def work(conn):
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
        print(f"Схема {SCHEMA_NAME} создана (или уже существовала).")
    _run(work)


def create_table():
    """Шаг 3. Создает таблицу витрины с заданной структурой."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        student_id          INTEGER NOT NULL,
        course_id           INTEGER NOT NULL,
        department_id       INTEGER,
        department_name     VARCHAR(255),
        education_level     VARCHAR(50),
        education_base      VARCHAR(50),
        semester            INTEGER,
        course_year         INTEGER,
        final_grade         INTEGER CHECK (final_grade IN (2, 3, 4, 5)),
        total_events        INTEGER,
        avg_weekly_events   DECIMAL(10, 2),
        total_course_views  INTEGER,
        total_quiz_views    INTEGER,
        total_module_views  INTEGER,
        total_submissions   INTEGER,
        peak_activity_week  INTEGER,
        consistency_score   DECIMAL(5, 2) CHECK (consistency_score BETWEEN 0 AND 1),
        activity_category   VARCHAR(20)
                            CHECK (activity_category IN ('низкая', 'средняя', 'высокая')),
        last_update         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (student_id, course_id)
    );
    """
    def work(conn):
        with conn.cursor() as cur:
            cur.execute(create_table_query)
        print(f"Таблица {TABLE_NAME} создана.")
    _run(work)


def fill_mart():
    """Шаг 4. Заполняет витрину агрегатами из public.user_logs и public.departments."""
    insert_query = f"""
    WITH base AS (
        -- Недельные логи. В зависимости от скрипта инициализации БД коды в user_logs
        -- могут храниться как текст (VARCHAR) или как числа (INTEGER), поэтому
        -- каждое значение сначала приводится к тексту, а затем к числу.
        SELECT
            userid,
            courseid,
            num_week,
            COALESCE(NULLIF(TRIM(s_all::TEXT), '')::NUMERIC, 0)                         AS s_all,
            COALESCE(NULLIF(TRIM(s_course_viewed::TEXT), '')::NUMERIC, 0)               AS s_course_viewed,
            COALESCE(NULLIF(TRIM(s_q_attempt_viewed::TEXT), '')::NUMERIC, 0)            AS s_q_attempt_viewed,
            COALESCE(NULLIF(TRIM(s_a_course_module_viewed::TEXT), '')::NUMERIC, 0)      AS s_a_course_module_viewed,
            COALESCE(NULLIF(TRIM(s_a_submission_status_viewed::TEXT), '')::NUMERIC, 0)  AS s_a_submission_status_viewed,
            NULLIF(TRIM(depart::TEXT), '')::INTEGER       AS department_id,
            NULLIF(TRIM(leveled::TEXT), '')::INTEGER      AS level_code,
            NULLIF(TRIM(name_osno::TEXT), '')::INTEGER    AS base_code,
            NULLIF(TRIM(num_sem::TEXT), '')::INTEGER      AS num_sem,
            NULLIF(TRIM(kurs::TEXT), '')::INTEGER         AS kurs,
            NULLIF(TRIM(namer_level::TEXT), '')::INTEGER  AS grade
        FROM public.user_logs
        WHERE NULLIF(TRIM(namer_level::TEXT), '') IS NOT NULL
    ),
    agg AS (
        SELECT
            userid,
            courseid,
            MAX(department_id)  AS department_id,
            MAX(level_code)     AS level_code,
            MAX(base_code)      AS base_code,
            MAX(num_sem)        AS semester,
            MAX(kurs)           AS course_year,
            MAX(grade)          AS final_grade,
            COUNT(*)            AS weeks,
            SUM(s_all)                        AS total_events,
            AVG(s_all)                        AS avg_events,
            STDDEV_POP(s_all)                 AS std_events,
            SUM(s_course_viewed)              AS total_course_views,
            SUM(s_q_attempt_viewed)           AS total_quiz_views,
            SUM(s_a_course_module_viewed)     AS total_module_views,
            SUM(s_a_submission_status_viewed) AS total_submissions
        FROM base
        GROUP BY userid, courseid
    ),
    peak AS (
        -- неделя с максимальным числом событий (при равенстве - более ранняя)
        SELECT DISTINCT ON (userid, courseid)
            userid, courseid, num_week AS peak_week
        FROM base
        ORDER BY userid, courseid, s_all DESC, num_week ASC
    )
    INSERT INTO {TABLE_NAME} (
        student_id, course_id, department_id, department_name,
        education_level, education_base, semester, course_year, final_grade,
        total_events, avg_weekly_events, total_course_views, total_quiz_views,
        total_module_views, total_submissions, peak_activity_week,
        consistency_score, activity_category
    )
    SELECT
        a.userid,
        a.courseid,
        a.department_id,
        d.name,
        CASE a.level_code
            WHEN 1 THEN 'бакалавриат'
            WHEN 2 THEN 'магистратура'
            WHEN 3 THEN 'специалитет'
            WHEN 4 THEN 'аспирантура'
        END,
        CASE a.base_code
            WHEN 1 THEN 'бюджет'
            WHEN 2 THEN 'контракт'
        END,
        a.semester,
        a.course_year,
        a.final_grade,
        a.total_events,
        ROUND(a.avg_events, 2),
        a.total_course_views,
        a.total_quiz_views,
        a.total_module_views,
        a.total_submissions,
        CASE WHEN a.total_events > 0 THEN p.peak_week END,
        CASE
            WHEN a.weeks > 1 AND a.avg_events > 0
                THEN ROUND(GREATEST(0, 1 - (a.std_events / a.avg_events) / SQRT((a.weeks - 1)::NUMERIC)), 2)
            ELSE 0
        END,
        CASE
            WHEN a.avg_events < %(low_max)s  THEN 'низкая'
            WHEN a.avg_events < %(high_min)s THEN 'средняя'
            ELSE 'высокая'
        END
    FROM agg a
    JOIN peak p ON p.userid = a.userid AND p.courseid = a.courseid
    LEFT JOIN public.departments d ON d.id = a.department_id
    WHERE a.final_grade IN (2, 3, 4, 5)
    ON CONFLICT (student_id, course_id) DO UPDATE SET
        department_id      = EXCLUDED.department_id,
        department_name    = EXCLUDED.department_name,
        education_level    = EXCLUDED.education_level,
        education_base     = EXCLUDED.education_base,
        semester           = EXCLUDED.semester,
        course_year        = EXCLUDED.course_year,
        final_grade        = EXCLUDED.final_grade,
        total_events       = EXCLUDED.total_events,
        avg_weekly_events  = EXCLUDED.avg_weekly_events,
        total_course_views = EXCLUDED.total_course_views,
        total_quiz_views   = EXCLUDED.total_quiz_views,
        total_module_views = EXCLUDED.total_module_views,
        total_submissions  = EXCLUDED.total_submissions,
        peak_activity_week = EXCLUDED.peak_activity_week,
        consistency_score  = EXCLUDED.consistency_score,
        activity_category  = EXCLUDED.activity_category,
        last_update        = CURRENT_TIMESTAMP;
    """
    def work(conn):
        with conn.cursor() as cur:
            for table in ("public.user_logs", "public.departments"):
                cur.execute("SELECT to_regclass(%s);", (table,))
                if cur.fetchone()[0] is None:
                    raise RuntimeError(f"Таблица {table} не найдена в БД: сначала подготовьте исходные данные.")
            cur.execute(insert_query, {"low_max": ACTIVITY_LOW_MAX, "high_min": ACTIVITY_HIGH_MIN})
            print(f"Витрина заполнена. Добавлено записей: {cur.rowcount}")
    _run(work)


def check_mart():
    """Шаг 5. Проверяет витрину: выводит статистику, при пустой витрине падает с ошибкой."""
    def work(conn):
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
            total = cur.fetchone()[0]
            print(f"Всего записей в {TABLE_NAME}: {total}")
            if total == 0:
                raise RuntimeError("Витрина пуста.")

            cur.execute(f"""
                SELECT activity_category, COUNT(*), ROUND(AVG(avg_weekly_events), 2)
                FROM {TABLE_NAME}
                GROUP BY activity_category
                ORDER BY MIN(avg_weekly_events);
            """)
            print("Категории активности (категория, записей, среднее событий в неделю):")
            for row in cur.fetchall():
                print("  ", row)

            cur.execute(f"""
                SELECT student_id, course_id, department_name, education_level, education_base,
                       final_grade, total_events, avg_weekly_events, peak_activity_week,
                       consistency_score, activity_category
                FROM {TABLE_NAME}
                ORDER BY student_id, course_id
                LIMIT 5;
            """)
            print("Первые 5 записей:")
            for row in cur.fetchall():
                print("  ", row)
    _run(work)


if __name__ == "__main__":
    # Для локального теста без Airflow: последовательно выполняем все шаги
    drop_mart()
    create_schema()
    create_table()
    fill_mart()
    check_mart()
