"""
Витрина данных dmr.analytics_student_performance (Data Mart Repository).

Что делает скрипт:
  1. создает схему dmr, если ее еще нет;
  2. создает витрину dmr.analytics_student_performance (одна строка = студент + курс);
  3. заполняет ее агрегатами из public.user_logs (логи портала по неделям)
     и справочником кафедр public.departments;
  4. выводит краткую проверку результата.

Каждый шаг реализован отдельной функцией. Скрипт можно запускать повторно:
записи обновляются по ключу (student_id, course_id), поле last_update
при этом получает новое значение.

Структура витрины:
    student_id         INTEGER        ID студента
    course_id          INTEGER        ID курса
    department_id      INTEGER        код кафедры
    department_name    VARCHAR        название кафедры
    education_level    VARCHAR        уровень образования
    education_base     VARCHAR        основа обучения
    semester           INTEGER        номер семестра
    course_year        INTEGER        курс обучения
    final_grade        INTEGER        итоговая оценка
    total_events       INTEGER        всего событий за семестр
    avg_weekly_events  DECIMAL(10,2)  среднее событий в неделю
    total_course_views INTEGER        всего просмотров курса
    total_quiz_views   INTEGER        всего просмотров тестов
    total_module_views INTEGER        всего просмотров модулей
    total_submissions  INTEGER        всего отправленных заданий
    peak_activity_week INTEGER        неделя с максимальной активностью
    consistency_score  DECIMAL(5,2)   коэффициент стабильности активности (0-1)
    activity_category  VARCHAR        категория активности (низкая/средняя/высокая)
    last_update        TIMESTAMP      дата обновления записи

Как считаются показатели (в public.user_logs одна строка = одна неделя семестра):
  * total_events, total_course_views, total_quiz_views, total_module_views,
    total_submissions - суммы по неделям столбцов s_all, s_course_viewed,
    s_q_attempt_viewed, s_a_course_module_viewed, s_a_submission_status_viewed;
  * avg_weekly_events - среднее значение s_all по неделям семестра;
  * peak_activity_week - неделя с максимальным s_all (при равенстве берется
    более ранняя); если активности не было совсем, значение NULL;
  * consistency_score = 1 - CV / sqrt(n - 1), где CV = стандартное отклонение /
    среднее число событий по неделям, n - число недель. Для неотрицательных
    данных CV не превышает sqrt(n - 1), поэтому коэффициент лежит в диапазоне
    от 0 до 1: 1 - активность распределена по неделям равномерно,
    0 - вся активность сосредоточена в одной неделе (или активности нет);
  * activity_category по avg_weekly_events: меньше ACTIVITY_LOW_MAX - «низкая»,
    от ACTIVITY_LOW_MAX до ACTIVITY_HIGH_MIN - «средняя», от
    ACTIVITY_HIGH_MIN и выше - «высокая» (пороги заданы константами ниже и
    примерно делят студентов на три равные группы);
  * education_level: 1 - бакалавриат, 2 - магистратура, 3 - специалитет,
    4 - аспирантура; education_base: 1 - бюджет, 2 - контракт.
"""

import os
import sys

import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------- настройки

SCHEMA_NAME = "dmr"
TABLE_NAME = "dmr.analytics_student_performance"

# Пороги категорий активности (среднее число событий в неделю)
ACTIVITY_LOW_MAX = 2.0    # меньше -> «низкая»
ACTIVITY_HIGH_MIN = 8.0   # от этого значения и выше -> «высокая»

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOCKER_ENV_PATH = os.path.join(BASE_DIR, "..", "task_1_Docker", ".env")
DEPARTMENTS_CSV = os.path.join(BASE_DIR, "..", "task_1_Docker", "datasets", "departments.csv")


# ------------------------------------------------------- подключение к БД

def get_db_config():
    """Формирует словарь с параметрами подключения к БД.

    Параметры берутся из .env в папке task_1_Docker (там же лежит настройка
    контейнера с PostgreSQL), затем из локального .env, затем из переменных
    окружения.
    """
    # override=True: значения из .env важнее системных переменных
    # (например, USER в Linux - это имя пользователя ОС, а не БД)
    load_dotenv(DOCKER_ENV_PATH, override=True)
    load_dotenv()
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB", "educational_portal"),
        "user": os.getenv("DB_USER") or os.getenv("USER", "postgres"),
        "password": os.getenv("PASSWORD", ""),
    }


def get_connection():
    """Устанавливает и возвращает соединение с БД."""
    config = get_db_config()
    try:
        conn = psycopg2.connect(**config)
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)
    conn.autocommit = False
    # пароль в вывод не попадает
    print(f"Подключение: {config['user']}@{config['host']}:{config['port']}/{config['database']}")
    return conn


# --------------------------------------------------------- шаги построения

def create_schema(conn):
    """Создает схему dmr, если она еще не существует."""
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
    conn.commit()
    print(f"Схема {SCHEMA_NAME} создана (или уже существовала).")


def create_table(conn):
    """Создает таблицу витрины dmr.analytics_student_performance."""
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
    with conn.cursor() as cur:
        cur.execute(create_table_query)
    conn.commit()
    print(f"Таблица {TABLE_NAME} создана (или уже существовала).")


def ensure_departments(conn):
    """Проверяет наличие справочника кафедр public.departments.

    Скрипт инициализации Docker создает только user_logs, поэтому если
    справочника в БД нет, он создается и загружается из departments.csv.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.departments');")
        if cur.fetchone()[0] is not None:
            print("Справочник public.departments уже есть в БД.")
            return

        cur.execute(
            "CREATE TABLE public.departments (id INTEGER PRIMARY KEY, name VARCHAR(255));"
        )
        if os.path.exists(DEPARTMENTS_CSV):
            with open(DEPARTMENTS_CSV, encoding="utf-8") as f:
                cur.copy_expert(
                    "COPY public.departments (id, name) FROM STDIN WITH (FORMAT csv, HEADER true)",
                    f,
                )
            print("Справочник public.departments создан и загружен из departments.csv.")
        else:
            print(f"Файл {DEPARTMENTS_CSV} не найден: public.departments создан пустым, "
                  "department_name будет NULL.")
    conn.commit()


def refresh_mart(conn):
    """Заполняет (или обновляет) витрину данными из public.user_logs."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.user_logs');")
        if cur.fetchone()[0] is None:
            raise RuntimeError("Таблица public.user_logs не найдена: сначала поднимите БД (task_1_Docker).")

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
    with conn.cursor() as cur:
        cur.execute(insert_query, {"low_max": ACTIVITY_LOW_MAX, "high_min": ACTIVITY_HIGH_MIN})
        affected = cur.rowcount
    conn.commit()
    print(f"Витрина заполнена. Добавлено/обновлено записей: {affected}")


def check_mart(conn):
    """Выводит краткую проверку содержимого витрины."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
        print(f"\nВсего записей в {TABLE_NAME}: {cur.fetchone()[0]}")

        cur.execute(f"""
            SELECT activity_category, COUNT(*), ROUND(AVG(avg_weekly_events), 2)
            FROM {TABLE_NAME}
            GROUP BY activity_category
            ORDER BY MIN(avg_weekly_events);
        """)
        print("\nКатегории активности (категория, студентов-курсов, среднее событий в неделю):")
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
        print("\nПервые 5 записей:")
        for row in cur.fetchall():
            print("  ", row)


def main():
    """Последовательное выполнение шагов."""
    conn = None
    try:
        conn = get_connection()
        create_schema(conn)
        create_table(conn)
        ensure_departments(conn)
        refresh_mart(conn)
        check_mart(conn)
        print("\nВсе операции выполнены успешно!")
    except Exception as e:
        print(f"Ошибка в процессе выполнения: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("Соединение с БД закрыто.")


if __name__ == "__main__":
    main()
