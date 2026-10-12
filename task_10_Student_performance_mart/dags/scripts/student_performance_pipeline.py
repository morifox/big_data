# dags/scripts/student_performance_pipeline.py
"""
Вариант 4. Студенты и успеваемость.
Пайплайн витрин mart.student_performance и mart.group_stats.

Функции (каждая - отдельная задача DAG):
    generate_data - генерация групп, студентов, предметов, преподавателей, оценок -> CSV
    load_raw      - загрузка CSV в схему raw (PostgreSQL)
    transform     - расчет показателей в схеме stg:
                    средний балл, задолженности, право на стипендию, агрегаты по группам
    create_mart   - создание витрин в схеме mart и проверка результата

Правила расчета:
    * средний балл считается только по экзаменам (оценки 2-5), зачёты в числовое среднее не входят;
    * положительный результат: оценка >= 3 или «зачёт»;
    * задолженность: оценка 2, «незачёт» или отсутствие оценки по предмету уже завершенного семестра;
    * стипендия: нет задолженностей и средний балл >= 4.0.

Данные генерируются на момент окончания весенней сессии 2025/26 учебного года:
у группы курса N завершены семестры 1..2N.
"""
import csv
import os
import random
from datetime import date, timedelta

import psycopg2

# ----------------------------------------------------------------------------
# Настройки
# ----------------------------------------------------------------------------
N_GROUPS, STUDENTS_PER_GROUP, N_TEACHERS = 10, 25, 12
SCHOLARSHIP_THRESHOLD = 4.0
MISSING_GRADE_SHARE = 0.01          # доля неявок (оценка отсутствует)
SEED = 2026
ACADEMIC_YEAR_END = 2026            # весенняя сессия 2025/26 учебного года

TABLES = ["teachers", "groups", "subjects", "students", "group_subjects", "grades"]


def get_data_dir():
    """Папка для CSV: переменная STUDENT_DATA_DIR (в Docker - папка ./data проекта) или dags/data."""
    path = os.getenv("STUDENT_DATA_DIR") or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "student_performance")
    try:
        os.makedirs(path, exist_ok=True)
        test = os.path.join(path, ".write_test")
        open(test, "w").close()
        os.remove(test)
    except OSError:                               # нет прав на запись - пишем во временную папку
        path = "/tmp/student_performance_data"
        os.makedirs(path, exist_ok=True)
    return path


def get_db_config():
    """Параметры подключения к базе students_db (из переменных окружения docker-compose)."""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5435"),
        "dbname": os.getenv("DB_NAME", "students_db"),
        "user": os.getenv("DB_USER", "student"),
        "password": os.getenv("DB_PASSWORD", "student"),
    }


def run_sql(sql_text, fetch=False):
    conn = psycopg2.connect(**get_db_config())
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql_text)
            return cur.fetchall() if fetch else None
    finally:
        conn.close()


# ----------------------------------------------------------------------------
# 1. generate_data
# ----------------------------------------------------------------------------
INSTITUTES = {
    "Институт энергетики и автоматизированных систем": [
        ("09.03.03 Прикладная информатика", "АПИб"), ("09.03.01 Информатика и вычислительная техника", "ИВТб")],
    "Институт металлургии, машиностроения и материалообработки": [
        ("22.03.02 Металлургия", "МТб"), ("15.03.01 Машиностроение", "МШб")],
    "Институт строительства, архитектуры и искусства": [
        ("08.03.01 Строительство", "СТб"), ("07.03.01 Архитектура", "АРб")],
}
DEPARTMENTS = ["Кафедра информатики и информационной безопасности", "Кафедра вычислительной техники и программирования",
               "Кафедра математики", "Кафедра физики", "Кафедра иностранных языков",
               "Кафедра экономики", "Кафедра философии и истории"]
SUBJECTS = [  # (название, семестр)
    ("Высшая математика", 1), ("Программирование на Python", 1), ("История России", 1), ("Английский язык", 1),
    ("Линейная алгебра", 2), ("Физика", 2), ("Дискретная математика", 2), ("Философия", 2),
    ("Алгоритмы и структуры данных", 3), ("Теория вероятностей и статистика", 3), ("Экономика", 3), ("Физическая культура", 3),
    ("Базы данных", 4), ("Операционные системы", 4), ("Объектно-ориентированное программирование", 4), ("Социология", 4),
    ("Компьютерные сети", 5), ("Web-технологии", 5), ("Психология", 5), ("Правоведение", 5),
    ("Проектирование информационных систем", 6), ("Тестирование ПО", 6), ("Культурология", 6), ("Безопасность жизнедеятельности", 6),
    ("Информационная безопасность", 7), ("Большие данные", 7), ("Менеджмент проектов", 7),
    ("Машинное обучение", 8), ("Основы искусственного интеллекта", 8), ("Основы бизнеса", 8),
]

# Запасные списки ФИО, если библиотека Faker не установлена
_M_LAST = ["Иванов", "Петров", "Смирнов", "Кузнецов", "Попов", "Соколов", "Лебедев", "Козлов", "Новиков", "Морозов",
           "Волков", "Алексеев", "Федоров", "Михайлов", "Николаев", "Орлов", "Андреев", "Макаров", "Захаров", "Зайцев"]
_M_FIRST = ["Александр", "Дмитрий", "Максим", "Сергей", "Андрей", "Алексей", "Артём", "Илья", "Кирилл", "Михаил",
            "Никита", "Матвей", "Роман", "Егор", "Иван", "Степан", "Павел", "Денис", "Тимур", "Владимир"]
_F_FIRST = ["Анна", "Мария", "Елена", "Дарья", "Алина", "Ирина", "Екатерина", "Полина", "Ольга", "Татьяна",
            "Наталья", "Юлия", "Ксения", "Виктория", "Софья", "Алёна", "Вероника", "Арина", "Валерия", "Марина"]
_PATR = ["Александров", "Дмитриев", "Сергеев", "Андреев", "Алексеев", "Иванов", "Михайлов", "Николаев", "Петров", "Владимиров"]


def _person_generator(rnd):
    try:
        from faker import Faker
        fake = Faker("ru_RU")
        fake.seed_instance(SEED)

        def person(gender):
            if gender == "М":
                return fake.last_name_male(), fake.first_name_male(), fake.middle_name_male()
            return fake.last_name_female(), fake.first_name_female(), fake.middle_name_female()
        print("ФИО генерируются библиотекой Faker (ru_RU)")
    except ImportError:
        def person(gender):
            last, patr = rnd.choice(_M_LAST), rnd.choice(_PATR)
            if gender == "М":
                return last, rnd.choice(_M_FIRST), patr + "ич"
            return last + "а", rnd.choice(_F_FIRST), patr + "на"
        print("Faker не установлен - ФИО генерируются из встроенных списков")
    return person


def _session_date(rnd, admission_year, semester):
    """Дата оценки: зимняя сессия (январь) для нечетных семестров, летняя (июнь) для четных."""
    if semester % 2 == 1:
        start = date(admission_year + (semester + 1) // 2, 1, 9)
    else:
        start = date(admission_year + semester // 2, 6, 1)
    return start + timedelta(days=rnd.randint(0, 24))


def _write_csv(data_dir, name, rows):
    path = os.path.join(data_dir, f"{name}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  {name}.csv: {len(rows)} строк")


def generate_data():
    rnd = random.Random(SEED)
    person = _person_generator(rnd)
    data_dir = get_data_dir()
    print(f"Папка для данных: {data_dir}")

    teachers = []
    for i in range(1, N_TEACHERS + 1):
        last, first, patr = person(rnd.choice(["М", "Ж"]))
        teachers.append({"teacher_id": i, "full_name": f"{last} {first} {patr}",
                         "department": DEPARTMENTS[(i - 1) % len(DEPARTMENTS)]})

    groups = []
    for i in range(1, N_GROUPS + 1):
        institute = list(INSTITUTES)[(i - 1) % len(INSTITUTES)]
        direction, prefix = rnd.choice(INSTITUTES[institute])
        course = (i - 1) % 4 + 1
        admission = ACADEMIC_YEAR_END - course
        groups.append({"group_id": i, "group_number": f"{prefix}-{admission % 100}-{(i - 1) // 4 + 1}",
                       "course": course, "institute": institute, "direction": direction,
                       "admission_year": admission})

    subjects, per_semester = [], {}
    for i, (name, semester) in enumerate(SUBJECTS, 1):
        k = per_semester[semester] = per_semester.get(semester, 0) + 1
        subjects.append({"subject_id": i, "subject_name": name, "semester": semester,
                         "hours": rnd.choice([72, 108, 144, 180]),
                         # в каждом семестре два первых предмета - экзамены, остальные - зачёты
                         "control_form": "экзамен" if k <= 2 else "зачёт",
                         "teacher_id": rnd.randint(1, N_TEACHERS)})

    students, ability = [], {}
    for grp in groups:
        for _ in range(STUDENTS_PER_GROUP):
            sid = len(students) + 1
            gender = rnd.choice(["М", "Ж"])
            last, first, patr = person(gender)
            birth = date(grp["admission_year"] - 18, 1, 1) + timedelta(days=rnd.randint(0, 600))
            students.append({"student_id": sid, "full_name": f"{last} {first} {patr}", "gender": gender,
                             "birth_date": birth.isoformat(), "group_id": grp["group_id"],
                             "admission_year": grp["admission_year"],
                             "study_form": rnd.choices(["очная", "очно-заочная", "заочная"], [0.8, 0.12, 0.08])[0]})
            ability[sid] = rnd.gauss(0, 1)      # условный уровень подготовки студента

    # каждая группа изучает все предметы учебного плана
    group_subjects = [{"group_id": g["group_id"], "subject_id": s["subject_id"]} for g in groups for s in subjects]

    # оценки - только по предметам завершенных семестров (1..2*курс)
    grades = []
    group_by_id = {g["group_id"]: g for g in groups}
    for st in students:
        grp = group_by_id[st["group_id"]]
        for sb in subjects:
            if sb["semester"] > grp["course"] * 2:
                continue
            if rnd.random() < MISSING_GRADE_SHARE:        # неявка: оценки нет -> задолженность
                continue
            a = ability[st["student_id"]]
            if sb["control_form"] == "экзамен":
                x = a + rnd.gauss(0.35, 0.6)
                value = "5" if x > 0.9 else "4" if x > 0 else "3" if x > -1.6 else "2"
            else:
                value = "зачёт" if a + rnd.gauss(1.0, 0.5) > -0.7 else "незачёт"
            grades.append({"grade_id": len(grades) + 1, "student_id": st["student_id"],
                           "subject_id": sb["subject_id"],
                           "grade_date": _session_date(rnd, grp["admission_year"], sb["semester"]).isoformat(),
                           "grade": value})

    print("Сгенерированы файлы:")
    for name, rows in zip(TABLES, [teachers, groups, subjects, students, group_subjects, grades]):
        _write_csv(data_dir, name, rows)


# ----------------------------------------------------------------------------
# 2. load_raw
# ----------------------------------------------------------------------------
RAW_DDL = """
DROP SCHEMA IF EXISTS raw CASCADE;
CREATE SCHEMA raw;

CREATE TABLE raw.teachers (
    teacher_id  INTEGER PRIMARY KEY,
    full_name   TEXT NOT NULL,
    department  TEXT
);
CREATE TABLE raw.groups (
    group_id       INTEGER PRIMARY KEY,
    group_number   TEXT NOT NULL,
    course         INTEGER NOT NULL,
    institute      TEXT,
    direction      TEXT,
    admission_year INTEGER
);
CREATE TABLE raw.subjects (
    subject_id   INTEGER PRIMARY KEY,
    subject_name TEXT NOT NULL,
    semester     INTEGER NOT NULL,
    hours        INTEGER NOT NULL,
    control_form TEXT NOT NULL CHECK (control_form IN ('экзамен', 'зачёт')),
    teacher_id   INTEGER REFERENCES raw.teachers(teacher_id)
);
CREATE TABLE raw.students (
    student_id     INTEGER PRIMARY KEY,
    full_name      TEXT NOT NULL,
    gender         TEXT,
    birth_date     DATE,
    group_id       INTEGER REFERENCES raw.groups(group_id),
    admission_year INTEGER,
    study_form     TEXT
);
CREATE TABLE raw.group_subjects (
    group_id   INTEGER REFERENCES raw.groups(group_id),
    subject_id INTEGER REFERENCES raw.subjects(subject_id),
    PRIMARY KEY (group_id, subject_id)
);
CREATE TABLE raw.grades (
    grade_id   INTEGER PRIMARY KEY,
    student_id INTEGER REFERENCES raw.students(student_id),
    subject_id INTEGER REFERENCES raw.subjects(subject_id),
    grade_date DATE,
    grade      TEXT NOT NULL      -- '2'..'5', 'зачёт', 'незачёт'
);
"""


def load_raw():
    data_dir = get_data_dir()
    conn = psycopg2.connect(**get_db_config())
    try:
        with conn, conn.cursor() as cur:
            cur.execute(RAW_DDL)
            for name in TABLES:               # порядок важен из-за внешних ключей
                with open(os.path.join(data_dir, f"{name}.csv"), encoding="utf-8") as f:
                    cur.copy_expert(f"COPY raw.{name} FROM STDIN WITH (FORMAT csv, HEADER true)", f)
                cur.execute(f"SELECT COUNT(*) FROM raw.{name}")
                print(f"  raw.{name}: {cur.fetchone()[0]} строк")
    finally:
        conn.close()


# ----------------------------------------------------------------------------
# 3. transform
# ----------------------------------------------------------------------------
TRANSFORM_SQL = f"""
DROP SCHEMA IF EXISTS stg CASCADE;
CREATE SCHEMA stg;

-- Все предметы, которые студент должен был сдать (семестр уже завершен), и его оценка по ним
CREATE TABLE stg.student_subject AS
SELECT st.student_id,
       st.group_id,
       sb.subject_id,
       sb.subject_name,
       sb.semester,
       sb.hours,
       sb.control_form,
       g.grade,
       CASE WHEN g.grade IN ('2', '3', '4', '5') THEN g.grade::INTEGER END              AS grade_num,
       COALESCE(g.grade IN ('3', '4', '5', 'зачёт'), FALSE)                              AS is_passed
FROM raw.students st
JOIN raw.groups gr          ON gr.group_id = st.group_id
JOIN raw.group_subjects gs  ON gs.group_id = st.group_id
JOIN raw.subjects sb        ON sb.subject_id = gs.subject_id
LEFT JOIN raw.grades g      ON g.student_id = st.student_id AND g.subject_id = sb.subject_id
WHERE sb.semester <= gr.course * 2;

-- Показатели студента: средний балл, зачетные часы, задолженности, стипендия
CREATE TABLE stg.student_metrics AS
SELECT student_id,
       group_id,
       ROUND(AVG(grade_num)::NUMERIC, 2)                                 AS avg_grade,
       COALESCE(SUM(hours) FILTER (WHERE is_passed), 0)::INTEGER          AS total_credits,
       COUNT(*) FILTER (WHERE NOT is_passed)::INTEGER                     AS debt_count
FROM stg.student_subject
GROUP BY student_id, group_id;

ALTER TABLE stg.student_metrics ADD COLUMN scholarship_eligible BOOLEAN;
UPDATE stg.student_metrics
SET scholarship_eligible = (debt_count = 0 AND COALESCE(avg_grade, 0) >= {SCHOLARSHIP_THRESHOLD});

-- Средний балл группы по каждому экзаменационному предмету
CREATE TABLE stg.group_subject_avg AS
SELECT group_id, subject_id, subject_name, AVG(grade_num) AS avg_grade
FROM stg.student_subject
WHERE grade_num IS NOT NULL
GROUP BY group_id, subject_id, subject_name;

-- Агрегаты по группам
CREATE TABLE stg.group_metrics AS
WITH grp_avg AS (
    SELECT group_id, ROUND(AVG(grade_num)::NUMERIC, 2) AS avg_grade_group
    FROM stg.student_subject
    GROUP BY group_id
),
pass AS (
    SELECT group_id,
           ROUND(100.0 * COUNT(*) FILTER (WHERE debt_count = 0) / COUNT(*), 2) AS pass_rate
    FROM stg.student_metrics
    GROUP BY group_id
),
ranked AS (
    SELECT group_id, subject_name,
           ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY avg_grade DESC, subject_name) AS rn_best,
           ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY avg_grade ASC,  subject_name) AS rn_worst
    FROM stg.group_subject_avg
)
SELECT gr.group_id,
       gr.course,
       gr.institute,
       a.avg_grade_group,
       p.pass_rate,
       (SELECT subject_name FROM ranked r WHERE r.group_id = gr.group_id AND r.rn_best = 1)  AS best_subject,
       (SELECT subject_name FROM ranked r WHERE r.group_id = gr.group_id AND r.rn_worst = 1) AS worst_subject
FROM raw.groups gr
LEFT JOIN grp_avg a ON a.group_id = gr.group_id
LEFT JOIN pass p    ON p.group_id = gr.group_id;
"""


def transform():
    run_sql(TRANSFORM_SQL)
    for t in ["student_subject", "student_metrics", "group_subject_avg", "group_metrics"]:
        print(f"  stg.{t}: {run_sql(f'SELECT COUNT(*) FROM stg.{t}', fetch=True)[0][0]} строк")


# ----------------------------------------------------------------------------
# 4. create_mart
# ----------------------------------------------------------------------------
MART_SQL = """
CREATE SCHEMA IF NOT EXISTS mart;
DROP TABLE IF EXISTS mart.student_performance;
DROP TABLE IF EXISTS mart.group_stats;

CREATE TABLE mart.student_performance (
    student_id           INTEGER PRIMARY KEY,
    group_id             INTEGER NOT NULL,
    avg_grade            NUMERIC(4, 2),
    total_credits        INTEGER NOT NULL,
    debt_count           INTEGER NOT NULL,
    scholarship_eligible BOOLEAN NOT NULL
);
COMMENT ON TABLE  mart.student_performance IS 'Персональные показатели успеваемости студентов';
COMMENT ON COLUMN mart.student_performance.avg_grade IS 'Средний балл по экзаменам (зачёты в среднее не входят)';
COMMENT ON COLUMN mart.student_performance.total_credits IS 'Сумма часов по предметам с оценкой >= 3 или «зачёт»';
COMMENT ON COLUMN mart.student_performance.debt_count IS 'Число задолженностей: 2, «незачёт» или нет оценки за завершенный семестр';
COMMENT ON COLUMN mart.student_performance.scholarship_eligible IS 'debt_count = 0 и avg_grade >= 4.0';

INSERT INTO mart.student_performance
SELECT student_id, group_id, avg_grade, total_credits, debt_count, scholarship_eligible
FROM stg.student_metrics;

CREATE TABLE mart.group_stats (
    group_id        INTEGER PRIMARY KEY,
    course          INTEGER NOT NULL,
    institute       TEXT,
    avg_grade_group NUMERIC(4, 2),
    pass_rate       NUMERIC(5, 2),
    best_subject    TEXT,
    worst_subject   TEXT
);
COMMENT ON TABLE  mart.group_stats IS 'Агрегированные показатели успеваемости групп';
COMMENT ON COLUMN mart.group_stats.avg_grade_group IS 'Средний балл всех экзаменационных оценок студентов группы';
COMMENT ON COLUMN mart.group_stats.pass_rate IS 'Процент студентов группы без задолженностей';
COMMENT ON COLUMN mart.group_stats.best_subject IS 'Предмет с наибольшим средним баллом в группе';
COMMENT ON COLUMN mart.group_stats.worst_subject IS 'Предмет с наименьшим средним баллом в группе';

INSERT INTO mart.group_stats
SELECT group_id, course, institute, avg_grade_group, pass_rate, best_subject, worst_subject
FROM stg.group_metrics;
"""


def create_mart():
    run_sql(MART_SQL)
    n_st = run_sql("SELECT COUNT(*), COUNT(*) FILTER (WHERE scholarship_eligible), "
                   "COUNT(*) FILTER (WHERE debt_count > 0), ROUND(AVG(avg_grade), 2) "
                   "FROM mart.student_performance", fetch=True)[0]
    if n_st[0] == 0:
        raise ValueError("Витрина mart.student_performance пуста")
    print(f"mart.student_performance: {n_st[0]} студентов, право на стипендию: {n_st[1]}, "
          f"с задолженностями: {n_st[2]}, средний балл: {n_st[3]}")
    print("mart.group_stats:")
    for row in run_sql("SELECT group_id, course, avg_grade_group, pass_rate, best_subject, worst_subject "
                       "FROM mart.group_stats ORDER BY group_id", fetch=True):
        print("  ", row)


if __name__ == "__main__":       # запуск без Airflow: python student_performance_pipeline.py
    generate_data()
    load_raw()
    transform()
    create_mart()
