#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    -- Создаем справочник подразделений
    CREATE TABLE departments (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255)
    );

    -- Загружаем справочник подразделений
    \copy departments FROM '/datasets/departments.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

    -- Создаем таблицу для логов
    CREATE TABLE user_logs (
        courseid INTEGER,
        userid INTEGER,
        num_week INTEGER,
        s_all INTEGER,
        s_all_avg VARCHAR(255),
        s_course_viewed INTEGER,
        s_course_viewed_avg VARCHAR(255),
        s_q_attempt_viewed INTEGER,
        s_q_attempt_viewed_avg VARCHAR(255),
        s_a_course_module_viewed INTEGER,
        s_a_course_module_viewed_avg VARCHAR(255),
        s_a_submission_status_viewed INTEGER,
        s_a_submission_status_viewed_avg VARCHAR(255),
        NameR_Level VARCHAR(255),
        Name_vAtt VARCHAR(255),
        Depart INTEGER,
        Name_OsnO VARCHAR(255),
        Name_FormOPril VARCHAR(255),
        LevelEd VARCHAR(255),
        Num_Sem INTEGER,
        Kurs INTEGER,
        Date_vAtt VARCHAR(255)
    );

    -- Загружаем логи
    \copy user_logs FROM '/datasets/aggrigation_logs_per_week.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

EOSQL