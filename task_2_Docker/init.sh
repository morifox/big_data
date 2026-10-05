#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    CREATE TABLE test_table (
        id SERIAL PRIMARY KEY,
        message VARCHAR(255)
    );

    INSERT INTO test_table (message)
    VALUES ('Docker PostgreSQL task 2');

EOSQL