CREATE TABLE IF NOT EXISTS "{{ params.dm_schema_name }}".employees (
    empl_id INT PRIMARY KEY,
    last_name VARCHAR,
    first_name VARCHAR
);

TRUNCATE TABLE "{{ params.dm_schema_name }}".employees CASCADE;

INSERT INTO
"{{ params.dm_schema_name }}".employees (
    empl_id,
    last_name,
    first_name
)
SELECT
    id,
    last_name,
    first_name
FROM
    "{{ params.dds_schema_name }}".employees;
