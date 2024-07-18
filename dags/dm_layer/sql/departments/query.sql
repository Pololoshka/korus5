CREATE TABLE IF NOT EXISTS "{{ params.dm_schema_name }}".departments (
    dep_id INT PRIMARY KEY,
    "name" VARCHAR NOT NULL
);

TRUNCATE TABLE "{{ params.dm_schema_name }}".departments;

INSERT INTO
"{{ params.dm_schema_name }}".departments (
    dep_id,
    "name"
)
SELECT
    id,
    department
FROM
    "{{ params.dds_schema_name }}".departments;
