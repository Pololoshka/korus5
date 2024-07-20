CREATE TABLE IF NOT EXISTS "{{ params.dm_schema_name }}".skills (
    skill_id INT PRIMARY KEY,
    "name" VARCHAR NOT NULL
);

TRUNCATE TABLE "{{ params.dm_schema_name }}".skills CASCADE;

INSERT INTO
"{{ params.dm_schema_name }}".skills (
    skill_id,
    "name"
)
SELECT
    id,
    skill_name

FROM
    "{{ params.dds_schema_name }}".skills;
