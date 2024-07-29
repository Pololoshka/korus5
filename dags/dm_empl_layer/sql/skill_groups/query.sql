CREATE TABLE IF NOT EXISTS "{{ params.dm_schema_name }}".skill_groups (
    group_id INT PRIMARY KEY,
    "name" VARCHAR NOT NULL
);

TRUNCATE TABLE "{{ params.dm_schema_name }}".skill_groups RESTART IDENTITY CASCADE;

INSERT INTO
"{{ params.dm_schema_name }}".skill_groups (
    group_id,
    "name"
)
SELECT
    id,
    skill_group_name
FROM
    "{{ params.dds_schema_name }}".skills_group;
