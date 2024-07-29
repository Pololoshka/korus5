CREATE TABLE IF NOT EXISTS "{{ params.dm_schema_name }}".positions (
    pos_id INT PRIMARY KEY,
    "name" VARCHAR NOT NULL
);

TRUNCATE TABLE "{{ params.dm_schema_name }}".positions RESTART IDENTITY CASCADE;

INSERT INTO "{{ params.dm_schema_name }}".positions (
    pos_id,
    "name"
)
SELECT
    id,
    position
FROM
    "{{ params.dds_schema_name }}".position;
