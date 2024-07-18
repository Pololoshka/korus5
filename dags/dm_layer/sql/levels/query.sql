CREATE TABLE IF NOT EXISTS "{{ params.dm_schema_name }}".levels (
    level_id INT PRIMARY KEY,
    "name" VARCHAR NOT NULL,
    level_num INT NOT NULL
);

TRUNCATE TABLE "{{ params.dm_schema_name }}".levels;

INSERT INTO
"{{ params.dm_schema_name }}".levels (
    level_id,
    "name",
    level_num
)
SELECT
    id,
    level,
    level_num
FROM
    "{{ params.dds_schema_name }}".levels;
