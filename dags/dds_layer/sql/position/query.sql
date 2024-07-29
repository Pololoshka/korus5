CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".position (
    id SERIAL PRIMARY KEY,
    position VARCHAR NOT NULL,
    UNIQUE (position)
);

TRUNCATE TABLE "{{ params.dds_schema_name }}".position RESTART IDENTITY CASCADE;

INSERT INTO
"{{ params.dds_schema_name }}".position (position)
SELECT DISTINCT coalesce(naming."new", empl."должность")
FROM
    "{{ params.ods_schema_name }}"."сотрудники_дар" AS empl
LEFT JOIN temp_positions_naming AS naming ON empl."должность" = naming."old"
WHERE
    empl."должность" NOT IN ('', '-');
