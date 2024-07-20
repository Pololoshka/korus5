CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".position (
    id SERIAL PRIMARY KEY,
    position VARCHAR NOT NULL,
    UNIQUE (position)
);

TRUNCATE TABLE "{{ params.dds_schema_name }}".position CASCADE;

INSERT INTO
"{{ params.dds_schema_name }}".position (position)
SELECT DISTINCT coalesce(tp."new", empl."должность")
FROM
    "{{ params.ods_schema_name }}"."сотрудники_дар" AS empl
LEFT JOIN temp_positions AS tp ON empl."должность" = tp."old"
WHERE
    empl."должность" NOT IN ('', '-');
