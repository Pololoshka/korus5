CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".position (
    id SERIAL PRIMARY KEY,
    position VARCHAR NOT NULL,
    UNIQUE (position)
);


INSERT INTO
"{{ params.dds_schema_name }}".position (position)
SELECT DISTINCT coalesce(tp."new", empl."должность")
FROM
    "{{ params.ods_schema_name }}"."сотрудники_дар" as empl
    LEFT JOIN temp_positions as tp ON empl."должность" = tp."old"
WHERE
    "должность" NOT IN ('', '-');
