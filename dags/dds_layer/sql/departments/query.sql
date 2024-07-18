CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".departments (
    id SERIAL PRIMARY KEY,
    department VARCHAR NOT NULL,
    UNIQUE (department)
);


INSERT INTO
"{{ params.dds_schema_name }}".departments (
    department
)
    SELECT DISTINCT coalesce(td."new", empl."подразделения")
FROM
    "{{ params.ods_schema_name }}"."сотрудники_дар" as empl
    LEFT JOIN temp_departments as td ON empl."подразделения" = td."old"
WHERE
    "подразделения" != '';



