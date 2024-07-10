CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".departments (
  id SERIAL PRIMARY KEY,
  department VARCHAR NOT NULL,
  UNIQUE (department)
);


INSERT INTO
  "{{ params.dds_schema_name }}".departments (department)
SELECT DISTINCT
  REPLACE ("подразделения", '. ', '')
FROM
  "{{ params.ods_schema_name }}"."сотрудники_дар"
WHERE
  "подразделения" != ''
ON CONFLICT (department) DO NOTHING;
