CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".position (
  id SERIAL PRIMARY KEY,
  position VARCHAR NOT NULL,
  UNIQUE (position)
);


INSERT INTO
  "{{ params.dds_schema_name }}".position (position)
SELECT DISTINCT
  "должность"
FROM
  "{{ params.ods_schema_name }}"."сотрудники_дар"
WHERE
  "должность" NOT IN ('', '-')
ON CONFLICT (position) DO NOTHING;

