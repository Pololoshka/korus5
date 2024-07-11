CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".skills (
  id INT PRIMARY KEY,
  skill_name VARCHAR NOT NULL,
  group_id INT NOT NULL,
  CONSTRAINT fk_skills_skills_group
    FOREIGN KEY (group_id)
      REFERENCES "{{ params.dds_schema_name }}".skills_group (id),
  UNIQUE (skill_name, group_id)
);

-- Creating temp table 'temp_skills' with all skills
CREATE TEMP TABLE temp_skills ON COMMIT DROP AS (
  SELECT
    source.src_id as id,
    source.skill_name as skill_name,
    skill_group.id as group_id,
    source.row_data as row_data
  FROM
    (
      SELECT
        src.id AS src_id,
        src."название" AS skill_name,
        'база данных' AS group_name,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."базы_данных" AS src
      UNION ALL
      SELECT
        src.id AS src_id,
        src."название" AS skill_name,
        'инструмент' AS group_name,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."инструменты" AS src
      UNION ALL
      SELECT
        src.id AS src_id,
        src."название" AS skill_name,
        'платформа' AS group_name,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."платформы" AS src
      UNION ALL
      SELECT
        src.id AS src_id,
        src."название" AS skill_name,
        'среда разработки' AS group_name,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."среды_разработки" AS src
      UNION ALL
      SELECT
        src.id AS src_id,
        src."название" AS skill_name,
        'технология' AS group_name,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."технологии" AS src
      UNION ALL
      SELECT
        src.id AS src_id,
        src."название" AS skill_name,
        'фреймворк' AS group_name,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."фреймворки" AS src
      UNION ALL
      SELECT
        src.id AS src_id,
        src."название" AS skill_name,
        'язык программирования' AS group_name,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."языки_программирования" AS src
      UNION ALL
      SELECT
        src.id AS src_id,
        src."название" AS skill_name,
        'отрасль' AS group_name,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."отрасли" AS src
      UNION ALL
      SELECT
        src.id AS src_id,
        src."название" AS skill_name,
        'предметная область' AS group_name,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."предметная_область" AS src
    ) AS source
    LEFT JOIN "{{ params.dds_schema_name }}".skills_group AS skill_group ON skill_group.skill_group_name = source.group_name
  WHERE
    source.modified_at > CAST('{{ var.json.dds_layer_transfer.previous_executed_at }}' AS TIMESTAMP)
);

-- Removing all invalid rows from temp table and filling 'failed_entities' table
WITH
  invalid_skills AS (
    DELETE FROM temp_skills
    WHERE skill_name IS NULL or skill_name = ''
    RETURNING *
  )
INSERT INTO
  "{{ params.dds_schema_name }}".failed_entities (entity_name, reason, entity)
SELECT
  'skills',
  'skill_name__is__null',
  row_data
FROM
  invalid_skills;

WITH
  invalid_skills AS (
    DELETE FROM temp_skills
    WHERE group_id IS NULL
    RETURNING *
  )
INSERT INTO
  "{{ params.dds_schema_name }}".failed_entities (entity_name, reason, entity)
SELECT
  'skills',
  'group_id__is__null',
  row_data
FROM
  invalid_skills;


-- Filling 'employees' table with correct employees

INSERT INTO
  "{{ params.dds_schema_name }}".skills (id, skill_name, group_id)
SELECT
  id,
  skill_name,
  group_id
FROM
  temp_skills
ON CONFLICT (id) DO
UPDATE
SET
  skill_name = EXCLUDED.skill_name;
