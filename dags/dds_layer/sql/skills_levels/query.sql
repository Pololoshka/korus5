CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".skills_levels (
  id INT PRIMARY KEY,
  "date" DATE NOT NULL,
  skill_id INT NOT NULL,
  level_id INT NOT NULL,
  empl_id INT NOT NULL,
  CONSTRAINT fk_skills_levels_skills
    FOREIGN KEY (skill_id)
      REFERENCES "{{ params.dds_schema_name }}".skills (id),
  CONSTRAINT fk_skills_levels_levels
    FOREIGN KEY (level_id)
      REFERENCES "{{ params.dds_schema_name }}".levels (id),
  CONSTRAINT fk_skills_levels_employees
    FOREIGN KEY (empl_id)
      REFERENCES "{{ params.dds_schema_name }}".employees (id)
-- UNIQUE (skill_id, level_id, empl_id)  This table will have duplicates. Suggested by BA
);

-- Creating temp table 'temp_skills_levels' with all user-skill association
CREATE TEMP TABLE temp_skills_levels ON COMMIT DROP AS (
  SELECT
    source.id,
    source.date,
    u.id as empl_id,
    l.id as level_id,
    s.id as skill_id,
    source.row_data
  FROM
    (
      SELECT
        src.id AS id,
        TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
        CAST(substring(src."название", 'User:(\d+)') AS INT) AS empl_id,
        CAST(substring(src."Базы данных", '\[(\d+)\]') AS INT) AS skill_id,
        CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."базы_данных_и_уровень_знаний_сотру" AS src

      UNION ALL
      SELECT
        src.id AS id,
        TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
        CAST(substring(src."название", 'User:(\d+)') AS INT) AS empl_id,
        CAST(substring(src."инструменты", '\[(\d+)\]') AS INT) AS skill_id,
        CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."инструменты_и_уровень_знаний_сотр" AS src

      UNION ALL
      SELECT
        src.id AS id,
        TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
        CAST(src."User ID" AS INT) AS empl_id,
        CAST(substring(src."платформы", '\[(\d+)\]') AS INT) AS skill_id,
        CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."платформы_и_уровень_знаний_сотруд" AS src

      UNION ALL
      SELECT
        src.id AS id,
        TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
        CAST(substring(src."название", 'User:(\d+)') AS INT) AS empl_id,
        CAST(substring(src."Среды разработки", '\[(\d+)\]') AS INT) AS skill_id,
        CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."среды_разработки_и_уровень_знаний_" AS src

      UNION ALL
      SELECT
        src.id AS id,
        TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
        CAST(substring(src."название", 'User:(\d+)') AS INT) AS empl_id,
        CAST(substring(src."технологии", '\[(\d+)\]') AS INT) AS skill_id,
        CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."технологии_и_уровень_знаний_сотру" AS src

      UNION ALL
      SELECT
        src.id AS id,
        TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
        CAST(substring(src."название", 'User:(\d+)') AS INT) AS empl_id,
        CAST(substring(src."фреймворки", '\[(\d+)\]') AS INT) AS skill_id,
        CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."фреймворки_и_уровень_знаний_сотру" AS src

      UNION ALL
      SELECT
        src.id AS id,
        TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
        CAST(substring(src."название", 'User:(\d+)') AS INT) AS empl_id,
        CAST(substring(src."Языки программирования", '\[(\d+)\]') AS INT) AS skill_id,
        CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
        CAST(src."Дата изм." AS TIMESTAMP) AS modified_at,
        row_to_json(src) AS row_data
      FROM
        "{{ params.ods_schema_name }}"."языки_программирования_и_уровень" AS src
    ) AS source
    LEFT JOIN "{{ params.dds_schema_name }}".employees AS u ON u.id = source.empl_id
    LEFT JOIN "{{ params.dds_schema_name }}".levels AS l ON l.id = source.level_id
    LEFT JOIN "{{ params.dds_schema_name }}".skills AS s ON s.id = source.skill_id
  WHERE
    source.modified_at > CAST('{{ var.json.dds_layer_transfer.previous_executed_at }}' AS TIMESTAMP)
);


-- Removing all invalid rows from temp table and filling 'failed_entities' table

WITH
  invalid_skills AS (
    DELETE FROM temp_skills_levels
    WHERE skill_id IS NULL
    RETURNING *
  )
INSERT INTO
  "{{ params.dds_schema_name }}".failed_entities (entity_name, reason, entity)
SELECT
  'skills_levels',
  'skill_id__is__null',
  row_data
FROM
  invalid_skills;

WITH
  invalid_skills AS (
    DELETE FROM temp_skills_levels
    WHERE level_id IS NULL
    RETURNING *
  )
INSERT INTO
  "{{ params.dds_schema_name }}".failed_entities (entity_name, reason, entity)
SELECT
  'skills_levels',
  'level_id__is__null',
  row_data
FROM
  invalid_skills;

WITH
  invalid_skills AS (
    DELETE FROM temp_skills_levels
    WHERE empl_id IS NULL
    RETURNING *
  )
INSERT INTO
  "{{ params.dds_schema_name }}".failed_entities (entity_name, reason, entity)
SELECT
  'skills_levels',
  'empl_id__is__null',
  row_data
FROM
  invalid_skills;

WITH
  invalid_skills AS (
    DELETE FROM temp_skills_levels
    WHERE "date" > CURRENT_DATE OR "date" IS NULL
    RETURNING *
  )
INSERT INTO
  "{{ params.dds_schema_name }}".failed_entities (entity_name, reason, entity)
SELECT
  'skills_levels',
  'date__is__invalid',
  row_data
FROM
  invalid_skills;


-- Filling 'skills_levels' table with correct user-skill association

INSERT INTO
  "{{ params.dds_schema_name }}".skills_levels (id, "date", empl_id, skill_id, level_id)
SELECT
    id,
    "date",
    empl_id,
    skill_id,
    level_id
FROM
  temp_skills_levels
ON CONFLICT (id) DO
UPDATE
SET
  "date" = EXCLUDED."date";
