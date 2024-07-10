CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".skills (
  id INT PRIMARY KEY,
  skill_name VARCHAR NOT NULL,
  group_id INT NOT NULL,
  CONSTRAINT fk_skills_skills_group
    FOREIGN KEY (group_id)
      REFERENCES "{{ params.dds_schema_name }}".skills_group (id),
  UNIQUE (skill_name, group_id)
);

INSERT INTO
  "{{ params.dds_schema_name }}".skills (id, skill_name, group_id)
SELECT
  source.src_id,
  source.skill_name,
  skill_group.id
FROM
  (
    SELECT
      src.id AS src_id,
      src."название" AS skill_name,
      'база данных' AS group_name,
      DATE(src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."базы_данных" AS src
    UNION ALL
    SELECT
      src.id AS src_id,
      src."название" AS skill_name,
      'инструмент' AS group_name,
      DATE(src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."инструменты" AS src
    UNION ALL
    SELECT
      src.id AS src_id,
      src."название" AS skill_name,
      'платформа' AS group_name,
      DATE(src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."платформы" AS src
    UNION ALL
    SELECT
      src.id AS src_id,
      src."название" AS skill_name,
      'среда разработки' AS group_name,
      DATE(src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."среды_разработки" AS src
    UNION ALL
    SELECT
      src.id AS src_id,
      src."название" AS skill_name,
      'технология' AS group_name,
      DATE(src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."технологии" AS src
    UNION ALL
    SELECT
      src.id AS src_id,
      src."название" AS skill_name,
      'фреймворк' AS group_name,
      DATE(src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."фреймворки" AS src
    UNION ALL
    SELECT
      src.id AS src_id,
      src."название" AS skill_name,
      'язык программирования' AS group_name,
      DATE(src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."языки_программирования" AS src
    UNION ALL
    SELECT
      src.id AS src_id,
      src."название" AS skill_name,
      'отрасль' AS group_name,
      DATE(src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."отрасли" AS src
    UNION ALL
    SELECT
      src.id AS src_id,
      src."название" AS skill_name,
      'предметная область' AS group_name,
      DATE(src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."предметная_область" AS src
  ) AS source
  JOIN "{{ params.dds_schema_name }}".skills_group AS skill_group ON skill_group.skill_group_name = source.group_name
WHERE
  source.modified_at > DATE('{{ var.json.dds_layer_transfer.previous_executed_at }}')
ON CONFLICT (id) DO
UPDATE
SET
  skill_name = EXCLUDED.skill_name;
