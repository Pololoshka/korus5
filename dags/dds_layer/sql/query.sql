BEGIN;

INSERT INTO
  "{{ params.dds_schema_name }}".skills_group (skill_group_name)
VALUES
  ('база данных'),
  ('инструмент'),
  ('платформа'),
  ('среда разработки'),
  ('технология'),
  ('тип системы'),
  ('фреймворк'),
  ('язык программирования') ON CONFLICT (skill_group_name) DO NOTHING;

INSERT INTO
  "{{ params.dds_schema_name }}".skills (skill_name, external_id, group_id)
SELECT
  source.skill_name,
  source.external_id,
  skill_group.id
FROM
  (
    SELECT
      src."название" AS skill_name,
      src.id AS external_id,
      'база данных' AS group_name,
      DATE (src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."базы_данных" AS src
    UNION ALL
    SELECT
      src."название" AS skill_name,
      src.id AS external_id,
      'инструмент' AS group_name,
      DATE (src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."инструменты" AS src
    UNION ALL
    SELECT
      src."название" AS skill_name,
      src.id AS external_id,
      'платформа' AS group_name,
      DATE (src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."платформы" AS src
    UNION ALL
    SELECT
      src."название" AS skill_name,
      src.id AS external_id,
      'среда разработки' AS group_name,
      DATE (src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."среды_разработки" AS src
    UNION ALL
    SELECT
      src."название" AS skill_name,
      src.id AS external_id,
      'технология' AS group_name,
      DATE (src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."технологии" AS src
    UNION ALL
    SELECT
      src."название" AS skill_name,
      src.id AS external_id,
      'тип системы' AS group_name,
      DATE (src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."типы_систем" AS src
    UNION ALL
    SELECT
      src."название" AS skill_name,
      src.id AS external_id,
      'фреймворк' AS group_name,
      DATE (src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."фреймворки" AS src
    UNION ALL
    SELECT
      src."название" AS skill_name,
      src.id AS external_id,
      'язык программирования' AS group_name,
      DATE (src."Дата изм.") AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."языки_программирования" AS src
  ) AS source
  JOIN "{{ params.dds_schema_name }}".skills_group AS skill_group ON skill_group.skill_group_name = source.group_name
WHERE
  source.modified_at > DATE (
    '{{ var.json.dds_layer_transfer.previous_executed_at }}'
  ) ON CONFLICT (external_id) DO
UPDATE
SET
  skill_name = EXCLUDED.skill_name;

INSERT INTO
  "{{ params.dds_schema_name }}".levels ("level", id)
SELECT
  "название",
  id
FROM
  "{{ params.ods_schema_name }}"."уровни_знаний"
WHERE
  DATE ("Дата изм.") > DATE (
    '{{ var.json.dds_layer_transfer.previous_executed_at }}'
  ) ON CONFLICT (id) DO
UPDATE
SET
  "level" = EXCLUDED."level";

INSERT INTO
  "{{ params.dds_schema_name }}".departments (department)
SELECT DISTINCT
  REPLACE ("подразделения", '. ', '')
FROM
  "{{ params.ods_schema_name }}"."сотрудники_дар"
WHERE
  "подразделения" != '' ON CONFLICT (department) DO NOTHING;

INSERT INTO
  "{{ params.dds_schema_name }}".position (position)
SELECT DISTINCT
  "должность"
FROM
  "{{ params.ods_schema_name }}"."сотрудники_дар"
WHERE
  "должность" NOT IN ('', '-') ON CONFLICT (position) DO NOTHING;

INSERT INTO
  "{{ params.dds_schema_name }}".employees (id, last_name, first_name, FOC, dep_id, pos_id)
SELECT
  u.id,
  u."фамилия",
  u."имя",
  u."цфо",
  dep.id,
  pos.id
FROM
  "{{ params.ods_schema_name }}"."сотрудники_дар" AS u
  JOIN "{{ params.dds_schema_name }}".position AS pos ON pos.position = u."должность"
  JOIN "{{ params.dds_schema_name }}".departments AS dep ON dep.department = REPLACE (u."подразделения", '. ', '') ON CONFLICT (id) DO
UPDATE
SET
  last_name = EXCLUDED.last_name,
  first_name = EXCLUDED.first_name,
  dep_id = EXCLUDED.dep_id,
  pos_id = EXCLUDED.pos_id;


INSERT INTO
  "{{ params.dds_schema_name }}".cv_dar (id, activity, user_id)
SELECT
  cv."ResumeID",
  cv."Активность",
  u.id
FROM
  "{{ params.ods_schema_name }}"."резюмедар" AS cv
  JOIN "{{ params.dds_schema_name }}".employees AS u ON u.id = cv."UserID"
  ON CONFLICT (id) DO
UPDATE
SET
  activity = EXCLUDED.activity;


INSERT INTO "{{ params.dds_schema_name }}".skills_levels (
  "date", user_id, skill_id, level_id
)
SELECT
  source.src_date,
  source.src_user,
  source.skill_id,
  source.level_id
FROM
  (
    SELECT
      min(
        to_date(
          NULLIF(src."дата", ''),
          'DD MM YYYY'
        )
      ) AS src_date,
      substring(
        src."название", 'User:(\d+)'
      ):: INT AS src_user,
      substring(
        src."Базы данных", '\[(\d+)\]'
      ):: INT AS skill_id,
      substring(
        src."Уровень знаний",
        '\[(\d+)\]'
      ):: INT AS level_id,
      max(
        DATE (src."Дата изм.")
      ) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."базы_данных_и_уровень_знаний_сотру" AS src
    group by
      src."название",
      src."Базы данных",
      src."Уровень знаний"
    UNION ALL
    SELECT
      min(
        to_date(
          NULLIF(src."дата", ''),
          'DD MM YYYY'
        )
      ) AS src_date,
      substring(
        src."название", 'User:(\d+)'
      ):: INT AS src_user,
      substring(
        src."инструменты", '\[(\d+)\]'
      ):: INT AS skill_id,
      substring(
        src."Уровень знаний",
        '\[(\d+)\]'
      ):: INT AS level_id,
      max(
        DATE (src."Дата изм.")
      ) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."инструменты_и_уровень_знаний_сотр" AS src
    group by
      src."название",
      src."инструменты",
      src."Уровень знаний"
    UNION ALL
    SELECT
      min(
        to_date(
          NULLIF(src."дата", ''),
          'DD MM YYYY'
        )
      ) AS src_date,
      src."User ID" :: INT AS src_user,
      substring(
        src."платформы", '\[(\d+)\]'
      ):: INT AS skill_id,
      substring(
        src."Уровень знаний",
        '\[(\d+)\]'
      ):: INT AS level_id,
      max(
        DATE (src."Дата изм.")
      ) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."платформы_и_уровень_знаний_сотруд" AS src
    group by
      src."User ID",
      src."платформы",
      src."Уровень знаний"
    UNION ALL
    SELECT
      min(
        to_date(
          NULLIF(src."дата", ''),
          'DD MM YYYY'
        )
      ) AS src_date,
      substring(
        src."название", 'User:(\d+)'
      ):: INT AS src_user,
      substring(
        src."Среды разработки",
        '\[(\d+)\]'
      ):: INT AS skill_id,
      substring(
        src."Уровень знаний",
        '\[(\d+)\]'
      ):: INT AS level_id,
      max(
        DATE (src."Дата изм.")
      ) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."среды_разработки_и_уровень_знаний_" AS src
    group by
      src."название",
      src."Среды разработки",
      src."Уровень знаний"
    UNION ALL
    SELECT
      max(
        to_date(
          NULLIF(src."дата", ''),
          'DD MM YYYY'
        )
      ) AS src_date,
      substring(
        src."название", 'User:(\d+)'
      ):: INT AS src_user,
      substring(
        src."технологии", '\[(\d+)\]'
      ):: INT AS skill_id,
      substring(
        src."Уровень знаний",
        '\[(\d+)\]'
      ):: INT AS level_id,
      min(
        DATE (src."Дата изм.")
      ) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."технологии_и_уровень_знаний_сотру" AS src
    group by
      src."название",
      src."технологии",
      src."Уровень знаний"
    UNION ALL
    SELECT
      min(
        to_date(
          NULLIF(src."дата", ''),
          'DD MM YYYY'
        )
      ) AS src_date,
      substring(
        src."название", 'User:(\d+)'
      ):: INT AS src_user,
      substring(
        src."фреймворки", '\[(\d+)\]'
      ):: INT AS skill_id,
      substring(
        src."Уровень знаний",
        '\[(\d+)\]'
      ):: INT AS level_id,
      max(
        DATE (src."Дата изм.")
      ) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."фреймворки_и_уровень_знаний_сотру" AS src
    group by
      src."название",
      src."фреймворки",
      src."Уровень знаний"
    UNION ALL
    select
      distinct min(
        to_date(
          NULLIF(src."дата", ''),
          'DD MM YYYY'
        )
      ) AS src_date,
      substring(
        src."название", 'User:(\d+)'
      ):: INT AS src_user,
      substring(
        src."Языки программирования",
        '\[(\d+)\]'
      ):: INT AS skill_id,
      substring(
        src."Уровень знаний",
        '\[(\d+)\]'
      ):: INT AS level_id,
      max(
        DATE (src."Дата изм.")
      ) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."языки_программирования_и_уровень" AS src
    group by
      src."название",
      src."Языки программирования",
      src."Уровень знаний"
  ) AS source
  JOIN "{{ params.dds_schema_name }}".employees AS u ON u.id = source.src_user
  JOIN "{{ params.dds_schema_name }}".levels AS l ON l.id = source.level_id
  JOIN "{{ params.dds_schema_name }}".skills AS s ON s.external_id = source.skill_id
WHERE
  source.src_date <= CURRENT_DATE
  AND modified_at > DATE (
    '{{ var.json.dds_layer_transfer.previous_executed_at }}'
  ) ON CONFLICT (user_id, skill_id, level_id) DO
UPDATE
SET
  "date" = EXCLUDED."date";


COMMIT;
