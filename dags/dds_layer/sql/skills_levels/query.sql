CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".skills_levels (
  id INT PRIMARY KEY,
  "date" DATE NOT NULL,
  skill_id INT NOT NULL,
  level_id INT NOT NULL,
  user_id INT NOT NULL,
  CONSTRAINT fk_skills_levels_skills
    FOREIGN KEY (skill_id)
      REFERENCES "{{ params.dds_schema_name }}".skills (id),
  CONSTRAINT fk_skills_levels_levels
    FOREIGN KEY (level_id)
      REFERENCES "{{ params.dds_schema_name }}".levels (id),
  CONSTRAINT fk_skills_levels_employees
    FOREIGN KEY (user_id)
      REFERENCES "{{ params.dds_schema_name }}".employees (id),
  UNIQUE (skill_id, level_id, user_id)
);

INSERT INTO
  "{{ params.dds_schema_name }}".skills_levels (id, "date", user_id, skill_id, level_id)
SELECT
  source.src_id,
  source.src_date,
  source.src_user,
  source.skill_id,
  source.level_id
FROM
  (
    SELECT
      MIN(src.id) AS src_id,
      MIN(TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY')) AS src_date,
      CAST(substring(src."название", 'User:(\d+)') AS INT) AS src_user,
      CAST(substring(src."Базы данных", '\[(\d+)\]') AS INT) AS skill_id,
      CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
      MAX(DATE(src."Дата изм.")) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."базы_данных_и_уровень_знаний_сотру" AS src
    GROUP BY
      src."название",
      src."Базы данных",
      src."Уровень знаний"
    UNION ALL
    SELECT
      MIN(src.id) AS src_id,
      MIN(TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY')) AS src_date,
      CAST(substring(src."название", 'User:(\d+)') AS INT) AS src_user,
      CAST(substring(src."инструменты", '\[(\d+)\]') AS INT) AS skill_id,
      CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
      MAX(DATE(src."Дата изм.")) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."инструменты_и_уровень_знаний_сотр" AS src
    GROUP BY
      src."название",
      src."инструменты",
      src."Уровень знаний"
    UNION ALL
    SELECT
      MIN(src.id) AS src_id,
      MIN(TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY')) AS src_date,
      CAST(src."User ID" AS INT) AS src_user,
      CAST(substring(src."платформы", '\[(\d+)\]') AS INT) AS skill_id,
      CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
      MAX(DATE(src."Дата изм.")) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."платформы_и_уровень_знаний_сотруд" AS src
    GROUP BY
      src."User ID",
      src."платформы",
      src."Уровень знаний"
    UNION ALL
    SELECT
      MIN(src.id) AS src_id,
      MIN(TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY')) AS src_date,
      CAST(substring(src."название", 'User:(\d+)') AS INT) AS src_user,
      CAST(substring(src."Среды разработки", '\[(\d+)\]') AS INT) AS skill_id,
      CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
      MAX(DATE(src."Дата изм.")) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."среды_разработки_и_уровень_знаний_" AS src
    GROUP BY
      src."название",
      src."Среды разработки",
      src."Уровень знаний"
    UNION ALL
    SELECT
      MIN(src.id) AS src_id,
      MAX(TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY')) AS src_date,
      CAST(substring(src."название", 'User:(\d+)') AS INT) AS src_user,
      CAST(substring(src."технологии", '\[(\d+)\]') AS INT) AS skill_id,
      CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
      MIN(DATE(src."Дата изм.")) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."технологии_и_уровень_знаний_сотру" AS src
    GROUP BY
      src."название",
      src."технологии",
      src."Уровень знаний"
    UNION ALL
    SELECT
      MIN(src.id) AS src_id,
      MIN(TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY')) AS src_date,
      CAST(substring(src."название", 'User:(\d+)') AS INT) AS src_user,
      CAST(substring(src."фреймворки", '\[(\d+)\]') AS INT) AS skill_id,
      CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
      MAX(DATE(src."Дата изм.")) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."фреймворки_и_уровень_знаний_сотру" AS src
    GROUP BY
      src."название",
      src."фреймворки",
      src."Уровень знаний"
    UNION ALL
    SELECT distinct
      MIN(src.id) AS src_id,
      MIN(TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY')) AS src_date,
      CAST(substring(src."название", 'User:(\d+)') AS INT) AS src_user,
      CAST(substring(src."Языки программирования", '\[(\d+)\]') AS INT) AS skill_id,
      CAST(substring(src."Уровень знаний", '\[(\d+)\]') AS INT) AS level_id,
      MAX(DATE(src."Дата изм.")) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."языки_программирования_и_уровень" AS src
    GROUP BY
      src."название",
      src."Языки программирования",
      src."Уровень знаний"
    UNION ALL
    SELECT distinct
      MIN(src.id) AS src_id,
      MIN(TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY')) AS src_date,
      src."User ID" AS src_user,
      CAST(substring(src."отрасли", '\[(\d+)\]') AS INT) AS skill_id,
      CAST(substring(src."Уровень знаний в отрасли", '\[(\d+)\]') AS INT) AS level_id,
      MAX(DATE(src."Дата изм.")) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."опыт_сотрудника_в_отраслях" AS src
    GROUP BY
      src."User ID",
      src."отрасли",
      src."Уровень знаний в отрасли"
    UNION ALL
    SELECT distinct
      MIN(src.id) AS src_id,
      MIN(TO_DATE (NULLIF(src."дата", ''), 'DD MM YYYY')) AS src_date,
      src."User ID" AS src_user,
      CAST(substring(src."Предментые области", '\[(\d+)\]') AS INT) AS skill_id,
      CAST(substring(src."Уровень знаний в предметной облас", '\[(\d+)\]') AS INT) AS level_id,
      MAX(DATE(src."Дата изм.")) AS modified_at
    FROM
      "{{ params.ods_schema_name }}"."опыт_сотрудника_в_предметных_обла" AS src
    GROUP BY
      src."User ID",
      src."Предментые области",
      src."Уровень знаний в предметной облас"
  ) AS source
  JOIN "{{ params.dds_schema_name }}".employees AS u ON u.id = source.src_user
  JOIN "{{ params.dds_schema_name }}".levels AS l ON l.id = source.level_id
  JOIN "{{ params.dds_schema_name }}".skills AS s ON s.id = source.skill_id
WHERE
  source.src_date <= CURRENT_DATE
  AND modified_at > DATE('{{ var.json.dds_layer_transfer.previous_executed_at }}')
ON CONFLICT (user_id, skill_id, level_id) DO
UPDATE
SET
  "date" = EXCLUDED."date";
