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
        u.id AS empl_id,
        l.id AS level_id,
        s.id AS skill_id,
        source.row_data
    FROM
        (
            SELECT
                src.id,
                TO_DATE(NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
                SUBSTRING(src."название", 'User:(\d+)')::INT AS empl_id,
                SUBSTRING(src."Базы данных", '\[(\d+)\]')::INT AS skill_id,
                SUBSTRING(src."Уровень знаний", '\[(\d+)\]')::INT AS level_id,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."базы_данных_и_уровень_знаний_сотру" AS src

            UNION ALL
            SELECT
                src.id,
                TO_DATE(NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
                SUBSTRING(src."название", 'User:(\d+)')::INT AS empl_id,
                SUBSTRING(src."инструменты", '\[(\d+)\]')::INT AS skill_id,
                SUBSTRING(src."Уровень знаний", '\[(\d+)\]')::INT AS level_id,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."инструменты_и_уровень_знаний_сотр" AS src

            UNION ALL
            SELECT
                src.id,
                TO_DATE(NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
                src."User ID"::INT AS empl_id,
                SUBSTRING(src."платформы", '\[(\d+)\]')::INT AS skill_id,
                SUBSTRING(src."Уровень знаний", '\[(\d+)\]')::INT AS level_id,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."платформы_и_уровень_знаний_сотруд" AS src

            UNION ALL
            SELECT
                src.id,
                TO_DATE(NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
                SUBSTRING(src."название", 'User:(\d+)')::INT AS empl_id,
                SUBSTRING(src."Среды разработки", '\[(\d+)\]')::INT AS skill_id,
                SUBSTRING(src."Уровень знаний", '\[(\d+)\]')::INT AS level_id,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."среды_разработки_и_уровень_знаний_" AS src

            UNION ALL
            SELECT
                src.id,
                TO_DATE(NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
                SUBSTRING(src."название", 'User:(\d+)')::INT AS empl_id,
                SUBSTRING(src."технологии", '\[(\d+)\]')::INT AS skill_id,
                SUBSTRING(src."Уровень знаний", '\[(\d+)\]')::INT AS level_id,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."технологии_и_уровень_знаний_сотру" AS src

            UNION ALL
            SELECT
                src.id,
                TO_DATE(NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
                SUBSTRING(src."название", 'User:(\d+)')::INT AS empl_id,
                SUBSTRING(src."фреймворки", '\[(\d+)\]')::INT AS skill_id,
                SUBSTRING(src."Уровень знаний", '\[(\d+)\]')::INT AS level_id,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."фреймворки_и_уровень_знаний_сотру" AS src

            UNION ALL
            SELECT
                src.id,
                TO_DATE(NULLIF(src."дата", ''), 'DD MM YYYY') AS "date",
                SUBSTRING(src."название", 'User:(\d+)')::INT AS empl_id,
                SUBSTRING(src."Языки программирования", '\[(\d+)\]')::INT AS skill_id,
                SUBSTRING(src."Уровень знаний", '\[(\d+)\]')::INT AS level_id,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."языки_программирования_и_уровень" AS src
        ) AS source
    LEFT JOIN "{{ params.dds_schema_name }}".employees AS u ON source.empl_id = u.id
    LEFT JOIN "{{ params.dds_schema_name }}".levels AS l ON source.level_id = l.id
    LEFT JOIN "{{ params.dds_schema_name }}".skills AS s ON source.skill_id = s.id
    WHERE
        source.modified_at > '{{ var.json.dds_layer_transfer.previous_executed_at }}'::TIMESTAMP
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
    'skills_levels' AS entity_name,
    'skill_id__is__null' AS reason,
    row_data AS entity
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
    'skills_levels' AS entity_name,
    'level_id__is__null' AS reason,
    row_data AS entity
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
    'skills_levels' AS entity_name,
    'empl_id__is__null' AS reason,
    row_data AS entity
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
    'skills_levels' AS entity_name,
    'date__is__invalid' AS reason,
    row_data AS entity
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
"date" = excluded."date";
