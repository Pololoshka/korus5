CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".skills (
    id INT PRIMARY KEY,
    skill_name VARCHAR NOT NULL,
    group_id INT NOT NULL,
    CONSTRAINT fk_skills_skills_group
    FOREIGN KEY (group_id)
    REFERENCES "{{ params.dds_schema_name }}".skills_group (id),
    UNIQUE (skill_name, group_id)
);

TRUNCATE TABLE "{{ params.dds_schema_name }}".skills RESTART IDENTITY CASCADE;

-- Creating temp table 'temp_skills' with all skills
CREATE TEMP TABLE temp_skills ON COMMIT DROP AS (
    SELECT
        source.src_id AS id,
        source.skill_name,
        skill_group.id AS group_id,
        source.row_data
    FROM
        (
            SELECT
                src.id AS src_id,
                src."название" AS skill_name,
                'Базы данных' AS group_name,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."базы_данных" AS src
            UNION ALL
            SELECT
                src.id AS src_id,
                src."название" AS skill_name,
                'Инструменты' AS group_name,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."инструменты" AS src
            UNION ALL
            SELECT
                src.id AS src_id,
                src."название" AS skill_name,
                'Платформы' AS group_name,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."платформы" AS src
            UNION ALL
            SELECT
                src.id AS src_id,
                src."название" AS skill_name,
                'Среды разработки' AS group_name,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."среды_разработки" AS src
            UNION ALL
            SELECT
                src.id AS src_id,
                src."название" AS skill_name,
                'Технологии' AS group_name,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."технологии" AS src
            UNION ALL
            SELECT
                src.id AS src_id,
                src."название" AS skill_name,
                'Фреймворки' AS group_name,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."фреймворки" AS src
            UNION ALL
            SELECT
                src.id AS src_id,
                src."название" AS skill_name,
                'Языки программирования' AS group_name,
                src."Дата изм."::TIMESTAMP AS modified_at,
                ROW_TO_JSON(src) AS row_data
            FROM
                "{{ params.ods_schema_name }}"."языки_программирования" AS src
        ) AS source
    LEFT JOIN
        "{{ params.dds_schema_name }}".skills_group AS skill_group
        ON source.group_name = skill_group.skill_group_name
    WHERE
        source.modified_at > '{{ var.json.dds_layer_transfer.previous_executed_at }}'::TIMESTAMP
);

-- Removing all invalid rows from temp table and filling 'failed_entities' table
WITH
invalid_skills AS (
    DELETE FROM temp_skills
    WHERE skill_name IS NULL OR skill_name = ''
    RETURNING *
)

INSERT INTO
"{{ params.dds_schema_name }}".failed_entities (entity_name, reason, entity)
SELECT
    'skills' AS entity_name,
    'skill_name__is__null' AS reason,
    row_data AS entity
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
    'skills' AS entity_name,
    'group_id__is__null' AS reason,
    row_data AS entity
FROM
    invalid_skills;


-- Filling 'skills' table with correct skills

INSERT INTO
"{{ params.dds_schema_name }}".skills (id, skill_name, group_id)
SELECT
    ts.id,
    COALESCE(naming."new", ts.skill_name) AS skill_name,
    ts.group_id
FROM
    temp_skills AS ts
LEFT JOIN temp_skills_naming AS naming ON ts.skill_name = naming."old"
ON CONFLICT (id) DO
UPDATE
SET
skill_name = excluded.skill_name;
