CREATE TABLE IF NOT EXISTS
  "{{ params.dm_schema_name }}".compare (
    id SERIAL PRIMARY KEY,
    start_year INT NOT NULL,
    finish_year INT NOT NULL,
    empl_id INT NOT NULL,
    dep_id INT NOT NULL,
    pos_id INT NOT NULL,
    change_id INT NOT NULL,
    group_id INT NOT NULL,
    skill_id INT NOT NULL,
    level_id INT NOT NULL,
    level_change INT NOT NULL
  );

TRUNCATE TABLE "{{ params.dm_schema_name }}".compare;

INSERT INTO
  "{{ params.dm_schema_name }}".compare (
    empl_id,
    dep_id,
    pos_id,
    change_id,
    group_id,
    skill_id,
    start_year,
    finish_year,
    level_id,
    level_change
  )
SELECT
  sd.empl_id,
  sd.dep_id,
  sd.pos_id,
  fd.change_id,
  sd.group_id,
  sd.skill_id,
  sd.year AS start_year,
  fd.year AS finish_year,
  fd.level_id,
  (fd.level_num % 10) - (sd.level_num % 10) AS level_change
FROM
  "{{ params.dm_schema_name }}"."now" AS sd
  CROSS JOIN "{{ params.dm_schema_name }}"."now" AS fd
WHERE
  sd.year < fd.year
  AND sd.empl_id = fd.empl_id
  AND sd.skill_id = fd.skill_id
  AND (fd.level_num % 10) - (sd.level_num % 10) > 0;

-- Вариант 2
-- WITH
--   start_data AS (
--     SELECT
--       empl_id,
--       dep_id,
--       pos_id,
--       change_id,
--       group_id,
--       skill_id,
--       "year",
--       level_id,
--       level_num
--     FROM
--       "{{ params.dm_schema_name }}".""now""
--   ),
--   years AS (
--     SELECT DISTINCT
--       "year"
--     FROM
--       "{{ params.dm_schema_name }}"."now"
--   ),
--   delta AS (
--     SELECT
--       y1."year" AS y1,
--       y2."year" AS y2
--     FROM
--       years AS y1
--       CROSS JOIN years AS y2
--     WHERE
--       y1."year" < y2."year"
--   ),
--   data_to_insert AS (
--     SELECT
--       sd.empl_id,
--       sd.dep_id,
--       sd.pos_id,
--       fd.change_id,
--       sd.group_id,
--       sd.skill_id,
--       sd."year" AS start_year,
--       fd."year" AS finish_year,
--       fd.level_id,
--       (fd.level_num % 10) - (sd.level_num % 10) AS level_change
--     FROM
--       start_data AS sd
--       INNER JOIN "{{ params.dm_schema_name }}".""now"" AS fd ON sd.empl_id = fd.empl_id
--       AND sd.skill_id = fd.skill_id
--       INNER JOIN delta AS d ON sd."year" = d.y1
--       AND fd."year" = d.y2
--     WHERE
--       sd."year" < fd."year"
--   )
-- INSERT INTO
--   ""{{ params.dm_schema_name }}""."compare" (
--     empl_id,
--     dep_id,
--     pos_id,
--     change_id,
--     group_id,
--     skill_id,
--     start_year,
--     finish_year,
--     level_id,
--     level_change
--   )
-- SELECT
--   *
-- FROM
--   data_to_insert
-- WHERE
--   level_change > 0
