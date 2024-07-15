CREATE TABLE IF NOT EXISTS
  "{{ params.dm_schema_name }}".complex_data (
    id SERIAL PRIMARY KEY,
    year_finish INT NOT NULL,
    skill_id INT NOT NULL,
    dep_id INT NOT NULL,
    pos_id INT NOT NULL,
    employees INT NOT NULL,
    employees_skill INT NOT NULL,
    project INT NOT NULL,
    novice INT NOT NULL,
    junior INT NOT NULL,
    middle INT NOT NULL,
    senior INT NOT NULL,
    expert INT NOT NULL,
    "project_%" NUMERIC(5, 2) NOT NULL,
    "novice_%" NUMERIC(5, 2) NOT NULL,
    "junior_%" NUMERIC(5, 2) NOT NULL,
    "middle_%" NUMERIC(5, 2) NOT NULL,
    "senior_%" NUMERIC(5, 2) NOT NULL,
    "expert_%" NUMERIC(5, 2) NOT NULL,
    "AVR_level_1" NUMERIC(5, 2) NOT NULL,
    "AVR_level_2" SMALLINT NOT NULL
  );

TRUNCATE TABLE "{{ params.dm_schema_name }}".complex_data;

WITH
  group_table AS (
    SELECT
      "year" AS year_finish,
      skill_id,
      dep_id,
      pos_id,
      COUNT(empl_id) AS employees,
      COUNT(CASE WHEN level_num > 0 THEN empl_id END) AS employees_skill,
      COUNT(CASE WHEN level_num = 1 THEN empl_id END) AS project,
      COUNT(CASE WHEN level_num = 2 THEN empl_id END) AS novice,
      COUNT(CASE WHEN level_num = 3 THEN empl_id END) AS junior,
      COUNT(CASE WHEN level_num = 4 THEN empl_id END) AS middle,
      COUNT(CASE WHEN level_num = 5 THEN empl_id END) AS senior,
      COUNT(CASE WHEN level_num = 6 THEN empl_id END) AS expert
    FROM
      "{{ params.dm_schema_name }}"."now" AS n
    GROUP BY
      (dep_id, pos_id, skill_id, "year")
  )
INSERT INTO
  "{{ params.dm_schema_name }}".complex_data (
    year_finish,
    skill_id,
    dep_id,
    pos_id,
    employees,
    employees_skill,
    project,
    novice,
    junior,
    middle,
    senior,
    expert,
    "project_%",
    "novice_%",
    "junior_%",
    "middle_%",
    "senior_%",
    "expert_%",
    "AVR_level_1",
    "AVR_level_2"
  )
SELECT
  gt.year_finish,
  gt.skill_id,
  gt.dep_id,
  gt.pos_id,
  gt.employees,
  gt.employees_skill,
  gt.project,
  gt.novice,
  gt.junior,
  gt.middle,
  gt.senior,
  gt.expert,
  round(100 * coalesce(gt.project::NUMERIC / NULLif(gt.employees_skill, 0), 0), 2) AS "project_%",
  round(100 * coalesce(gt.novice::NUMERIC / NULLif(gt.employees_skill, 0), 0), 2) AS "novice_%",
  round(100 * coalesce(gt.junior::NUMERIC / NULLif(gt.employees_skill, 0), 0), 2) AS "junior_%",
  round(100 * coalesce(gt.middle::NUMERIC / NULLif(gt.employees_skill, 0), 0), 2) AS "middle_%",
  round(100 * coalesce(gt.senior::NUMERIC / NULLif(gt.employees_skill, 0), 0), 2) AS "senior_%",
  round(100 * coalesce(gt.expert::NUMERIC / NULLif(gt.employees_skill, 0), 0), 2) AS "expert_%",
  coalesce(
    (1 * gt.project + 2 * gt.novice + 3 * gt.junior + 4 * gt.middle + 5 * gt.senior + 6 * gt.expert)::NUMERIC / NULLif(gt.employees_skill, 0),
    0
  ) AS AVR_level_1,
  ceil(
    coalesce(
      (1 * gt.project + 2 * gt.novice + 3 * gt.junior + 4 * gt.middle + 5 * gt.senior + 6 * gt.expert)::NUMERIC / NULLif(gt.employees_skill, 0),
      0
    )
  ) AS AVR_level_2
FROM
  group_table AS gt;
