CREATE TABLE IF NOT EXISTS "{{ params.dm_schema_name }}".employees_statistic (
    id SERIAL PRIMARY KEY,
    start_year INT NOT NULL,
    finish_year INT NOT NULL,
    empl_id INT NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    dep_id INT NOT NULL,
    department VARCHAR NOT NULL,
    pos_id INT NOT NULL,
    position VARCHAR NOT NULL,
    skill_id INT NOT NULL,
    skill_name VARCHAR NOT NULL,
    level_id INT,
    level_name VARCHAR,
    level_num INT,
    group_id INT NOT NULL,
    group_name VARCHAR NOT NULL,
    empl_total_count INT,
    empl_count INT,
    empl_project_count INT,
    empl_project_pct NUMERIC(5, 2),
    empl_novice_count INT,
    empl_novice_pct NUMERIC(5, 2),
    empl_junior_count INT,
    empl_junior_pct NUMERIC(5, 2),
    empl_middle_count INT,
    empl_middle_pct NUMERIC(5, 2),
    empl_senior_count INT,
    empl_senior_pct NUMERIC(5, 2),
    empl_expert_count INT,
    empl_expert_pct NUMERIC(5, 2),
    avr_skill_level INT,
    level_change INT,
    level_change_total INT,
    marker VARCHAR NOT NULL
);

TRUNCATE TABLE "{{ params.dm_schema_name }}".employees_statistic;

WITH empl_empty_skills AS (
    -- Создаем временую таблицу, где для каждого сотрудника, прописываем все возмоные навыки для всех годов,
    -- начиная с 2019, со значением skill_level_id("{{ params.dds_schema_name }}".skills_levels) = 0
    SELECT
        "year",
        empl.id AS empl_id,
        s.id AS skill_id,
        0 AS skill_level_id
    FROM
        "{{ params.dds_schema_name }}".employees AS empl
    FULL OUTER JOIN "{{ params.dds_schema_name }}".skills AS s ON true
    FULL OUTER JOIN generate_series(
        cast(extract(YEAR FROM CURRENT_DATE) AS INT) - 5,
        cast(extract(YEAR FROM CURRENT_DATE) AS INT)
    ) AS "year" ON true
),
-- Создаем временую таблицу, где для каждого сотрудника, прописываем все существющие навыки с указанным годом получения,
-- начиная с 2019. Если год получени навыка меньше 2019, то ставим 2019. Данные берем из "{{ params.dds_schema_name }}".skills_levels
empl_filled_skills AS (
    SELECT
        sl.empl_id,
        sl.skill_id,
        greatest(extract(YEAR FROM sl.date), extract(YEAR FROM current_date) - 5) AS "year",
        MAX(sl.id) AS skill_level_id
    FROM
        "{{ params.dds_schema_name }}".skills_levels AS sl
    GROUP BY
        sl.empl_id,
        sl.skill_id,
        extract(YEAR FROM sl.date)
),
-- Создаем временую таблицу, где объединяем первые две временые таблицы (empl_empty_skills, empl_filled_skills)
slim_empl_skills AS (
    SELECT
        empty_skill."year",
        empty_skill.empl_id,
        empty_skill.skill_id,
        -- Так как при LEFT JOIN таблиц filled_skill.skill_level_id может быть NULL, используем coalesce (берет первое NOT NULL значение)
        max(coalesce(sl.level_id, 0)) OVER w AS level_id
    FROM
        empl_empty_skills AS empty_skill
    LEFT JOIN
        empl_filled_skills AS filled_skill
        ON empty_skill."year" = filled_skill."year" AND empty_skill.empl_id = filled_skill.empl_id AND empty_skill.skill_id = filled_skill.skill_id
    LEFT JOIN
        "{{ params.dds_schema_name }}".skills_levels AS sl
        ON filled_skill.skill_level_id = sl.id AND filled_skill.skill_level_id != 0
    WINDOW
        w AS (
            PARTITION BY empty_skill.empl_id, empty_skill.skill_id
            ORDER BY empty_skill."year"
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
),
-- Создаем временую таблицу, где добавляем данные из ODS леера
empl_skills AS (
    SELECT
        empl_skill."year" as start_year,
        empl_skill."year" as finish_year,
        empl.id AS empl_id,
        empl.first_name,
        empl.last_name,
        dep.id AS dep_id,
        dep.department,
        pos.id AS pos_id,
        pos.position,
        skill.id AS skill_id,
        skill.skill_name,
        lev.id AS level_id,
        lev.level AS level_name,
        lev.level_num,
        skill_group.id AS group_id,
        skill_group.skill_group_name AS group_name
    FROM
        slim_empl_skills AS empl_skill
    JOIN "{{ params.dds_schema_name }}".employees AS empl ON empl_skill.empl_id = empl.id
    JOIN "{{ params.dds_schema_name }}".departments AS dep ON empl.dep_id = dep.id
    JOIN "{{ params.dds_schema_name }}".position AS pos ON empl.pos_id = pos.id
    JOIN "{{ params.dds_schema_name }}".skills AS skill ON empl_skill.skill_id = skill.id
    JOIN "{{ params.dds_schema_name }}".levels AS lev ON empl_skill.level_id = lev.id
    JOIN "{{ params.dds_schema_name }}".skills_group AS skill_group ON skill.group_id = skill_group.id
),
-- Добавляем данные по количеству сотрудников в том же департаменте, на той же должности и с тем же скилом
slim_empl_stat AS (
    SELECT
        *,
        count(empl_id) OVER w AS empl_total_count,
        count(CASE WHEN level_num > 0 THEN 1 END) OVER w AS empl_count,
        count(CASE WHEN level_num = 1 THEN 1 END) OVER w AS empl_project_count,
        count(CASE WHEN level_num = 2 THEN 1 END) OVER w AS empl_novice_count,
        count(CASE WHEN level_num = 3 THEN 1 END) OVER w AS empl_junior_count,
        count(CASE WHEN level_num = 4 THEN 1 END) OVER w AS empl_middle_count,
        count(CASE WHEN level_num = 5 THEN 1 END) OVER w AS empl_senior_count,
        count(CASE WHEN level_num = 6 THEN 1 END) OVER w AS empl_expert_count
    FROM
        empl_skills
    WINDOW w AS (PARTITION BY dep_id, pos_id, skill_id, finish_year)
),
-- Добавляем данные по количеству сотрудников в том же департаменте, на той же должности и с тем же скилом
empl_stat AS (
    SELECT
        *,
        round(100 * coalesce(cast (empl_project_count AS NUMERIC) / nullif(empl_count, 0), 0), 2) AS empl_project_pct,
        round(100 * coalesce(cast (empl_novice_count AS NUMERIC) / nullif(empl_count, 0), 0), 2) AS empl_novice_pct,
        round(100 * coalesce(cast (empl_junior_count AS NUMERIC) / nullif(empl_count, 0), 0), 2) AS empl_junior_pct,
        round(100 * coalesce(cast (empl_middle_count AS NUMERIC) / nullif(empl_count, 0), 0), 2) AS empl_middle_pct,
        round(100 * coalesce(cast (empl_senior_count AS NUMERIC) / nullif(empl_count, 0), 0), 2) AS empl_senior_pct,
        round(100 * coalesce(cast (empl_expert_count AS NUMERIC) / nullif(empl_count, 0), 0), 2) AS empl_expert_pct,
        coalesce(
            cast ((
                1 * empl_project_count
                + 2 * empl_novice_count
                + 3 * empl_junior_count
                + 4 * empl_middle_count
                + 5 * empl_senior_count
                + 6 * empl_expert_count
            ) AS NUMERIC) / nullif(empl_count, 0),
            0
        ) AS avr_skill_level,
        'NOW' as marker,
        null:: INT as level_change,
        null:: INT as level_change_total
    FROM
        slim_empl_stat
),
empl_change_skill as (
 select
  start_data.start_year AS start_year,
  finish_data.start_year AS finish_year,
  start_data.empl_id,
  start_data.first_name,
  start_data.last_name,
  start_data.dep_id,
  start_data.department,
  start_data.pos_id,
  start_data.position,
  start_data.skill_id,
  start_data.skill_name,
  finish_data.level_id as level_id,
  finish_data.level_name as level_name,
  finish_data.level_num as level_num,
  start_data.group_id,
  start_data.group_name,
  null:: INT as empl_total_count,
  null:: INT as empl_count,
  null:: INT as empl_project_count,
  null:: INT as empl_novice_count,
  null:: INT as empl_junior_count,
  null:: INT as empl_middle_count,
  null:: INT as empl_senior_count,
  null:: INT as empl_expert_count,
  null:: NUMERIC(5, 2) as empl_project_pct,
  null:: NUMERIC(5, 2) as empl_novice_pct,
  null:: NUMERIC(5, 2) as empl_junior_pct,
  null:: NUMERIC(5, 2) as empl_middle_pct,
  null:: NUMERIC(5, 2) as empl_senior_pct,
  null:: NUMERIC(5, 2) as empl_expert_pct,
  null:: INT as avr_skill_level,
  'CHANGE' as marker,
  (finish_data.level_num % 10) - (start_data.level_num % 10) AS level_change
FROM
  empl_stat AS start_data
  CROSS JOIN empl_stat AS finish_data
WHERE
  start_data.start_year < finish_data.start_year
  AND start_data.empl_id = finish_data.empl_id
  AND start_data.skill_id = finish_data.skill_id
  AND (finish_data.level_num % 10) - (start_data.level_num % 10) > 0),
empl_sum_change_skills as(
SELECT
    *,
    SUM(sd.level_change) over (partition by sd.empl_id, sd.start_year, sd.finish_year)  as level_change_total
FROM
  empl_change_skill AS sd),
 statistic as (
 select * from empl_stat
 union all
 select * from empl_sum_change_skills
 )
 insert into
 "{{ params.dm_schema_name }}".employees_statistic (
    start_year,
    finish_year,
    empl_id,
    first_name,
    last_name,
    dep_id,
    department,
    pos_id,
    position,
    skill_id,
    skill_name,
    level_id,
    level_name,
    level_num,
    group_id,
    group_name,
    empl_total_count,
    empl_count,
    empl_project_count,
    empl_novice_count,
    empl_junior_count,
    empl_middle_count,
    empl_senior_count,
    empl_expert_count,
    empl_project_pct,
    empl_novice_pct,
    empl_junior_pct,
    empl_middle_pct,
    empl_senior_pct,
    empl_expert_pct,
    avr_skill_level,
    marker,
    level_change,
    level_change_total
  )
SELECT * FROM statistic;
