CREATE TABLE IF NOT EXISTS "{{ params.dm_schema_name }}".employees_statistic (
    id SERIAL PRIMARY KEY,
    start_year INT NOT NULL,
    finish_year INT NOT NULL,
    empl_id INT NOT NULL,
    dep_id INT NOT NULL,
    pos_id INT NOT NULL,
    skill_id INT NOT NULL,
    level_id INT,
    level_num INT,
    group_id INT NOT NULL,
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
    marker VARCHAR NOT NULL,
    CONSTRAINT fk_employees_statistic_employees
    FOREIGN KEY (empl_id)
    REFERENCES "{{ params.dm_schema_name }}".employees (
        empl_id
    ),
    CONSTRAINT fk_employees_statistic_departments
    FOREIGN KEY (dep_id)
    REFERENCES "{{ params.dm_schema_name }}".departments (
        dep_id
    ),
    CONSTRAINT fk_employees_statistic_positions
    FOREIGN KEY (pos_id)
    REFERENCES "{{ params.dm_schema_name }}".positions (
        pos_id
    ),
    CONSTRAINT fk_employees_statistic_skills
    FOREIGN KEY (skill_id)
    REFERENCES "{{ params.dm_schema_name }}".skills (
        skill_id
    ),
    CONSTRAINT fk_employees_statistic_skill_groups
    FOREIGN KEY (group_id)
    REFERENCES "{{ params.dm_schema_name }}".skill_groups (
        group_id
    ),
    CONSTRAINT fk_employees_statistic_levels
    FOREIGN KEY (level_id)
    REFERENCES "{{ params.dm_schema_name }}".levels (
        level_id
    )
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
        extract(YEAR FROM current_date)::INT - 5,
        extract(YEAR FROM current_date)::INT
    ) AS "year" ON true
),

-- Создаем временую таблицу, где для каждого сотрудника, прописываем все существющие навыки с указанным годом получения,
-- начиная с 2019. Если год получени навыка меньше 2019, то ставим 2019. Данные берем из "{{ params.dds_schema_name }}".skills_levels
empl_filled_skills AS (
    SELECT
        sl.empl_id,
        sl.skill_id,
        greatest(
            extract(YEAR FROM (sl.date - INTERVAL '4 month')),
            extract(YEAR FROM (current_date - INTERVAL '4 month')) - 5
        ) AS "year",
        max(sl.id) AS skill_level_id
    FROM
        "{{ params.dds_schema_name }}".skills_levels AS sl
    GROUP BY
        sl.empl_id,
        sl.skill_id,
        extract(YEAR FROM (sl.date - INTERVAL '4 month'))
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
        ON
            empty_skill."year" = filled_skill."year"
            AND empty_skill.empl_id = filled_skill.empl_id
            AND empty_skill.skill_id = filled_skill.skill_id
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
        empl_skill."year" AS start_year,
        empl_skill."year" AS finish_year,
        empl.id AS empl_id,
        empl.dep_id,
        empl.pos_id,
        skill.id AS skill_id,
        lev.id AS level_id,
        lev.level_num,
        skill.group_id
    FROM
        slim_empl_skills AS empl_skill
    INNER JOIN "{{ params.dds_schema_name }}".employees AS empl ON empl_skill.empl_id = empl.id
    INNER JOIN "{{ params.dds_schema_name }}".skills AS skill ON empl_skill.skill_id = skill.id
    INNER JOIN "{{ params.dds_schema_name }}".levels AS lev ON empl_skill.level_id = lev.id
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
        round(100 * coalesce(empl_project_count::NUMERIC / nullif(empl_count, 0), 0), 2) AS empl_project_pct,
        round(100 * coalesce(empl_novice_count::NUMERIC / nullif(empl_count, 0), 0), 2) AS empl_novice_pct,
        round(100 * coalesce(empl_junior_count::NUMERIC / nullif(empl_count, 0), 0), 2) AS empl_junior_pct,
        round(100 * coalesce(empl_middle_count::NUMERIC / nullif(empl_count, 0), 0), 2) AS empl_middle_pct,
        round(100 * coalesce(empl_senior_count::NUMERIC / nullif(empl_count, 0), 0), 2) AS empl_senior_pct,
        round(100 * coalesce(empl_expert_count::NUMERIC / nullif(empl_count, 0), 0), 2) AS empl_expert_pct,
        coalesce(
            (
                1 * empl_project_count
                + 2 * empl_novice_count
                + 3 * empl_junior_count
                + 4 * empl_middle_count
                + 5 * empl_senior_count
                + 6 * empl_expert_count
            )::NUMERIC / nullif(empl_count, 0),
            0
        ) AS avr_skill_level,
        'NOW' AS marker,
        null::INT AS level_change,
        null::INT AS level_change_total
    FROM
        slim_empl_stat
),

empl_change_skill AS (
    SELECT
        start_data.start_year,
        finish_data.start_year AS finish_year,
        start_data.empl_id,
        start_data.dep_id,
        start_data.pos_id,
        start_data.skill_id,
        finish_data.level_id,
        finish_data.level_num,
        start_data.group_id,
        null::INT AS empl_total_count,
        null::INT AS empl_count,
        null::INT AS empl_project_count,
        null::INT AS empl_novice_count,
        null::INT AS empl_junior_count,
        null::INT AS empl_middle_count,
        null::INT AS empl_senior_count,
        null::INT AS empl_expert_count,
        null::NUMERIC(5, 2) AS empl_project_pct,
        null::NUMERIC(5, 2) AS empl_novice_pct,
        null::NUMERIC(5, 2) AS empl_junior_pct,
        null::NUMERIC(5, 2) AS empl_middle_pct,
        null::NUMERIC(5, 2) AS empl_senior_pct,
        null::NUMERIC(5, 2) AS empl_expert_pct,
        null::INT AS avr_skill_level,
        'CHANGE' AS marker,
        finish_data.level_num - start_data.level_num AS level_change
    FROM
        empl_stat AS start_data
    CROSS JOIN empl_stat AS finish_data
    WHERE
        start_data.finish_year < finish_data.finish_year
        AND start_data.empl_id = finish_data.empl_id
        AND start_data.skill_id = finish_data.skill_id
        AND finish_data.level_num - start_data.level_num > 0
),

empl_sum_change_skills AS (
    SELECT
        *,
        sum(sd.level_change) OVER (PARTITION BY sd.empl_id, sd.start_year, sd.finish_year) AS level_change_total
    FROM
        empl_change_skill AS sd
),

statistic AS (
    SELECT * FROM empl_stat
    UNION ALL
    SELECT * FROM empl_sum_change_skills
)

INSERT INTO
"{{ params.dm_schema_name }}".employees_statistic (
    start_year,
    finish_year,
    empl_id,
    dep_id,
    pos_id,
    skill_id,
    level_id,
    level_num,
    group_id,
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
SELECT
    start_year + 1 AS start_year,
    finish_year + 1 AS finish_year,
    empl_id,
    dep_id,
    pos_id,
    skill_id,
    level_id,
    level_num,
    group_id,
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
FROM statistic;
