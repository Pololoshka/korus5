/* noqa: disable=L034 */
CREATE TABLE IF NOT EXISTS "{{ params.dm_schema_name }}".departments_statistic (
    id SERIAL PRIMARY KEY,
    "year" INT NOT NULL,
    empl_id INT NOT NULL,
    dep_id INT NOT NULL,
    skill_id INT NOT NULL,
    level_id INT,
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
    for_rate_empl_skill INT,
    employees_skill_pct NUMERIC(5, 2),
    CONSTRAINT fk_department_statistic_employees
    FOREIGN KEY (empl_id)
    REFERENCES "{{ params.dm_schema_name }}".employees (
        empl_id
    ),
    CONSTRAINT fk_department_statistic_departments
    FOREIGN KEY (dep_id)
    REFERENCES "{{ params.dm_schema_name }}".departments (
        dep_id
    ),
    CONSTRAINT fk_department_statistic_skills
    FOREIGN KEY (skill_id)
    REFERENCES "{{ params.dm_schema_name }}".skills (
        skill_id
    ),
    CONSTRAINT fk_department_statistic_skill_groups
    FOREIGN KEY (group_id)
    REFERENCES "{{ params.dm_schema_name }}".skill_groups (
        group_id
    ),
    CONSTRAINT fk_department_statistic_levels
    FOREIGN KEY (level_id)
    REFERENCES "{{ params.dm_schema_name }}".levels (
        level_id
    )
);

TRUNCATE TABLE "{{ params.dm_schema_name }}".departments_statistic RESTART IDENTITY;

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

-- Создаем временую таблицу, где добавляем данные из DDS леера
empl_skills AS (
    SELECT
        empl_skill."year",
        empl.id AS empl_id,
        empl.dep_id,
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
        count(CASE WHEN level_num > 2 THEN 1 END) OVER w AS for_rate_empl_skill,
        count(CASE WHEN level_num = 1 THEN 1 END) OVER w AS empl_project_count,
        count(CASE WHEN level_num = 2 THEN 1 END) OVER w AS empl_novice_count,
        count(CASE WHEN level_num = 3 THEN 1 END) OVER w AS empl_junior_count,
        count(CASE WHEN level_num = 4 THEN 1 END) OVER w AS empl_middle_count,
        count(CASE WHEN level_num = 5 THEN 1 END) OVER w AS empl_senior_count,
        count(CASE WHEN level_num = 6 THEN 1 END) OVER w AS empl_expert_count
    FROM
        empl_skills
    WINDOW w AS (PARTITION BY dep_id, skill_id, "year")
),

-- Добавляем данные по количеству сотрудников в том же департаменте, на той же должности и с тем же скилом
empl_stat AS (
    SELECT
        *,
        round(100 * coalesce(empl_project_count::NUMERIC / nullif(empl_count, 0), 0)) AS empl_project_pct,
        round(100 * coalesce(empl_novice_count::NUMERIC / nullif(empl_count, 0), 0)) AS empl_novice_pct,
        round(100 * coalesce(empl_junior_count::NUMERIC / nullif(empl_count, 0), 0)) AS empl_junior_pct,
        round(100 * coalesce(empl_middle_count::NUMERIC / nullif(empl_count, 0), 0)) AS empl_middle_pct,
        round(100 * coalesce(empl_senior_count::NUMERIC / nullif(empl_count, 0), 0)) AS empl_senior_pct,
        round(100 * coalesce(empl_expert_count::NUMERIC / nullif(empl_count, 0), 0)) AS empl_expert_pct,
        round(100 * coalesce(empl_count::NUMERIC / nullif(empl_total_count, 0), 0)) AS employees_skill_pct
    FROM
        slim_empl_stat
),

-- Считаем корректировку и определяем поле, в котором самый большой процент
department_totals AS (
    SELECT
        *,
        empl_project_pct + empl_novice_pct + empl_junior_pct + empl_middle_pct
        + empl_senior_pct + empl_expert_pct AS total_percentage,
        CASE
            WHEN (
                empl_project_pct + empl_novice_pct + empl_junior_pct + empl_middle_pct
                + empl_senior_pct + empl_expert_pct
            ) = 0
                THEN 0
            WHEN (
                empl_project_pct + empl_novice_pct + empl_junior_pct + empl_middle_pct
                + empl_senior_pct + empl_expert_pct
            ) != 100
                THEN (
                    100 - empl_project_pct - empl_novice_pct - empl_junior_pct - empl_middle_pct
                    - empl_senior_pct - empl_expert_pct
                )
            ELSE 0
        END AS adjustment_value,
        CASE
            WHEN
                empl_project_pct = greatest(
                    empl_project_pct, empl_novice_pct, empl_junior_pct, empl_middle_pct,
                    empl_senior_pct, empl_expert_pct
                )
                THEN 'empl_project_pct'
            WHEN
                empl_novice_pct = greatest(
                    empl_project_pct, empl_novice_pct, empl_junior_pct, empl_middle_pct,
                    empl_senior_pct, empl_expert_pct
                )
                THEN 'empl_novice_pct'
            WHEN
                empl_junior_pct = greatest(
                    empl_project_pct, empl_novice_pct, empl_junior_pct, empl_middle_pct,
                    empl_senior_pct, empl_expert_pct
                )
                THEN 'empl_junior_pct'
            WHEN
                empl_middle_pct = greatest(
                    empl_project_pct, empl_novice_pct, empl_junior_pct, empl_middle_pct,
                    empl_senior_pct, empl_expert_pct
                )
                THEN 'empl_middle_pct'
            WHEN
                empl_senior_pct = greatest(
                    empl_project_pct, empl_novice_pct, empl_junior_pct, empl_middle_pct,
                    empl_senior_pct, empl_expert_pct
                )
                THEN 'empl_senior_pct'
            WHEN
                empl_expert_pct = greatest(
                    empl_project_pct, empl_novice_pct, empl_junior_pct, empl_middle_pct,
                    empl_senior_pct, empl_expert_pct
                )
                THEN 'empl_expert_pct'
        END AS max_percentage_field
    FROM empl_stat
),

temp_empl_stat AS (
    SELECT
        dt."year",
        dt.empl_id,
        dt.dep_id,
        dt.skill_id,
        dt.level_id,
        dt.group_id,
        dt.empl_total_count,
        dt.empl_count,
        dt.for_rate_empl_skill,
        dt.empl_project_count,
        dt.empl_novice_count,
        dt.empl_junior_count,
        dt.empl_middle_count,
        dt.empl_senior_count,
        dt.empl_expert_count,
        dt.employees_skill_pct,
        CASE
            WHEN dt.max_percentage_field = 'empl_project_pct' AND dt.adjustment_value != 0
                THEN dt.empl_project_pct + dt.adjustment_value
            ELSE dt.empl_project_pct
        END AS empl_project_pct,
        CASE
            WHEN dt.max_percentage_field = 'empl_novice_pct' AND dt.adjustment_value != 0
                THEN dt.empl_novice_pct + dt.adjustment_value
            ELSE dt.empl_novice_pct
        END AS empl_novice_pct,
        CASE
            WHEN dt.max_percentage_field = 'empl_junior_pct' AND dt.adjustment_value != 0
                THEN dt.empl_junior_pct + dt.adjustment_value
            ELSE dt.empl_junior_pct
        END AS empl_junior_pct,
        CASE
            WHEN dt.max_percentage_field = 'empl_middle_pct' AND dt.adjustment_value != 0
                THEN dt.empl_middle_pct + dt.adjustment_value
            ELSE dt.empl_middle_pct
        END AS empl_middle_pct,
        CASE
            WHEN dt.max_percentage_field = 'empl_senior_pct' AND dt.adjustment_value != 0
                THEN dt.empl_senior_pct + dt.adjustment_value
            ELSE dt.empl_senior_pct
        END AS empl_senior_pct,
        CASE
            WHEN dt.max_percentage_field = 'empl_expert_pct' AND dt.adjustment_value != 0
                THEN dt.empl_expert_pct + dt.adjustment_value
            ELSE dt.empl_expert_pct
        END AS empl_expert_pct
    FROM department_totals AS dt
)

INSERT INTO
"{{ params.dm_schema_name }}".departments_statistic (
    "year",
    empl_id,
    dep_id,
    skill_id,
    level_id,
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
    for_rate_empl_skill,
    employees_skill_pct
)
SELECT
    year + 1 AS "year",
    empl_id,
    dep_id,
    skill_id,
    level_id,
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
    for_rate_empl_skill,
    employees_skill_pct
FROM temp_empl_stat;
