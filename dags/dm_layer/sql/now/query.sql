CREATE TABLE IF NOT EXISTS "{{ params.dm_schema_name }}"."now" (
  id SERIAL PRIMARY KEY,
  "year" INT NOT NULL,
  year_for_ui INT NOT NULL,
  empl_id INT NOT NULL,
  dep_id INT NOT NULL,
  pos_id INT NOT NULL,
  change_id INT NOT NULL,
  group_id INT NOT NULL,
  skill_id INT NOT NULL,
  level_id INT NOT NULL,
  level_num INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_now ON "{{ params.dm_schema_name }}"."now" (year_for_ui, empl_id, dep_id, pos_id, skill_id);

TRUNCATE TABLE "{{ params.dm_schema_name }}"."now";

with
  full_table AS (
    -- Создаем временую таблицу, где для каждого сотрудника, прописываем все возмоные навыки для всех годов,
    -- начиная с 2019, со значением change_id(dds_polina.skills_levels) = 0
    SELECT
      "year" AS "year",
      empl.id AS empl_id,
      s.id AS skill_id,
      0 AS change_id
    FROM
      dds_polina.employees AS empl
      FULL OUTER JOIN dds_polina.skills AS s ON TRUE
      FULL OUTER JOIN generate_series (2019, CAST(EXTRACT(YEAR FROM current_date) AS INT)) AS "year" ON TRUE
  ),
  current_levels AS
  -- Создаем временую таблицу, где для каждого сотрудника, прописываем все существющие навыки с указанным годом получения,
  -- начиная с 2019. Если год получени навыка меньше 2019, то ставим 2019. Данные берем из dds_polina.skills_levels
  (
    SELECT
      Greatest (EXTRACT(YEAR FROM sl.date), 2019) AS "year",
      sl.empl_id AS empl_id,
      sl.skill_id AS skill_id,
      MAX(sl.id) AS change_id
    FROM
      dds_polina.skills_levels AS sl
    GROUP BY
      sl.empl_id,
      sl.skill_id,
      EXTRACT(YEAR FROM sl.date)
  ),
  -- Создаем временую таблицу, где объединяем первые две временые таблицы (full_table, current_levels)
  combain_table AS (
    SELECT
      ft.empl_id,
      ft.skill_id,
      ft."year",
    -- Так как при LEFT JOIN таблиц cl.change_id может быть NULL, используем coalesce (берет первое NOT NULL значение)
      coalesce(cl.change_id, ft.change_id) AS change_id
    FROM
      full_table AS ft
      LEFT JOIN current_levels AS cl USING (empl_id, skill_id, "year")
  ),
  -- Создаем временую таблицу, которая полностью подготавливает данные для INSERT
  data_to_isert AS (
    SELECT
      ct.empl_id,
      ct.skill_id,
      ct."year",
      ct."year" + 1 AS year_for_ui,
      max(ct.change_id) OVER w AS change_id,
      empl.dep_id AS dep_id,
      empl.pos_id AS pos_id,
      skills.group_id AS group_id,
    -- Так как при LEFT JOIN таблиц sl.level_id может быть NULL, используем coalesce (берет первое NOT NULL значение)
      max(coalesce(sl.level_id, 0)) OVER w AS level_id,
      max(levels.level_num) OVER w AS level_num
    FROM
      combain_table AS ct
      JOIN "{{ params.dds_schema_name }}".employees AS empl ON empl.id = ct.empl_id
      JOIN "{{ params.dds_schema_name }}".skills AS skills ON skills.id = ct.skill_id
      LEFT JOIN "{{ params.dds_schema_name }}".skills_levels AS sl ON sl.id = ct.change_id AND ct.change_id != 0
      LEFT JOIN "{{ params.dds_schema_name }}".levels AS levels ON levels.id = coalesce(sl.level_id, 0)
    -- Создаем PARTITION, чтобы к каждому следующему году после получения навыка, если он не обновлялся, добавлялось максимальное значение за предыдущие года
    WINDOW
      w AS (
        PARTITION BY
          ct.empl_id,
          ct.skill_id
        ORDER BY
          ct."year" ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW
      )
  )
INSERT INTO
  "{{ params.dm_schema_name }}"."now" (
    empl_id,
    skill_id,
    "year",
    year_for_ui,
    change_id,
    dep_id,
    pos_id,
    group_id,
    level_id,
    level_num
  )
SELECT * FROM data_to_isert;
