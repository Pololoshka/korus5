CREATE TABLE IF NOT EXISTS
  "{{ params.dm_schema_name }}".employees_new_skils_total (
    id SERIAL PRIMARY KEY,
    start_year INT NOT NULL,
    finish_year INT NOT NULL,
    empl_id INT NOT NULL,
    dep_id INT NOT NULL,
    pos_id INT NOT NULL,
    level_change_total INT NOT NULL
  );

TRUNCATE TABLE "{{ params.dm_schema_name }}".employees_new_skils_total;

INSERT INTO
  "{{ params.dm_schema_name }}".employees_new_skils_total (
    start_year,
    finish_year,
    empl_id,
    dep_id,
    pos_id,
    level_change_total
  )
SELECT
    start_year,
    finish_year,
    empl_id,
    dep_id,
    pos_id,
    SUM(level_change) as level_change_total
FROM
  "{{ params.dm_schema_name }}"."compare" AS sd
 GROUP BY (empl_id, start_year, finish_year, dep_id, pos_id)
