CREATE TABLE IF NOT EXISTS {{ params.dds_schema_name }}".employees (
    id INT PRIMARY KEY,
    last_name VARCHAR,
    first_name VARCHAR,
    foc VARCHAR(50),
    dep_id INT NOT NULL,
    pos_id INT NOT NULL,
    CONSTRAINT fk_employees_departments
    FOREIGN KEY (dep_id)
    REFERENCES {{ params.dds_schema_name }}".departments (
        id
    ),
    CONSTRAINT fk_employees_position
    FOREIGN KEY (pos_id)
    REFERENCES {{ params.dds_schema_name }}".position (
        id
    )
);

-- Creating temp table 'temp_employees' with all employees
CREATE TEMP TABLE temp_employees ON COMMIT DROP AS (
  SELECT
    u.id as id,
    u."фамилия" as last_name,
    u."имя" as first_name,
    u."цфо" as FOC,
    dep.id as dep_id,
    pos.id as pos_id,
    cv."Активность" as is_active_cv,
    row_to_json(u) as row_data
  FROM
    "{{ params.ods_schema_name }}"."сотрудники_дар" AS u
  LEFT JOIN "{{ params.dds_schema_name }}".departments AS dep ON dep.department = REPLACE (u."подразделения", '. ', '')
  LEFT JOIN "{{ params.dds_schema_name }}".position AS pos ON pos.position = u."должность"
  LEFT JOIN "{{ params.ods_schema_name }}"."резюмедар" AS cv ON cv."UserID" = u.id
);

-- Removing all invalid rows from temp table and filling 'failed_entities' table
WITH
  invalid_employes AS (
    DELETE FROM temp_employees
    WHERE
      is_active_cv != 'Да' OR is_active_cv IS NULL
    RETURNING *
  )
INSERT INTO
  "{{ params.dds_schema_name }}".failed_entities (entity_name, reason, entity)
SELECT
  'employees',
  'is_active_cv__is__false',
  row_data
FROM
  invalid_employes;

WITH
  invalid_employees AS (
    DELETE FROM temp_employees
    WHERE
      dep_id IS NULL
    RETURNING *
  )
INSERT INTO
  "{{ params.dds_schema_name }}".failed_entities (entity_name, reason, entity)
SELECT
  'employees',
  'dep_id__is__null',
  row_data
FROM
  invalid_employees;

WITH
  invalid_employes AS (
    DELETE FROM temp_employees
    WHERE
      pos_id IS NULL
    RETURNING *
  )
INSERT INTO
  "{{ params.dds_schema_name }}".failed_entities (entity_name, reason, entity)
SELECT
  'employees',
  'pos_id__is__null',
  row_data
FROM
  invalid_employes;

-- Filling 'employees' table with correct employees
INSERT INTO
  "{{ params.dds_schema_name }}".employees (id, last_name, first_name, FOC, dep_id, pos_id)
SELECT
  id,
  last_name,
  first_name,
  FOC,
  dep_id,
  pos_id
FROM
  temp_employees
ON CONFLICT (id) DO
UPDATE
SET
  last_name = EXCLUDED.last_name,
  first_name = EXCLUDED.first_name,
  dep_id = EXCLUDED.dep_id,
  pos_id = EXCLUDED.pos_id;
