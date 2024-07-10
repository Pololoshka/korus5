CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".employees (
  id INT PRIMARY KEY,
  last_name VARCHAR,
  first_name VARCHAR,
  FOC VARCHAR(50),
  dep_id INT NOT NULL,
  pos_id INT NOT NULL,
  CONSTRAINT fk_employees_departments
    FOREIGN KEY (dep_id)
      REFERENCES "{{ params.dds_schema_name }}".departments (id),
  CONSTRAINT fk_employees_position
    FOREIGN KEY (pos_id)
      REFERENCES "{{ params.dds_schema_name }}".position (id)
);

INSERT INTO
  "{{ params.dds_schema_name }}".employees (id, last_name, first_name, FOC, dep_id, pos_id)
SELECT
  u.id,
  u."фамилия",
  u."имя",
  u."цфо",
  dep.id,
  pos.id
FROM
  "{{ params.ods_schema_name }}"."сотрудники_дар" AS u
  JOIN "{{ params.dds_schema_name }}".position AS pos ON pos.position = u."должность"
  JOIN "{{ params.dds_schema_name }}".departments AS dep ON dep.department = REPLACE (u."подразделения", '. ', '')
  JOIN "{{ params.ods_schema_name }}"."резюмедар" AS cv ON cv."UserID" = u.id
  WHERE cv."Активность" = 'Да'
ON CONFLICT (id) DO
UPDATE
SET
  last_name = EXCLUDED.last_name,
  first_name = EXCLUDED.first_name,
  dep_id = EXCLUDED.dep_id,
  pos_id = EXCLUDED.pos_id;

