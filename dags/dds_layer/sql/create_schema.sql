BEGIN;

CREATE SCHEMA IF NOT EXISTS "{{ params.schema_name }}";

CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}".skills_group (
  id SERIAL PRIMARY KEY,
  skill_group_name VARCHAR(50) NOT NULL,
  UNIQUE (skill_group_name)
);

CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}".levels (
  id INT PRIMARY KEY,
  "level" VARCHAR(50) NOT NULL,
  level_num SMALLSERIAL NOT NULL,
  UNIQUE (level_num)
);

CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}".departments (
  id SERIAL PRIMARY KEY,
  department VARCHAR NOT NULL,
  UNIQUE (department)
);

CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}".position (
  id SERIAL PRIMARY KEY,
  position VARCHAR NOT NULL,
  UNIQUE (position)
);


CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}".skills (
  id SERIAL PRIMARY KEY,
  skill_name VARCHAR NOT NULL,
  external_id INT NOT NULL,
  group_id INT NOT NULL,
  CONSTRAINT fk_skills_skills_group
    FOREIGN KEY (group_id)
      REFERENCES "{{ params.schema_name }}".skills_group (id),
  UNIQUE (external_id),
  UNIQUE (skill_name, group_id, external_id)
);

CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}".employees (
  id INT PRIMARY KEY,
  last_name VARCHAR,
  first_name VARCHAR,
  FOC VARCHAR(50),
  dep_id INT NOT NULL,
  pos_id INT NOT NULL,
  CONSTRAINT fk_employees_departments
    FOREIGN KEY (dep_id)
      REFERENCES "{{ params.schema_name }}".departments (id),
  CONSTRAINT fk_employees_position
    FOREIGN KEY (pos_id)
      REFERENCES "{{ params.schema_name }}".position (id)
);

CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}".skills_levels (
  id SERIAL PRIMARY KEY,
  "date" DATE NOT NULL,
  skill_id INT NOT NULL,
  level_id INT NOT NULL,
  user_id INT NOT NULL,
  CONSTRAINT fk_skills_levels_skills
    FOREIGN KEY (skill_id)
      REFERENCES "{{ params.schema_name }}".skills (id),
  CONSTRAINT fk_skills_levels_levels
    FOREIGN KEY (level_id)
      REFERENCES "{{ params.schema_name }}".levels (id),
  CONSTRAINT fk_skills_levels_employees
    FOREIGN KEY (user_id)
      REFERENCES "{{ params.schema_name }}".employees (id),
  UNIQUE (skill_id, level_id, user_id)
);

CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}".cv_dar (
  id INT PRIMARY KEY,
  activity VARCHAR(10) NOT NULL,
  user_id INT NOT NULL,
  CONSTRAINT fk_cv_dar_employees
    FOREIGN KEY (user_id)
      REFERENCES "{{ params.schema_name }}".employees (id)
);

COMMIT;
