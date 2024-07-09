CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}"."{{ params.table_name }}" (
  название text NULL,
  активность text NULL,
  "Сорт." int4 NULL,
  "Дата изм." text NULL,
  id int4 NULL,
  язык text NULL,
  "Уровень знаний ин. языка" text NULL
);
