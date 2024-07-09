CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}"."{{ params.table_name }}" (
  id int4 NULL,
  "Дата рождения" text NULL,
  активность text NULL,
  пол text NULL,
  фамилия text NULL,
  имя text NULL,
  "Последняя авторизация" text NULL,
  должность text NULL,
  цфо text NULL,
  "Дата регистрации" text NULL,
  "Дата изменения" text NULL,
  подразделения text NULL,
  "E-Mail" text NULL,
  логин text NULL,
  компания text NULL,
  "Город проживания" text NULL
);
