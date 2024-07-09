CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}"."{{ params.table_name }}" (
  "UserID" int4 NULL,
  "ResumeID" int4 NULL,
  "Активность" text NULL,
  "Образование" text NULL,
  "Сертификаты/Курсы" text NULL,
  "Языки" text NULL,
  "Базыданных" text NULL,
  "Инструменты" text NULL,
  "Отрасли" text NULL,
  "Платформы" text NULL,
  "Предметныеобласти" text NULL,
  "Средыразработки" text NULL,
  "Типысистем" text NULL,
  "Фреймворки" text NULL,
  "Языкипрограммирования" text NULL,
  "Технологии" text NULL
);
