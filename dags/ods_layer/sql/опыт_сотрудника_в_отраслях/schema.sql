CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}"."{{ params.table_name }}" (
	"User ID" int4 NULL,
	активность varchar(50) NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar(50) NULL,
	id int4 NULL,
	дата varchar(50) NULL,
	отрасли varchar(50) NULL,
	"Уровень знаний в отрасли" varchar(128) NULL
);
