CREATE TABLE IF NOT EXISTS "{{ params.schema_name }}"."{{ params.table_name }}" (
    "User ID" int4 NULL,
    "активность" text NULL,
    "Сорт." int4 NULL,
    "Дата изм." text NULL,
    id int4 NULL,
    "Год сертификата" int4 NULL,
    "Наименование сертификата" text NULL,
    "Организация, выдавшая сертификат" text NULL
);
