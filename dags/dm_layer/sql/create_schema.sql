DROP SCHEMA "{{ params.dm_schema_name }}" CASCADE;
CREATE SCHEMA IF NOT EXISTS "{{ params.dm_schema_name }}";
