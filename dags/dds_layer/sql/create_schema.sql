DROP SCHEMA "{{ params.dds_schema_name }}" CASCADE;
CREATE SCHEMA IF NOT EXISTS "{{ params.dds_schema_name }}";
