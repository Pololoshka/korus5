CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".skills_group (
    id SERIAL PRIMARY KEY,
    skill_group_name VARCHAR(50) NOT NULL,
    UNIQUE (skill_group_name)
);

TRUNCATE TABLE "{{ params.dds_schema_name }}".skills_group CASCADE;

INSERT INTO
"{{ params.dds_schema_name }}".skills_group (
    skill_group_name
)
VALUES
('база данных'),
('инструмент'),
('платформа'),
('среда разработки'),
('технология'),
('тип системы'),
('фреймворк'),
('язык программирования')
ON CONFLICT (skill_group_name) DO NOTHING;
