BEGIN;
INSERT INTO dds_polina.skills_group (skill_group_name)
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


INSERT INTO dds_polina.levels ("level", level_num)
VALUES
 SELECT "название", id
 FROM ods_polina."уровни_знаний"
 WHERE DATE("Дата изм.") > DATE(0)
ON CONFLICT (level_num) DO UPDATE
SET "level" = EXCLUDED."level";

INSERT INTO dds_polina.departments (department)
VALUES
 SELECT DISTINCT REPLACE("подразделения", '. ', '')
 FROM ods_polina."сотрудники_дар"
ON CONFLICT (department) DO NOTHING;
COMMIT;
