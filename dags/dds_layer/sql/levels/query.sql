CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".levels (
    id INT PRIMARY KEY,
    level VARCHAR NOT NULL,
    level_num INT NOT NULL,
    UNIQUE (level_num)
);

INSERT INTO
"{{ params.dds_schema_name }}".levels (
    id, level, level_num
)
VALUES
(0, 'Не владею', 0),
(283045, 'Использовал на проекте', 1),
(115637, 'Novice', 2),
(115638, 'Junior', 3),
(115639, 'Middle', 4),
(115640, 'Senior', 5),
(115641, 'Expert', 6)
ON CONFLICT (id) DO
UPDATE
SET
level = excluded.level;
