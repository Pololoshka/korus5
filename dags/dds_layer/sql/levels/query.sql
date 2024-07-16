CREATE TABLE IF NOT EXISTS {{ params.dds_schema_name }}".levels (
    id INT PRIMARY KEY,
    level VARCHAR NOT NULL,
    level_num INT NOT NULL,
    UNIQUE (level_num)
);

INSERT INTO
{{ params.dds_schema_name }}".levels (
    id, level, level_num
)
VALUES
(0, 'Не владею', 0),
(283045, 'Использовал на проекте', 1),
(115637, 'Novice', 2),
(115638, 'Junior', 3),
(115639, 'Middle', 4),
(115640, 'Senior', 5),
(115641, 'Expert', 6),
(115772, 'Я знаю специфику отрасли', 11),
(115773, 'Я знаю специфику отрасли и переложил это на систему', 12),
(
    115774,
    'Я знаю специфику отрасли, могу переложить на систему, убедить клиента и реализовать',
    13
),
(115761, 'Я знаю предметную область', 21),
(
    115762,
    'Я знаю все особенности предметной области и могу переложить это на систему',
    22
),
(
    115763,
    'Я знаю специфику предметной области, могу переложить на систему, убедить клиента и реализовать',
    23
)
ON CONFLICT (id) DO
UPDATE
SET
level = excluded.level;
