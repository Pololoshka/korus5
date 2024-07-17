CREATE TABLE IF NOT EXISTS "{{ params.dds_schema_name }}".cv_dar (
    id INT PRIMARY KEY,
    activity VARCHAR(10) NOT NULL,
    user_id INT NOT NULL,
    CONSTRAINT fk_cv_dar_employees
    FOREIGN KEY (user_id)
    REFERENCES "{{ params.dds_schema_name }}".employees (
        id
    )
);

INSERT INTO
"{{ params.dds_schema_name }}".cv_dar (
    id, activity, user_id
)
SELECT
    cv."ResumeID",
    cv."Активность",
    u.id
FROM
"{{ params.ods_schema_name }}"."резюмедар" AS cv
INNER JOIN
    "{{ params.dds_schema_name }}".employees AS u
    ON cv."UserID" = u.id
WHERE cv."Активность" = 'Да'
ON CONFLICT (id) DO
UPDATE
SET
activity = excluded.activity;
