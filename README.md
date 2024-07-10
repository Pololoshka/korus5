# korus5

Что сделано:
- Инициализирован проект (poetry)
- Добавлен [Dockerfile](./Dockerfile) (для устанковки зависимостей в контейнер с airflow с помощью poetry)
- Подняты контейнеры с помошью [docker compose](./docker-compose.yaml)
- Добавлен документ [Архитектурное_решение](./documentation/architecture_solution.docx)
- Добавлена [Архитектурная_схема](./documentation/architecture_schema.drawio) в формате drawio
- Добавлена [Схема](./documentation/architecture_schema.drawio) слоя "ODS layer" в формате drawio
- Добавлен [DAG](./dags/ods_layer/ods_layer_transfer.py), предназначенный для создания слоя "ODS layer"
- Добавлен [DDL-скрипт](./dags/dds_layer/sql/create_schema.sql) для создания слоя "DDS layer"
- Добавлен [DAG](./dags/dds_layer/dds_layer_transfer.py), предназначенный для создания слоя "DDS layer"
- Добавлены Variables и Connections в docker-compose.yaml
