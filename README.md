# korus5

Что сделано:
- Инициализирован проект (poetry)
- Добавлен [Dockerfile](./Dockerfile) (для устанковки зависимостей в контейнер с airflow с помощью poetry)
- Подняты контейнеры с помошью [docker compose](./docker-compose.yaml)
- Создан тестовый [DAG](./dags/task1/dag.py)
