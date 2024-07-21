# korus5 Анализ данных об уровне знаний сотрудников ДАР

Backend часть проекта реализована с помощью оркестратора Apache Airflow, который использует язык программирования Python.
В качестве СУБД используется PostgreSQL.
Backend часть проекта преназначена для создания Хранилища данных с целью последующего использования для аналитики. Для загрузки данных используется ETL процесс.

![Code Check Actions](https://github.com/Pololoshka/korus5/actions/workflows/check-code.yml/badge.svg)

# Содержание

- [Технологии](#технологии)
- [Структура проекта](#стурктура)
- [Требования](#требования)
- [Установка зависимостей](#poetry)
- [Развертывание Apache Airflow](#деплой)
- [Форматирование и проверка кода](#форматирование)
- [To do](#todo)
- [Команда](#команда)

# Технологии

<a name="технологии"></a>

- Python
- Apache Airflow
- Poetry
- Ruff
- Docker
- Docker Compose

# Структура проекта

<a name="структура"></a>
В папке [dags](./dags/) содержатся все файлы с DAGs, а также инструкции, которые выполняются внутри них, для обработки данных и формарования всех слоев ETL процесса.
Основным файлом, который последовательно запускет процессы для формирования слоев ODS, DDS и DM является [entrypoint_dag](./dags/entrypoint_dag.py). В нем с помощью операторов TriggerDagRunOperator последовательно запускаются следующие DAGs:

- ["ods_layer_transfer"](./dags/ods_layer/ods_layer_transfer.py).
  Предназначен для формирования слоя "ODS layer", который содержит в себе данные, загруженные "as is" из семпла с исходными данными.
  Здесь используются оператор SQLExecuteQueryOperator для создания схемы, а также операторы TaskGroup и GenericTransfer для параллельной загрузки данных из семла в Базу Данных проекта.
  Все файлы с sql-скриптами, используемыми в операторах, хранятся с соответствующей [папке](./dags/ods_layer/sql/).
- ["dds_layer_transfer"](./dags/dds_layer/dds_layer_transfer.py)
  Предназначен для формирования слоя "DDS layer". Он содержит скрипт, который читает данные из слоя "ODS layer", затем в соответствии с постановкой аналитиков реализует подготовку (маппинг, очистку, фильтрацию) данных и сохраняет результат в слой "DDS layer". Здесь используется оператор SQLExecuteQueryOperator для создания схемы, также для обработки и загрузки данных в слой "DDS layer".
  Все файлы с sql-скриптами, используемыми в DAG, хранятся с соответствующей [папке](./dags/dds_layer/sql/).
- ["dm_layer_transfer"](./dags/dm_layer/dm_layer_transfer.py)
  Предназначен для формирования слоя "DM layer", который используется для расчета витрины. Он содержит скрипт, который вычитывает данные из слоя "DDS layer", производит необходимые расчеты и агрегации и сохраняет результат в целевые таблицы слоя "DM layer". Здесь используется оператор SQLExecuteQueryOperator для создания схемы, также для обработки и загрузки данных в слой "DM layer".
  Все файлы с sql-скриптами, используемыми в DAG, хранятся с соответствующей [папке](./dags/dds_layer/sql/).

В папке [config](./config/) содержатся конфигурации для подключения к Базам Данных, которые автоматически пробрасываются в Apache Airflow с помощью сервиса airflow-init-variables-dev.

В папке [documentation](./documentation/) содержится [Документ](./documentation/architecture_solution.docx) "Архитекурное решение" и другие сопутсвующие файлы.

Файлы [Dockerfile](./Dockerfile) и [docker_compose.ymal](./docker-compose.yaml) необходимы запуска Apache Airflow в конетейнере с использoванием [Poetry](./pyproject.toml).

Файл [pre-commit-config.yaml](.pre-commit-config.yaml) необходим для форматирования кода перед коммитом. В нем используются следующие проверки: ruff, mypy и sqlfluff.

В файле [check-code.yml](.github/workflows/check-code.yml) содержатся инструкции для запуска проверки кода в GitHub на Push и Pull Request.

# Требования

<a name="требования"></a>

Для установки и запуска проекта в режиме разработки необходим
[Python3.12](https://www.python.org/downloads/release/python-3120/), а также [Poetry](https://python-poetry.org/).

# Установка зависимостей

<a name="poetry"></a>

Для установки зависимостей и активации виртуального окружения выполните команду:

```bash
poetry install

poetry shell
```

# Развертывание Apache Airflow

<a name="деплой"></a>

Для развертывания Apache Airflow в проекте используется Docker Compose.
Перед тем как развернуть Apache Airflow в Docker, проверьте, что в файле [конфигураций] (./config/connections.json) прописаны верные данные для соединений с Базами Данных. Так как эти конекшены будут автоматически проброшены в Apache Airflow с помощью сервиса airflow-init-variables-dev и будут использовать для дальнейшего подключения к Базам Данных.
Для запуска Apache Airflow используйте команду:

```bash
docker compose up -d
```

Для открытия Apache Airflow в браузере в адресную строку введите: http://localhost:8080
Логин: airflow
Пароль: airflow

# Форматирование и проверка кода

<a name="форматирование"></a>

Для форматирования кода используйте команды из файла [Makefile](Makefile):

```bash
make fix
make check
```

# To do

<a name="todo"></a>

- [x] Инициализировать проект (Poetry)
- [x] Добавить [Dockerfile](./Dockerfile) (для устанковки зависимостей в контейнер с Airflow с помощью Poetry)
- [x] Поднять контейнер с помошью [Docker Compose](./docker-compose.yaml)
- [x] Написать документ [Архитектурное_решение](./documentation/architecture_solution.docx)
- [x] Добавить [DAG](./dags/ods_layer/ods_layer_transfer.py), предназначенный для создания слоя "ODS layer"
- [x] Добавить [DAG](./dags/dds_layer/dds_layer_transfer.py), предназначенный для создания слоя "DDS layer"
- [x] Обработать некорректные данные во время формирования слоя "DDS layer".
- [x] Добавить [DAG](./dags/dm_layer/dm_layer/dm_layer_transfer.py), предназначенный для создания слоя "DM layer".
- [x] Добавить [DAG](./dags/entrypoint_dag.py), предназначенный для последовательно запуска всех процессов раз в день.

# Команда проекта

<a name="команда"></a>
[Соколова Полина — Data Engineer](https://github.com/Pololoshka)

[Унучек Ксения — Data Engineer](https://github.com/KseniyaUnuchek)
