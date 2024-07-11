"""
- Создает схему "ods_polina" в БД "etl_db_5", если её не существует (SQLExecuteQueryOperator)
- Параллельно для каждой таблицы (GenericTransfer)
    - Создает таблицу в БД "etl_db_5", если её не сущестует и очищает в ней данные
    - Достает записи из таблицы в БД "source.source_data" и добавляет их в БД "etl_db_5.ods_polina"
"""

from datetime import UTC, datetime
from typing import TypedDict

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator


class StageVar(TypedDict):
    schema_name: str
    source_conn_id: str
    destination_conn_id: str
    tables: list[str]
    previous_run_at: str


const = StageVar(
    schema_name="ods_polina",
    source_conn_id="source",
    destination_conn_id="etl_db_5",
    tables=[],
    previous_run_at=datetime.now(tz=UTC).isoformat(),
)


DAG_ID = "to-delete"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 7, 7, tzinfo=UTC),
    schedule="@once",
    tags=["korus5", "STG"],
    catchup=False,
    params={"schema_name": "foo"},
) as dag:
    init = EmptyOperator(task_id="init")

    edge = EmptyOperator(task_id="edge")
    init >> edge
