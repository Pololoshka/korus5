"""
- Создает схему "ods_polina" в БД "etl_db_5", если её не существует (SQLExecuteQueryOperator)
- Параллельно для каждой таблицы (GenericTransfer)
    - Создает таблицу в БД "etl_db_5", если её не существует и очищает в ней данные
    - Достает записи из таблицы в БД "source.source_data" и добавляет их в БД "etl_db_5.ods_polina"
"""

import datetime

import stage_layer.const as c
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="ods_layer_transfer",
    start_date=datetime.datetime(2024, 7, 7, tzinfo=datetime.UTC),
    schedule="@once",
    tags=["korus5", "STG"],
    catchup=False,
    params={"schema_name": c.SCHEMA_NAME},
) as dag:
    init = EmptyOperator(task_id="init")

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id=c.DESTINATION_CONN_ID,
        sql="sql/create_schema.sql",
    )
    load_upload_data = [
        GenericTransfer(
            task_id=f"load_table_{table}",
            sql=f"sql/{table}/query.sql",
            destination_table=f'"{c.SCHEMA_NAME}"."{table}"',
            destination_conn_id=c.DESTINATION_CONN_ID,
            source_conn_id=c.SOURCE_CONN_ID,
            preoperator=[f"sql/{table}/schema.sql", "sql/truncate.sql"],
            params={"table_name": table},
        )
        for table in c.TABLES
    ]

    edge = EmptyOperator(task_id="edge")
    init >> create_schema >> load_upload_data >> edge
