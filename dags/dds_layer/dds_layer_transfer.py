"""
- Создает схему "dds_polina" в БД "etl_db_5", если её не существует (SQLExecuteQueryOperator)
- Достает записи из таблицы в БД "etl_db_5.ods_polina" и добавляет их в БД "etl_db_5.dds_polina",
согласно схеме, перед этим проводит очистку данных
"""

import datetime

from airflow.models import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import constants as c
from dds_layer.dds_layer_transfer_constants import TABLES

with DAG(
    dag_id="dds_layer_transfer",
    start_date=datetime.datetime(2024, 7, 7, tzinfo=datetime.UTC),
    schedule=None,
    tags=["korus5", "DDS"],
    catchup=False,
    params={
        "ods_schema_name": c.ODS_SCHEMA_NAME,
        "dds_schema_name": c.DDS_SCHEMA_NAME,
    },
) as dag:
    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id=c.CONN_ID,
        sql="sql/create_schema.sql",
    )
    transfer_data = SQLExecuteQueryOperator(
        task_id="transfer_data",
        conn_id=c.CONN_ID,
        autocommit=False,
        sql=[f"sql/{table}/query.sql" for table in TABLES],
    )

    create_schema >> transfer_data
