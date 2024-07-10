"""
- Создает схему "dds_polina" в БД "etl_db_5", если её не существует (SQLExecuteQueryOperator)
- Достает записи из таблицы в БД "etl_db_5.ods_polina" и добавляет их в БД "etl_db_5.dds_polina",
согласно схеме, перед этим проводит очистку данных
"""

import datetime

from airflow.models import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import dds_layer.dds_layer_transfer_const as c

with DAG(
    dag_id="dds_layer_transfer",
    start_date=datetime.datetime(2024, 7, 7, tzinfo=datetime.UTC),
    schedule="@once",
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
    load_upload_data = SQLExecuteQueryOperator(
        task_id="load_upload_data",
        conn_id=c.CONN_ID,
        sql="sql/query.sql",
    )

    # TODO: dds_layer_transfer.previous_executed_at to  datetime.now()

    create_schema >> load_upload_data
