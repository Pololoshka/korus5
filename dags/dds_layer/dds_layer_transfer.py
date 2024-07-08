"""
- Создает схему "dds_polina" в БД "etl_db_5", если её не существует (SQLExecuteQueryOperator)
"""

import datetime

import dds_layer.dds_layer_transfer_const as c
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="dds_layer_transfer",
    start_date=datetime.datetime(2024, 7, 7, tzinfo=datetime.UTC),
    schedule="@once",
    tags=["korus5", "STG"],
    catchup=False,
    params={"schema_name": c.SCHEMA_NAME},
) as dag:

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id=c.CONN_ID,
        sql="sql/create_schema.sql",
    )

    create_schema >> EmptyOperator(task_id="edge")
