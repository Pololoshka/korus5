"""
- Подключает к БД source
- Достает запись из таблицы source_data.сотрудники_дар (SQLExecuteQueryOperator)
- Логирует полученные данные (PythonOperator)
"""

import datetime
import logging
from typing import Any

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)

DAG_ID = "initial_dag"


def handle_data(**kwargs: Any) -> None:
    results = kwargs["ti"].xcom_pull(task_ids="read")
    logger.info("Id=%s, isActive=%s", *results[0])


with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2, tzinfo=datetime.UTC),
    schedule="@once",
    catchup=False,
) as dag:
    init = EmptyOperator(task_id="init")
    read = SQLExecuteQueryOperator(
        task_id="read",
        conn_id="source",
        sql="SELECT id, активность FROM source_data.сотрудники_дар WHERE id = %(_id)s",
        parameters={"_id": 60},
        return_last=True,
    )
    handle = PythonOperator(
        task_id="handle",
        python_callable=handle_data,
    )
    edge = EmptyOperator(task_id="edge")
    init >> read >> handle >> edge
