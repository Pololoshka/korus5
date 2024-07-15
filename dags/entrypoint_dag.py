"""
-  Точка входа для запуска всех процессов
"""

from datetime import UTC, datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="entrypoint",
    start_date=datetime(2024, 7, 7, tzinfo=UTC),
    schedule="@once",
    tags=["korus5"],
    catchup=False,
) as dag:
    init = EmptyOperator(task_id="init")
    edge = EmptyOperator(task_id="edge")

    init >> edge
