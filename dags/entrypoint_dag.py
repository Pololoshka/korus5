"""
-  Точка входа для запуска всех процессов
"""

from datetime import UTC, datetime

from airflow.models import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="entrypoint",
    start_date=datetime(2024, 7, 7, tzinfo=UTC),
    schedule="@once",
    tags=["korus5"],
    catchup=False,
) as dag:
    ods = TriggerDagRunOperator(
        task_id="ods",
        trigger_dag_id="ods_layer_transfer",
        wait_for_completion=True,
        poke_interval=30,
    )
    dds = TriggerDagRunOperator(
        task_id="dds",
        trigger_dag_id="dds_layer_transfer",
        wait_for_completion=True,
        poke_interval=30,
    )
    dm = TriggerDagRunOperator(
        task_id="dm",
        trigger_dag_id="dm_layer_transfer",
        wait_for_completion=True,
        poke_interval=30,
    )

    ods >> dds >> dm
