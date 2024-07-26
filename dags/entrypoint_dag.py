"""
-  Точка входа для запуска всех процессов
"""

from datetime import UTC, datetime

from airflow.models import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="entrypoint",
    start_date=datetime(2024, 7, 7, tzinfo=UTC),
    schedule="@daily",
    tags=["korus5"],
    catchup=False,
) as dag:
    ods = TriggerDagRunOperator(
        task_id="ods",
        trigger_dag_id="ods_layer_transfer",
        wait_for_completion=True,
        poke_interval=10,
    )
    dds = TriggerDagRunOperator(
        task_id="dds",
        trigger_dag_id="dds_layer_transfer",
        wait_for_completion=True,
        poke_interval=10,
    )
    dm_empl = TriggerDagRunOperator(
        task_id="dm_empl",
        trigger_dag_id="dm_empl_layer_transfer",
        wait_for_completion=True,
        poke_interval=10,
    )
    dm_dep = TriggerDagRunOperator(
        task_id="dm_dep",
        trigger_dag_id="dm_dep_layer_transfer",
        wait_for_completion=True,
        poke_interval=10,
    )

    ods >> dds >> [dm_empl, dm_dep]
