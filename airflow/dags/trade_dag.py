from airflow import DAG  # type: ignore
from airflow.tasks.trade_pipline_task import TradePipelineTask
from airflow.utils.dates import days_ago  # type: ignore
from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}
with DAG(
    dag_id="Trade_dag",
    schedule_interval=None,
    catchup=False,
) as dag:
    wait_for_init = ExternalTaskSensor(
        task_id="wait_for_project_init",
        external_dag_id="Project_init_dag",
        mode="poke",
        timeout=600,
        poke_interval=30,
    )
    image = "dwh-stock-bigdata:3.0"
    trade_pipeline = TradePipelineTask(image).build()
    wait_for_init >> trade_pipeline
