from airflow import DAG  # type: ignore
from tasks.ticker_pipline_task import TickerPipelineTask
from datetime import datetime, timedelta
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor  # type: ignore

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),

}
with DAG(
    dag_id="Ticker_dag",
    schedule=None,
    default_args=default_args,
    catchup=False,
) as dag:
    wait_for_init = ExternalTaskSensor(
        task_id="wait_for_project_init",
        external_dag_id="Project_init_dag",
        external_task_id=None,
        execution_date_fn=lambda _: None,
        check_existence=False,
        mode="poke",
        timeout=600,
        poke_interval=30,
    )
    image = "dwh-stock-bigdata:3.0"
    ticker_pipeline = TickerPipelineTask(image).build()
    wait_for_init >> ticker_pipeline
globals()["Ticker_dag"] = dag
