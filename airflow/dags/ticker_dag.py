from airflow import DAG  # type: ignore
from tasks.ticker_pipline_task import TickerPipelineTask
from datetime import datetime, timedelta
from tasks.sensor_check_project_init_task import SensorCheckProjectInitTask
from common.secret_loader import load_secrets
from common.dag_config import get_dag_config

with DAG(
    dag_id="Ticker_Dag",
    schedule=None,
    default_args=get_dag_config(),
    catchup=False,
) as dag:
    check_task = SensorCheckProjectInitTask(state="Success").build()
    secrets = load_secrets()
    image = "dwh-stock-bigdata:3.0"
    ticker_pipeline = TickerPipelineTask(image, secrets=secrets).build()
    check_task >> ticker_pipeline
globals()["Ticker_Dag"] = dag
