from airflow import DAG  # type: ignore
from datetime import datetime
from tasks.book_ticker_pipline_task import BookTickerPipelineTask
from airflow.dags.tasks.sensor_check_project_init_task import SensorCheckProjectInitTask
from common.secret_loader import load_secrets
from common.dag_config import get_dag_config

with DAG(
    dag_id="Book_Ticker_dag",
    default_args=get_dag_config(),
    schedule=None,
    catchup=False,
) as dag:
    check_task = SensorCheckProjectInitTask(state="Success").build()

    secrets = load_secrets()
    image = "dwh-stock-bigdata:3.0"

    book_ticker_task = BookTickerPipelineTask(
        image=image,
        secrets=secrets
    ).build()
    check_task >> book_ticker_task


globals()["Book_Ticker_dag"] = dag
