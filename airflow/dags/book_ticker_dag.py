from airflow import DAG  # type: ignore
from datetime import datetime
from airflow.providers.common.sql.sensors.sql import SqlSensor  # type: ignore
from tasks.book_ticker_pipline_task import BookTickerPipelineTask
from common.secret_loader import load_secrets

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 27, 3, 46, 37),
}

with DAG(
    dag_id="Book_Ticker_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    check_task = SqlSensor(
        task_id="check_project_init_success",
        conn_id="airflow_db",
        sql="""SELECT COUNT(1)
               FROM dag_run
               WHERE dag_id = 'Project_init_dag'
               AND state = 'success'"""
    )

    secrets = load_secrets()
    image = "dwh-stock-bigdata:3.0"

    book_ticker_task = BookTickerPipelineTask(
        image=image,
        secrets=secrets
    ).build()

    check_task >> book_ticker_task


globals()["Book_Ticker_dag"] = dag
