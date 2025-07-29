from airflow import DAG  # type: ignore
from tasks.trade_pipline_task import TradePipelineTask
from tasks.sensor_check_project_init_task import SensorCheckProjectInitTask
from common.secret_loader import load_secrets
from common.dag_config import get_dag_config

with DAG(
    dag_id="Trade_Dag",
    schedule=None,
    default_args=get_dag_config(),
    catchup=False,
) as dag:
    check_task = SensorCheckProjectInitTask(state="success").build()
    secrets = load_secrets()
    image = "dwh-stock-bigdata:3.0"
    trade_pipeline = TradePipelineTask(image, secrets=secrets).build()
    check_task >> trade_pipeline

globals()["Trade_Dag"] = dag
