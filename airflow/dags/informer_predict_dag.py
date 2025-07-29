from airflow import DAG  # type: ignore
from tasks.informer_predict_task import InformerPredictTask
from tasks.sensor_check_project_init_task import SensorCheckProjectInitTask
from common.secret_loader import load_secrets
from common.dag_config import get_dag_config
with DAG(
    dag_id="Predict_Dag",
    schedule=None,
    default_args=get_dag_config(),
    catchup=False,
) as dag:
    secrets = load_secrets()
    check_task = SensorCheckProjectInitTask(state="success").build()
    image = "informer-ai:3.0"
    predict_task = InformerPredictTask(image, secrets=secrets).build()
    check_task >> predict_task
globals()["Predict_Dag"] = dag
