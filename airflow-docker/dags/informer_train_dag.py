from airflow import DAG  # type: ignore
from tasks.informer_train_task import InformerTrainTask
from tasks.sensor_check_project_init_task import SensorCheckProjectInitTask
from common.secret_loader import load_secrets
from common.dag_config import get_dag_config
with DAG(
    dag_id="Train_Dag",
    schedule=None,
    default_args=get_dag_config(),
    catchup=False,
) as dag:
    secrets = load_secrets()
    check_task = SensorCheckProjectInitTask(state="success").build()
    image = "informer-ai:3.0"
    train_task = InformerTrainTask(image, secrets=secrets).build()
    check_task >> train_task
globals()["Train_Dag"] = dag
