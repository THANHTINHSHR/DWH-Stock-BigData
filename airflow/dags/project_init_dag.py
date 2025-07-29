from airflow import DAG  # type: ignore
from tasks.project_init_task import ProjectInitTask
from datetime import datetime
from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore
from common.secret_loader import load_secrets
from common.dag_config import get_dag_config

with DAG(
    dag_id="Project_init_dag",
    schedule="@once",
    default_args=get_dag_config(),
    catchup=False,
) as dag:
    secrets = load_secrets()
    image = "dwh-stock-bigdata:3.0"
    project_init_task = ProjectInitTask(image, secrets=secrets).build()
    project_init_task
globals()["Project_init_dag"] = dag
