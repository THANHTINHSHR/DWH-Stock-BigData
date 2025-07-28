from airflow import DAG  # type: ignore
from tasks.project_init_task import ProjectInitTask
from datetime import datetime
from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore
from common.secret_loader import load_secrets
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 27, 3, 46, 37),
}

with DAG(
    dag_id="Project_init_dag",
    schedule="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    secrets = load_secrets()
    image = "dwh-stock-bigdata:3.0"
    project_init_task = ProjectInitTask(image, secrets=secrets).build()
    project_init_task
globals()["Project_init_dag"] = dag
