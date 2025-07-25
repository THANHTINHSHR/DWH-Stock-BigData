from airflow import DAG  # type: ignore
from tasks.project_init_task import ProjectInitTask
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),

}
with DAG(
    dag_id="Project_init_dag",
    schedule="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    image = "dwh-stock-bigdata:3.0"
    project_init_task = ProjectInitTask(image).build()
    task = project_init_task
globals()["Project_init_dag"] = dag
