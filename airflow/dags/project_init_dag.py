from airflow import DAG  # type: ignore
from airflow.tasks.project_init_task import ProjectInitTask
from airflow.utils.dates import days_ago  # type: ignore
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}
with DAG(
    dag_id="Project_init_dag",
    schedule_interval=None,
    catchup=False,
) as dag:
    image = "dwh-stock-bigdata:3.0"
    project_init_task = ProjectInitTask(image).build()
    project_init_task
