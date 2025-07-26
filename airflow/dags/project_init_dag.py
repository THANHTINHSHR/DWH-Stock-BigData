from airflow import DAG  # type: ignore
from tasks.project_init_task import ProjectInitTask
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
}

secret_keys = [
    "SPARK_LOCAL_DIR", "SPARK_MODE", "WSS_ENDPOINT", "URL_TOP", "LIMIT", "STREAM_TYPES",
    "AWS_DEFAULT_REGION", "BUCKET_NAME", "ROOT_DB", "ATHENA_DB", "S3_STAGING_DIR",
    "BOOTSTRAP_SERVERS", "BINANCE_TOPIC", "NUM_PARTITIONS",
    "GRAFANA_URL", "GRAFANA_DB_URL", "INFLUXDB_URL", "INFLUXDB_ORG", "INFLUXDB_BUCKET",
    "SUPERSET_URL",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
    "GRAFANA_KEY", "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD",
    "INFLUXDB_TOKEN",
    "SUPERSET_USERNAME", "SUPERSET_PASSWORD", "SUPERSET_SECRET_KEY"
]


secrets = [
    Secret("env", key, secret="project-secret", key=key)
    for key in secret_keys
]

with DAG(
    dag_id="Project_init_dag",
    schedule="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    image = "dwh-stock-bigdata:3.0"
    project_init_task = ProjectInitTask(image, secrets=secrets).build()
    task = project_init_task

globals()["Project_init_dag"] = dag
