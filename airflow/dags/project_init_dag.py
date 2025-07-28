from airflow import DAG  # type: ignore
from tasks.project_init_task import ProjectInitTask
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 27, 3, 46, 37),
}

secret_keys = [
    # aiSpark
    "AI_SPARK_MODE", "AI_SPARK_LOCAL_DIR", "AI_SPARK_APP_NAME",

    # core
    "SPARK_LOCAL_DIR", "SPARK_MODE",
    "WSS_ENDPOINT", "URL_TOP", "LIMIT", "STREAM_TYPES",
    "AWS_DEFAULT_REGION", "BUCKET_NAME", "ROOT_DB", "ATHENA_DB", "S3_STAGING_DIR",
    "BOOTSTRAP_SERVERS", "BINANCE_TOPIC", "NUM_PARTITIONS",
    "GRAFANA_URL", "GRAFANA_DB_URL",
    "INFLUXDB_URL", "INFLUXDB_ORG", "INFLUXDB_BUCKET",
    "SUPERSET_URL",

    # aws
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",

    # grafana
    "GRAFANA_KEY", "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD",

    # influxdb
    "INFLUXDB_TOKEN",

    # superset
    "SUPERSET_USERNAME", "SUPERSET_PASSWORD", "SUPERSET_SECRET_KEY",

    # informer
    "AI_APP_NAME", "REPARTITION", "TRAIN_RATIO", "VAL_RATIO",
    "BATCH_SIZE", "N_DAYS_AGO", "MAX_DIRECTORIES",
    "SEQUENCE_LENGTH", "PREDICTION_LENGTH", "NUM_EPOCHS"
]


secrets = [
    Secret("env", key, secret="project-secret", key=key)
    for key in secret_keys
]


def push_success_flag(**kwargs):
    kwargs['ti'].xcom_push(key='init_success', value=True)


with DAG(
    dag_id="Project_init_dag",
    schedule="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    image = "dwh-stock-bigdata:3.0"
    project_init_task = ProjectInitTask(image, secrets=secrets).build()
    project_init_task.on_success_callback = push_success_flag

    project_init_task
globals()["Project_init_dag"] = dag
