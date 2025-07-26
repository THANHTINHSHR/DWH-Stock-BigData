from airflow import DAG  # type: ignore
from tasks.ticker_pipline_task import TickerPipelineTask
from datetime import datetime, timedelta
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor  # type: ignore
from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore
default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),

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
with DAG(
    dag_id="Ticker_dag",
    schedule=None,
    default_args=default_args,
    catchup=False,
) as dag:
    wait_for_init = ExternalTaskSensor(
        task_id="wait_for_project_init",
        external_dag_id="Project_init_dag",
        external_task_id=None,
        execution_date_fn=lambda _: None,
        check_existence=False,
        mode="poke",
        timeout=600,
        poke_interval=30,
    )
    image = "dwh-stock-bigdata:3.0"
    ticker_pipeline = TickerPipelineTask(image, secrets=secrets).build()
    wait_for_init >> ticker_pipeline
globals()["Ticker_dag"] = dag
