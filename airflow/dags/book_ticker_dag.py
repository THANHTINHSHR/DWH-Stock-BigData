from airflow import DAG  # type: ignore
from datetime import datetime
from airflow.providers.common.sql.sensors.sql import SqlSensor  # type: ignore
from tasks.book_ticker_pipeline_task import BookTickerPipelineTask  # type: ignore


def load_secrets():
    from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore

    secret_keys = [
        "AI_SPARK_MODE", "AI_SPARK_LOCAL_DIR", "AI_SPARK_APP_NAME",
        "SPARK_LOCAL_DIR", "SPARK_MODE", "WSS_ENDPOINT", "URL_TOP", "LIMIT",
        "STREAM_TYPES", "AWS_DEFAULT_REGION", "BUCKET_NAME", "ROOT_DB", "ATHENA_DB",
        "S3_STAGING_DIR", "BOOTSTRAP_SERVERS", "BINANCE_TOPIC", "NUM_PARTITIONS",
        "GRAFANA_URL", "GRAFANA_DB_URL", "INFLUXDB_URL", "INFLUXDB_ORG", "INFLUXDB_BUCKET",
        "SUPERSET_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "GRAFANA_KEY",
        "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD", "INFLUXDB_TOKEN",
        "SUPERSET_USERNAME", "SUPERSET_PASSWORD", "SUPERSET_SECRET_KEY",
        "AI_APP_NAME", "REPARTITION", "TRAIN_RATIO", "VAL_RATIO", "BATCH_SIZE",
        "N_DAYS_AGO", "MAX_DIRECTORIES", "SEQUENCE_LENGTH", "PREDICTION_LENGTH", "NUM_EPOCHS"
    ]

    return [Secret("env", key, secret="project-secret", key=key) for key in secret_keys]


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 27, 3, 46, 37),
}

with DAG(
    dag_id="Book_Ticker_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    check_task = SqlSensor(
        task_id="check_project_init_success",
        conn_id="airflow_db",
        sql="""SELECT COUNT(1)
               FROM dag_run
               WHERE dag_id = 'Project_init_dag'
               AND state = 'success'"""
    )

    secrets = load_secrets()
    image = "dwh-stock-bigdata:3.0"

    book_ticker_task = BookTickerPipelineTask(
        image=image,
        secrets=secrets
    ).build()

    check_task >> book_ticker_task


globals()["Book_Ticker_dag"] = dag
