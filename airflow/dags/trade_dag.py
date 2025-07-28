from airflow import DAG  # type: ignore
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator  # type: ignore
from airflow.models import XCom  # type: ignore
from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 27, 3, 46, 37),
}


def build_secrets():
    secret_keys = [
        "AI_SPARK_MODE", "AI_SPARK_LOCAL_DIR", "AI_SPARK_APP_NAME",
        "SPARK_LOCAL_DIR", "SPARK_MODE",
        "WSS_ENDPOINT", "URL_TOP", "LIMIT", "STREAM_TYPES",
        "AWS_DEFAULT_REGION", "BUCKET_NAME", "ROOT_DB", "ATHENA_DB", "S3_STAGING_DIR",
        "BOOTSTRAP_SERVERS", "BINANCE_TOPIC", "NUM_PARTITIONS",
        "GRAFANA_URL", "GRAFANA_DB_URL",
        "INFLUXDB_URL", "INFLUXDB_ORG", "INFLUXDB_BUCKET",
        "SUPERSET_URL",
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
        "GRAFANA_KEY", "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD",
        "INFLUXDB_TOKEN",
        "SUPERSET_USERNAME", "SUPERSET_PASSWORD", "SUPERSET_SECRET_KEY",
        "AI_APP_NAME", "REPARTITION", "TRAIN_RATIO", "VAL_RATIO",
        "BATCH_SIZE", "N_DAYS_AGO", "MAX_DIRECTORIES",
        "SEQUENCE_LENGTH", "PREDICTION_LENGTH", "NUM_EPOCHS"
    ]

    return [Secret("env", key, secret="project-secret", key=key) for key in secret_keys]

# 👇 Hàm kiểm tra XCom


def check_init_success(**kwargs):
    execution_date = kwargs['execution_date'] - timedelta(minutes=5)

    success_flag = XCom.get_value(
        key='init_success',
        task_id='Project_init_Task',
        dag_id='Project_init_dag',
        execution_date=execution_date
    )

    if success_flag is True:
        print("✅ Project_init_dag success.")
    else:
        raise ValueError("❌ Task Project_init_dag Failed.")


with DAG(
    dag_id="Trade_dag",
    schedule=None,
    default_args=default_args,
    catchup=False,
) as dag:
    from tasks.trade_pipline_task import TradePipelineTask

    secrets = build_secrets()
    image = "dwh-stock-bigdata:3.0"

    check_task = PythonOperator(
        task_id='check_project_init_success',
        python_callable=check_init_success,
    )

    trade_pipeline = TradePipelineTask(image, secrets=secrets).build()

    check_task >> trade_pipeline

globals()["Trade_dag"] = dag
