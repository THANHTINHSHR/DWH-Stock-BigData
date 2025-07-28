from airflow import DAG  # type: ignore
from tasks.trade_pipline_task import TradePipelineTask
from datetime import datetime
from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

# IMPORT FOR AIRFLOW 3.0.2
from airflow.models.dagrun import DagRun  # type: ignore
from airflow.utils.state import DagRunState  # type: ignore
from airflow.utils.session import NEW_SESSION  # type: ignore
from airflow.exceptions import AirflowFailException  # type: ignore


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


def check_project_init_dag_success(**context):
    with NEW_SESSION() as session:
        dag_runs = session.query(DagRun).filter(
            DagRun.dag_id == 'Project_init_dag').all()
        success_runs = [
            dr for dr in dag_runs if dr.state == DagRunState.SUCCESS]
        if not success_runs:
            raise AirflowFailException("❌ Project_init_dag Un_Success.")
        print(f"✅ Project_init_dag run success {len(success_runs)} times.")


with DAG(
    dag_id="Trade_dag",
    schedule=None,
    default_args=default_args,
    catchup=False,
) as dag:
    print(">>>", check_project_init_dag_success)
    print(">>>", type(check_project_init_dag_success))

    check_task = PythonOperator(
        task_id='check_project_init_success',
        python_callable=check_project_init_dag_success,
    )

    image = "dwh-stock-bigdata:3.0"
    trade_pipeline = TradePipelineTask(image, secrets=secrets).build()

    check_task >> trade_pipeline

globals()["Trade_dag"] = dag
