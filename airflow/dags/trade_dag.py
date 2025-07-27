from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor  # type: ignore
from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore
from airflow.models import DagRun  # type: ignore
from airflow.utils.state import State  # type: ignore
from datetime import datetime  # type: ignore
from tasks.trade_pipline_task import TradePipelineTask

# 🔧 DAG setup
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


def get_latest_success_execution_date(**kwargs):
    session = kwargs['session']
    dag_runs = session.query(DagRun).filter(
        DagRun.dag_id == 'Project_init_dag',
        DagRun.state == State.SUCCESS
    ).order_by(DagRun.execution_date.desc()).limit(1).all()

    if dag_runs:
        latest_date = dag_runs[0].execution_date
        kwargs['ti'].xcom_push(key='latest_execution_date', value=latest_date)
    else:
        raise ValueError("Không tìm thấy DAG thành công nào")


with DAG(
    dag_id="Trade_dag",
    schedule=None,
    default_args=default_args,
    catchup=False
) as dag:

    get_date_task = PythonOperator(
        task_id='get_latest_execution_date',
        python_callable=get_latest_success_execution_date,
        provide_context=True
    )

    wait_for_init_task = ExternalTaskSensor(
        task_id='Wait_For_Init_Task',
        external_dag_id='Project_init_dag',
        external_task_id='Project_init_Task',
        execution_date="{{ task_instance.xcom_pull(task_ids='get_latest_execution_date', key='latest_execution_date') }}",
        mode='poke',
        timeout=600,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed', 'skipped']
    )

    image = "dwh-stock-bigdata:3.0"
    trade_pipeline = TradePipelineTask(image, secrets=secrets).build()

    get_date_task >> wait_for_init_task >> trade_pipeline

globals()["Trade_dag"] = dag
