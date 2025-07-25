from airflow import DAG  # type: ignore
from datetime import datetime
with DAG("test_dag", start_date=datetime(2023, 1, 1), schedule="@once", catchup=False) as dag:
    pass
