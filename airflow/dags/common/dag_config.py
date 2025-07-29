from datetime import datetime


def get_dag_config() -> dict:
    default_args = {
        "owner": "airflow",
        "start_date": datetime(2025, 7, 27, 3, 46, 37),

    }
    return default_args
