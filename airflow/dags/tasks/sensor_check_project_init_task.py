from airflow.providers.common.sql.sensors.sql import SqlSensor  # type: ignore


class SensorCheckProjectInitTask:
    def __init__(self, state="Success"):
        self.task_id = f"Check_Project_Init_{state}"
        self.conn_id = 'airflow_db'
        self.state = state
        self.sql = f"SELECT COUNT(1) FROM dag_run WHERE dag_id = 'Project_init_dag' AND state = '{self.state}'"

    def build(self):
        return SqlSensor(
            task_id=self.task_id,
            conn_id=self.conn_id,
            sql=self.sql
        )
