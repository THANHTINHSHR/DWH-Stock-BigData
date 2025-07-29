from airflow.providers.common.sql.sensors.sql import SqlSensor  # type: ignore


class SensorCheckProjectInitTask:
    def __init__(self, state="Success"):
        self.task_id = f"Project_Init_{state}"
        self.conn_id = 'airflow_db'
        self.state = state
        self.sql = f"SELECT COUNT(1) FROM dag_run WHERE dag_id = 'Project_init_dag' AND state = '{self.state}'"

    def build(self):
        return SqlSensor(
            task_id=self.task_id,
            conn_id=self.conn_id,
            sql=self.sql,
            timeout=180,  # wait for 3 minutes
            poke_interval=60,  # recheck every 60 seconds
            mode='reschedule',  # use reschedule mode to check the condition
            soft_fail=True  # allow the task to fail without failing the DAG
        )
