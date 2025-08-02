

class SensorCheckProjectInitTask:
    def __init__(self, state="success"):
        self.task_id = f"Check_Project_Init_{state}"
        self.conn_id = 'airflow_db'
        self.state = state
        self.sql = f"SELECT COUNT(1) FROM task_instance WHERE dag_id = 'Project_Init_Dag' AND task_id = 'Project_Init_Task' AND state = '{self.state}'"

    def build(self):
        from airflow.providers.common.sql.sensors.sql import SqlSensor  # type: ignore
        return SqlSensor(
            task_id=self.task_id,
            conn_id=self.conn_id,
            sql=self.sql,
            timeout=120,  # wait for 2 minutes
            poke_interval=30,  # recheck every 30 seconds
            mode='reschedule',  # use reschedule mode to check the condition
        )
