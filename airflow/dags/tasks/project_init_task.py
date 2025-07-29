
from airflow import DAG  # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore
from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore


class ProjectInitTask:

    def __init__(self, image, namespace="default", secrets: Secret = None):
        self.task_id = "Project_Init_Task"
        self.image = image
        self.cmds = ["python3"]
        self.arguments = ["core/run/run_init.py"]
        self.namespace = namespace
        self.secrets = secrets
        self.request_memory = "1Gi"

    def build(self):
        from airflow.utils.operator_resources import Resources  # type: ignore
        return KubernetesPodOperator(
            task_id=self.task_id,
            name=self.task_id,
            namespace=self.namespace,
            image=self.image,
            cmds=self.cmds,
            arguments=self.arguments,
            get_logs=True,
            is_delete_operator_pod=True,
            secrets=self.secrets,
            resources=Resources(
                ram=self.request_memory
            ),
        )
