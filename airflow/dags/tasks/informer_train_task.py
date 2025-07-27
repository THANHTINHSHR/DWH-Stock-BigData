
from airflow import DAG  # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore
from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore


class InformerTrainTask:
    def __init__(self, image, namespace="default", secrets: Secret = None):
        self.task_id = "Informer_Train_Task"
        self.image = image
        self.cmds = ["python3"]
        self.arguments = [
            "core/streaming/informerAI/train/ai_ticker_trainer.py"]
        self.namespace = namespace,
        self.secrets = secrets

    def build(self):
        return KubernetesPodOperator(
            task_id=self.task_id,
            name=self.task_id,
            namespace=self.namespace,
            image=self.image,
            cmds=self.cmds,
            arguments=self.arguments,
            get_logs=True,
            is_delete_operator_pod=True,
            secrets=self.secrets
        )
