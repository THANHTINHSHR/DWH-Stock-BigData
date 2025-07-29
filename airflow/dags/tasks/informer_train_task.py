
class InformerTrainTask:
    def __init__(self, image, namespace="default", secrets=None):
        self.task_id = "Informer_Train_Task"
        self.image = image
        self.cmds = ["python3"]
        self.arguments = [
            "core/streaming/informerAI/train/ai_ticker_trainer.py"]
        self.namespace = namespace
        self.secrets = secrets
        self.request_memory = 1024*1024*16

    def build(self):
        from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore
        from airflow.utils.operator_resources import Resources  # type: ignore
        from airflow.providers.cncf.kubernetes.volume import Volume  # type: ignore
        from airflow.providers.cncf.kubernetes.volume_mount import VolumeMount  # type: ignore
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
            volumes=[Volume.get_informer_volume()],
            volume_mounts=[VolumeMount.get_informer_volume_mount()],

        )
