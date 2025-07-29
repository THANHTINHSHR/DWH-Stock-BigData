

class TradePipelineTask:
    def __init__(self, image, namespace="default", secrets=None):
        self.task_id = "Trade_Pipe_line_Task"
        self.image = image
        self.cmds = ["python3"]
        self.arguments = ["core/run/run_trade_pipline.py"]
        self.namespace = namespace
        self.secrets = secrets
        self.request_memory = "1Gi"
        self.limit_memory = "4Gi"

    def build(self):
        from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore
        from kubernetes.client import V1ResourceRequirements  # type: ignore
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
            resources=V1ResourceRequirements(
                requests={"memory": self.request_memory},
                limits={"memory": self.limit_memory},
            ),

        )
