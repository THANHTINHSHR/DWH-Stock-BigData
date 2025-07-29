

class InformerPredictTask:
    def __init__(self, image, namespace="default", secrets=None):
        self.task_id = "Informer_Predict_Task"
        self.image = image
        self.cmds = ["python3"]
        self.arguments = [
            "core/streaming/informerAI/predict/ai_ticker_predictor.py"]
        self.namespace = namespace
        self.secrets = secrets

    def build(self):
        from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore
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
