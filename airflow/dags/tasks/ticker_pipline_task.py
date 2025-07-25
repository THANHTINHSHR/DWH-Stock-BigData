from airflow import DAG  # type: ignore
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # type: ignore


class TickerPipelineTask:
    def __init__(self, image, namespace="default"):
        self.task_id = "Ticker_Pipe_line_Task"
        self.image = image
        self.cmds = ["python3"]
        self.arguments = ["core/run/run_ticker_pipline.py"]
        self.namespace = namespace

    def build(self):
        return KubernetesPodOperator(
            task_id=self.task_id,
            name=self.task_id,
            namespace=self.namespace,
            image=self.image,
            cmds=self.cmds,
            arguments=self.arguments,
            get_logs=True,
            is_delete_operator_pod=True
        )
