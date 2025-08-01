from airflow.operators.docker_operator import DockerOperator  # type: ignore
from common.pvc_mount import PVCMount


class InformerPredictTask:
    def __init__(self, image):
        self.task_id = "Informer_Predict_Task"
        self.image = image
        self.command = "python3 core/streaming/informerAI/predict/ai_ticker_predictor.py"
        self.docker_url = "unix://var/run/docker.sock"
        self.network_mode = "bridge"
        self.mountPath = PVCMount.get_informer_volume_mount()

    def build(self):
        return DockerOperator(
            task_id=self.task_id,
            image=self.image,
            api_version="auto",
            auto_remove=True,
            command=self.command,
            docker_url=self.docker_url,
            network_mode=self.network_mode,
            mounts=self.mountPath  # type: ignore

        )
