
from airflow.operators.docker_operator import DockerOperator  # type: ignore


class BookTickerPipelineTask:
    def __init__(self, image,):
        self.task_id = "Book_Ticker_Pipe_line_Task"
        self.image = image
        self.command = "python3 core/run/run_book_ticker_pipline.py"
        self.docker_url = "unix://var/run/docker.sock"
        self.network_mode = "bridge"

    def build(self):
        return DockerOperator(
            task_id=self.task_id,
            image=self.image,
            api_version="auto",
            auto_remove=True,
            command=self.command,
            docker_url=self.docker_url,
            network_mode=self.network_mode,
        )
