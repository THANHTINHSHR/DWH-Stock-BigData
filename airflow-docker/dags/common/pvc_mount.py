from docker.types import Mount  # type: ignore


class PVCMount:
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_informer_volume_mount():
        mounts = [
            Mount(source="core/streaming/informerAI/files",
                  target="core/streaming/informerAI/files", type="bind"),
        ]
        return mounts
