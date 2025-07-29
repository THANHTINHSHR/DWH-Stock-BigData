from airflow.kubernetes.volume import Volume  # type: ignore
from airflow.kubernetes.volume_mount import VolumeMount  # type: ignore


class PVCMount:
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_informer_volume() -> Volume:
        volume = Volume(name="informer-storage", configs={
            "persistentVolumeClaim": {"claimName": "informer-storage-pvc"}
        })
        return volume

    @staticmethod
    def get_informer_volume_mount() -> VolumeMount:
        # Mount path should be suitable for the application
        volume_mount = VolumeMount(
            name="informer-storage",
            mount_path="core/streaming/informerAI/files",
            read_only=False
        )
        return volume_mount
