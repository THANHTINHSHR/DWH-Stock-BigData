from kubernetes.client import models as k8s  # type: ignore


class PVCMount:
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_informer_volume():
        volume = k8s.V1Volume(
            name='my-volume',
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name='my-pvc')
        )
        return volume

    @staticmethod
    def get_informer_volume_mount():
        # Mount path should be suitable for the application
        volume_mount = k8s.V1VolumeMount(
            name="informer-storage",
            mount_path="core/streaming/informerAI/files",
            read_only=False
        )
        return volume_mount
