from influxdb_client import InfluxDBClient
from dotenv import load_dotenv
import os, logging

load_dotenv()


class InfluxDBConnector:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(InfluxDBConnector, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized") or not self._initialized:
            # InfluxDB config
            self.INFLUXDB_URL = os.getenv("INFLUXDB_URL")
            self.INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
            self.INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
            self.INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
            # env
            self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
            self.client = InfluxDBClient(
                url=self.INFLUXDB_URL, token=self.INFLUXDB_TOKEN, org=self.INFLUXDB_ORG
            )
            self.write_api = self.client.write_api()
            self._initialized = True
            self.logger = logging.getLogger(self.__class__.__name__)

    @classmethod
    def get_instance(cls):
        """Method to get the singleton instance."""
        if cls._instance is None:
            cls._instance = InfluxDBConnector()
        return cls._instance

    def create_buckets(self):
        buckets_api = self.client.buckets_api()
        existing_buckets = [b.name for b in buckets_api.find_buckets().buckets]
        for stream_type in self.STREAM_TYPES:
            if stream_type not in existing_buckets:
                buckets_api.create_bucket(
                    bucket_name=stream_type, org=self.INFLUXDB_ORG
                )
                self.logger.info(f"✅ Created bucket: {stream_type}")
            else:
                self.logger.warning(f"⚠️ Bucket already exists: {stream_type}")

    def send_line_data(self, bucket_name, line_data):
        pass
        try:
            self.write_api.write(bucket=bucket_name, record=line_data)
        except Exception as e:
            self.logger.error(f"❌ Failed to send data to bucket {bucket_name}: {e}")
