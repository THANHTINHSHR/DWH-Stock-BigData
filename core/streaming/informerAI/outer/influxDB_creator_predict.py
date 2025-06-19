# autopep8: off
import findspark  # type: ignore
findspark.init()
from pyspark.sql.types import TimestampType, LongType # type: ignore
from pyspark.sql.functions import col  # type: ignore

from pyspark.sql.types import StructType, StructField, DoubleType, StringType # type: ignore
from influxdb_client import InfluxDBClient, Point, WritePrecision  # type: ignore
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
from dotenv import load_dotenv
import os
import logging
# autopep8: on
load_dotenv()


class InfluxDBConnectorPredict:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(InfluxDBConnectorPredict, cls).__new__(
                cls, *args, **kwargs)
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
            self.delete_api = self.client.delete_api()
            self._initialized = True
            self.logger = logging.getLogger(self.__class__.__name__)

    @classmethod
    def get_instance(cls):
        """Method to get the singleton instance."""
        if cls._instance is None:
            cls._instance = InfluxDBConnectorPredict()
        return cls._instance

    def create_buckets(self):
        buckets_api = self.client.buckets_api()
        existing_buckets = [b.name for b in buckets_api.find_buckets().buckets]
        for b in existing_buckets:
            self.logger.info(f"Existing bucket: {b}")
        for stream_type in self.STREAM_TYPES:
            if f"{stream_type}_predict" not in existing_buckets:
                buckets_api.create_bucket(
                    bucket_name=f"{stream_type}_predict", org=self.INFLUXDB_ORG
                )
                self.logger.info(f"✅ Created bucket: {stream_type}_predict")
            else:
                self.logger.warning(
                    f"⚠️ Bucket already exists: {stream_type}_predict")

    def send_line_data(self, bucket_name, line_data):
        pass
        try:
            self.write_api.write(bucket=bucket_name, record=line_data)
        except Exception as e:
            self.logger.error(
                f"❌ Failed to send data to bucket {bucket_name}: {e}")

    def send_bulk_data(self, type, df):
        try:
            # Clean Old Data
            self.clean_old_predict_data(type)

            rows = df.collect()
            points = []

            for i, row in enumerate(rows):
                point = Point(f"{type}") \
                    .tag("symbol", row.symbol) \
                    .field("last_price", float(row.last_price)) \
                    .field("best_bid_price", float(row.best_bid_price)) \
                    .field("best_ask_price", float(row.best_ask_price)) \
                    .field("trade_count", int(row.trade_count)) \
                    .time(row.event_time, WritePrecision.NS)
                points.append(point)

                self.logger.info(
                    f"📤 Sending row {i + 1}: symbol={row.symbol}, last_price={row.last_price}, time={row.event_time}")

            # Write Data
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.write_api.write(bucket=f"{type}_predict", record=points)
            self.client.close()

            self.logger.info(
                f"✅ Successfully sent bulk data to bucket '{type}_predict'")

        except Exception as e:
            self.logger.error(
                f"❌ Failed to send bulk data to bucket '{type}_predict': {e}")

    def clean_old_predict_data(self, type):
        try:
            start = "1970-01-01T00:00:00Z"
            stop = datetime.utcnow().isoformat() + "Z"
            self.delete_api.delete(start, stop, predicate="",
                                   bucket=f"{type}_predict", org=self.INFLUXDB_ORG)
            self.logger.info(
                f"✅ Successfully cleaned old data from bucket '{type}_predict'")
        except Exception as e:
            self.logger.error(
                f"❌ Failed to clean old data from bucket '{type}_predict': {e}")


if __name__ == "__main__":
    pass
