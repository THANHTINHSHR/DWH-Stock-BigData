from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, isnan
from functools import reduce
from abc import ABC, abstractmethod
from pathlib import Path

import os, json, logging, time
from dotenv import load_dotenv
import glob

load_dotenv()


class PipelineBase(ABC):

    def __init__(self):
        # Initialize environment variables
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.AWS_REGION = os.getenv(
            "AWS_DEFAULT_REGION", "us-east-1"
        )  # Default to us-east-1 if not provided

        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.logger = logging.getLogger(self.__class__.__name__)  # Add logger here

    def get_spark_session(self, app_name):
        """Returns the single instance of SparkSession"""
        jar_files = glob.glob("/opt/spark/jars/*.jar")
        jars = ",".join(jar_files)

        spark = (
            SparkSession.builder.appName(f"{app_name}")
            .config("spark.hadoop.fs.s3a.access.key", self.AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", self.AWS_SECRET_ACCESS_KEY)
            .config(
                "spark.hadoop.fs.s3a.endpoint", f"s3.{self.AWS_REGION}.amazonaws.com"
            )
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("checkpointLocation", f"{self.BUCKET_NAME}/checkpoints")
            .config("spark.jars", jars)
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")
            .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
            .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
            .config("spark.hadoop.fs.s3a.retry.limit", "3")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config("spark.hadoop.hadoop.metrics.logger", "NONE")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.sql.caseSensitive", "true")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )
        return spark

    def avro_type_to_spark_type(self, avro_type):
        """Map basic Avro types to PySpark types"""
        if isinstance(avro_type, list):
            avro_type = [t for t in avro_type if t != "null"][0]
        if isinstance(avro_type, dict):
            return self.avro_to_struct(avro_type)
        mapping = {
            "string": StringType(),
            "int": IntegerType(),
            "long": LongType(),
            "float": FloatType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
        }
        return mapping.get(avro_type, StringType())

    def avro_to_struct(self, avro_schema):
        """Convert Avro schema dict to PySpark StructType"""
        fields = []
        for field in avro_schema.get("fields", []):
            name = field["name"]
            dtype = self.avro_type_to_spark_type(field["type"])
            fields.append(StructField(name, dtype, True))
        return StructType(fields)

    def get_schema(self, schema_name):
        """Load all .avro files into self.streams_schema"""

        base_dir = Path(__file__).resolve().parent
        parent_dir = base_dir.parent
        schema_dir = parent_dir / "kafka" / "schema_avsc"

        self.logger.info(f"📁 Schema dir: {schema_dir}")
        self.logger.info(f"📁 Loading schema: {schema_name}")
        avro_file = schema_dir / f"{schema_name}.avsc"
        with open(avro_file, "r") as f:
            schema = json.load(f)
            struct = self.avro_to_struct(schema)
        return struct

    def show_df_stream(self, data: dict):
        # df = df.selectExpr("cast(value as string)", "timestamp")
        df = data["df"]
        symbol = data["symbol"]
        stream_type = data["stream_type"]
        query = (
            df.writeStream.format("console")
            .outputMode("update")
            .option("truncate", False)
            .option("numRows", 10)
            .start()
        )
        query.awaitTermination()

    def get_filter_condition(self, stream_type):
        conditions = []
        schema = self.get_schema(stream_type)
        for field in schema.fields:
            name = field.name
            dtype = field.dataType

            # Base: Not null
            cond = col(name).isNotNull()

            # String: not empty
            if isinstance(dtype, StringType):
                cond = cond & (col(name) != "")

            # Numeric: not NaN
            elif isinstance(dtype, (FloatType, DoubleType)):
                cond = cond & (~isnan(col(name)))

            # Boolean: is boolean
            elif isinstance(dtype, BooleanType):
                cond = cond

            conditions.append(cond)

        return reduce(lambda a, b: a & b, conditions)

    @abstractmethod
    def read_stream(self, symbol):
        pass

    @abstractmethod
    def transform_stream(self, data: dict):
        pass

    @abstractmethod
    def to_line_protocol(self, row: Row):
        pass

    @abstractmethod
    def load_to_InfluxDB(self, df):
        pass

    def load_to_S3(self, df, type):
        try:
            df.write.mode("append").format("parquet").option(
                "compression", "snappy"
            ).save(f"s3a://{self.BUCKET_NAME}/{type}/{int(time.time())}/")
            self.logger.info(f"✅ Success send data to S3: {self.type}")
        except Exception as e:
            self.logger.error(f"❌Fail to write to S3: {e}")

    @abstractmethod
    def run_streams(self):
        pass
