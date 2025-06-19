# autopep8: off
import findspark  # type: ignore
findspark.init()
from core.streaming.kafka.topic_creator import TopicCreator
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.types import * # type: ignore
from pyspark.sql.functions import col, isnan # type: ignore
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,LongType,FloatType,DoubleType,BooleanType,Row# type: ignore

from functools import reduce
from abc import ABC, abstractmethod
from pathlib import Path

import os, json, logging, time
from dotenv import load_dotenv
# autopep8:on

load_dotenv()


class PipelineBase(ABC):

    def __init__(self, type):
        self.type = type
        # Initialize environment variables
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.AWS_REGION = os.getenv(
            "AWS_DEFAULT_REGION", "us-east-1"
        )  # Default to us-east-1 if not provided

        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")

        script_file_path = Path(__file__).resolve()
        self.project_root_dir = script_file_path.parent.parent.parent.parent

        self.logger = logging.getLogger(
            self.__class__.__name__)  # Add logger here

    def get_spark_session(self, app_name):
        """Returns the single instance of SparkSession"""
        jars_directory = self.project_root_dir / "jars"
        jar_files_list = list(jars_directory.glob("*.jar"))
        jars = ",".join([str(f) for f in jar_files_list])

        # jar_files = glob.glob("/opt/spark/jars/*.jar")
        # jars = ",".join(jar_files)

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
            # Points to the S3 bucket
            .config("spark.hadoop.fs.defaultFS", f"s3a://{self.BUCKET_NAME}/")
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
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")

            # spark configurations memory and cores
            .config("max.poll.records", "50")
            .config("trigger", "ProcessingTime(10 seconds)")
            .config("spark.sql.shuffle.partitions", "300")
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
        query = (
            df.writeStream.format("console")
            .outputMode("update")
            .option("truncate", False)
            .option("numRows", 10)
            .option("checkpointLocation", f"{self.BUCKET_NAME}/checkpoints/{self.type}")
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
            self.logger.info(f"✅ Success send data to S3: {type}")
        except Exception as e:
            self.logger.error(f"❌Fail to write to S3: {e}")

    def run_streams(self):
        self.logger.info(
            f"✅ Starting {self.type} streams for {len(TopicCreator.TOPCOIN)} symbols."
        )
        queries = []
        for symbol in TopicCreator.TOPCOIN:
            self.logger.info(f"✅ Setting up stream for {self.type}: {symbol}")
            raw_data = self.read_stream(symbol)
            # Consider logging raw schema if needed
            transformed_data = self.transform_stream(raw_data)  # type: ignore
            df_to_influx = transformed_data["df"].select("*")  # type: ignore
            df_to_s3 = transformed_data["df"].select("*")  # type: ignore
            query_influx = df_to_influx.writeStream \
                .foreachBatch(lambda df, epoch_id: self.load_to_InfluxDB(df)) \
                .option("checkpointLocation", f"{self.BUCKET_NAME}/checkpoints/influx/{symbol}") \
                .start()

            query_s3 = df_to_s3.writeStream \
                .foreachBatch(lambda df, epoch_id: self.load_to_S3(df, self.type)) \
                .option("checkpointLocation", f"{self.BUCKET_NAME}/checkpoints/s3/{symbol}") \
                .start()

            queries.append(query_s3)
            queries.append(query_influx)
        for query in queries:
            query.awaitTermination()
