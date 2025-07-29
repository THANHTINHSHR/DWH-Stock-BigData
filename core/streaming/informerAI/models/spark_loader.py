# autopep8: off
import findspark  # type: ignore
findspark.init()
from pyspark.sql.functions import col, to_date  # type: ignore
from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.types import TimestampType, LongType # type: ignore
import datetime
from datetime import datetime, timedelta
import logging
import os, glob
from dotenv import load_dotenv
from pathlib import Path
from py4j.java_gateway import java_import
from enum import Enum

# autopep8: on
load_dotenv()


class SparkMode(str, Enum):
    LOCAL = "local"
    DOCKER = "docker"
    K8S = "k8s"


class SparkLoader:
    def __init__(self):
        self.mode = SparkMode(os.getenv("AI_SPARK_MODE", "local").lower())
        self.project_root_dir = Path(__file__).resolve().parent

        self.AI_APP_NAME = os.getenv("AI_APP_NAME", "InformerAI_App")
        self.AI_SPARK_LOCAL_DIR = os.getenv(
            "AI_SPARK_LOCAL_DIR", "/tmp/ai-spark")
        # AWS S3 configuration
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.AWS_REGION = os.getenv(
            "AWS_DEFAULT_REGION", "us-east-1"
        )
        # Project path setup
        script_file_path = Path(__file__).resolve()
        self.project_root_dir = script_file_path.parent.parent.parent.parent.parent

        # Define the base directory for output files
        self.output_target_base_dir = self.project_root_dir / \
            "core"/"streaming" / "informerAI" / "files"
        # Spark configuration
        self.spark = self.get_spark(self.AI_APP_NAME)
        self.REPARTITION = int(os.getenv("REPARTITION", 12))

        # Logging configuration
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Project root directory: {self.project_root_dir}")
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

    def getInstance(self):
        return self

    def get_spark(self, app_name: str) -> SparkSession:
        jars, log4j_path = self.get_jars_and_log4j()

        builder = SparkSession.builder.appName(app_name) \
            .config("spark.hadoop.fs.s3a.access.key", self.AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", self.AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.defaultFS", f"s3a://{self.BUCKET_NAME}/") \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{self.AWS_REGION}.amazonaws.com") \
            .config("spark.jars", jars) \
            .config("spark.sql.debug.maxToStringFields", 100) \
            .config("spark.hadoop.fs.s3a.multipart.uploads.enabled", "false")

        # Spark local dir (temp files)
        if self.mode == SparkMode.LOCAL:
            temp_dir = (self.project_root_dir /
                        self.AI_SPARK_LOCAL_DIR).as_posix()
        elif self.mode in [SparkMode.DOCKER, SparkMode.K8S]:
            temp_dir = self.AI_SPARK_LOCAL_DIR

        builder = builder.config("spark.local.dir", temp_dir)

        # log4j configuration
        if log4j_path:
            builder = builder \
                .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile={log4j_path}") \
                .config("spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile={log4j_path}")

        # LOCAL mode
        if self.mode == SparkMode.LOCAL:
            builder = builder.config(
                "spark.driver.host", "localhost").master("local[*]")

        return builder.getOrCreate()

    def get_jars_and_log4j(self):
        if self.mode == SparkMode.LOCAL:
            jars_directory = self.project_root_dir / "jars"
            jar_files_list = list(jars_directory.glob("*.jar"))
            jars = ",".join([str(f) for f in jar_files_list])
            log4j_path = (self.project_root_dir / "log4j.properties").as_uri()
            return jars, log4j_path

        elif self.mode in (SparkMode.DOCKER, SparkMode.K8S):
            jars = ",".join(glob.glob("/opt/spark/jars/*.jar"))
            log4j_path = "file:/opt/spark-dist/conf/log4j.properties"
            return jars, log4j_path
        else:
            # Default fallback to avoid returning None
            return "", None

    def close_spark(self):
        """Close the Spark session."""
        if self.spark:
            self.spark.stop()
            self.logger.info("🚀 Spark session closed.")
        else:
            self.logger.warning("No Spark session to close.")

    def read_s3(self, stream_type: str, days_ago: int = 1, max_directories: int = 10) -> DataFrame:
        """Read files in a specific S3 bucket and stream type within a date range."""
        try:
            # Import Java classes for Hadoop FileSystem
            java_import(self.spark._jvm, "org.apache.hadoop.fs.FileSystem")
            java_import(self.spark._jvm, "org.apache.hadoop.fs.Path")

            # Initialize the Hadoop FileSystem for S3 access
            fs = self.spark._jvm.FileSystem.get(
                self.spark._jsc.hadoopConfiguration())

            # Define the S3 path for the given bucket and stream type
            s3_path = f"s3a://{self.BUCKET_NAME}/{stream_type}/"

            # Calculate the time range for file selection
            start_time = int((datetime.now() - timedelta(days=days_ago)
                              ).timestamp() * 1000)  # Earliest valid date

            # Retrieve the list of files in the specified S3 directory
            status = fs.listStatus(self.spark._jvm.Path(s3_path))
            # Filter files that were created within the required date range

            directories = sorted(
                [f"{s3_path}/" + file.getPath().getName() + "/" for file in status
                 if file.isDirectory() and int(file.getPath().getName()) <= start_time],
                key=lambda x: int(x.strip("/").split("/")[-1]),
                reverse=True
            )

            df = self.spark.read.parquet(
                *directories[:max_directories]).repartition(self.REPARTITION)
            # OPTION - SAVE CSV BIG FILE TO EASY USE (DONT NEED RUN COLLECTED DATA AGAIN)
            df.coalesce(1).write.mode("overwrite").csv(
                f"s3a://{self.BUCKET_NAME}/Big_csv/{stream_type}_{max_directories}dir_data.csv", header=True)

            self.logger.info(
                f"📂 Found {len(directories)} directories created before {days_ago} days ago, get limit {max_directories}:")
            return df

        except Exception as e:
            # Log any errors encountered during execution
            self.logger.error(f"❌ Error counting files in {stream_type}: {e}")
            raise

    def read_csv(self, path):
        """SUPPORT DEBUG AND TEST ONLY"""
        self.logger.info(
            f"✅output_target_base_dir: {self.output_target_base_dir}")
        relative_path = os.path.join(
            self.output_target_base_dir, path)
        return self.spark.read.csv(relative_path, header=True, inferSchema=True).repartition(self.REPARTITION)

    def write_csv(self, df: DataFrame, path):
        """SUPPORT DEBUG AND TEST ONLY"""
        self.logger.info(
            f"✅output_target_base_dir: {self.output_target_base_dir}")

        relative_path = os.path.join(
            self.output_target_base_dir, "csv", path)

        df.coalesce(1).write.mode("overwrite").csv(
            relative_path, header=True).repartition(self.REPARTITION)
        self.logger.info(f"✅ DataFrame has been written to {relative_path}.")

    def write_parquet(self, df: DataFrame, path):
        self.logger.info(
            f"✅output_target_base_dir: {self.output_target_base_dir}")

        relative_path = os.path.join(
            self.output_target_base_dir, "parquet", path)

        # df.write.mode("overwrite").parquet(relative_path)
        df.coalesce(1).write.mode("overwrite").parquet(
            relative_path).repartition(self.REPARTITION)
        self.logger.info(f"✅ DataFrame has been written to {relative_path}.")

    def upload_predict_data(self, type: str, df: DataFrame):
        """Uplaod data predict to s3"""
        # {self.BUCKET_NAME}/
        try:
            self.delete_s3_predict_data(type)  # Delete old data

            relative_path = f"{type}_predict"
            df = df.withColumn("event_time", col(
                "event_time").cast(TimestampType()))
            df = df.withColumn("trade_count", col(
                "trade_count").cast(LongType()))

            df.write.mode("overwrite").parquet(
                f"/{relative_path}")
            self.logger.info(
                f"✅ Successfully uploaded prediction data to s3: {relative_path}.")
        except Exception as e:
            self.logger.error(f"❌ Failed to upload prediction data: {e}")

    def delete_s3_predict_data(self, type: str):
        """Delete data predict from s3"""
        try:
            relative_path = f"{type}_predict"

            hadoop_conf = self.spark._jsc.hadoopConfiguration()
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                hadoop_conf)

            path = self.spark._jvm.org.apache.hadoop.fs.Path(
                f"s3a://{self.BUCKET_NAME}/{relative_path}")
            fs.delete(path, True)

            self.logger.info(
                f"✅ Successfully deleted prediction data from s3: {relative_path}.")
        except Exception as e:
            self.logger.error(f"❌ Failed to delete prediction data: {e}")
