from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from dotenv import load_dotenv

load_dotenv()


class SparkSessionSingleton:
    _instance = None

    def __init__(self):
        # Initialize environment variables
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.AWS_REGION = os.getenv(
            "AWS_DEFAULT_REGION", "us-east-1"
        )  # Default to us-east-1 if not provided

    @staticmethod
    def get_spark_session():
        """Returns the single instance of SparkSession"""

        # If you prefer using packages instead of local jars:
        # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
        if SparkSessionSingleton._instance is None:
            log4j_path = os.path.abspath("log4j.properties")

            # Create conf
            conf = SparkConf()
            # conf.set(
            #    "spark.driver.extraJavaOptions",
            # conf.set(
            #   "spark.executor.extraJavaOptions",
            #    f"-Dlog4j.configuration=file:{log4j_path}",
            # )
            # Create an instance of the class to access environment variables
            instance = SparkSessionSingleton()
            # Create SparkSession with configurations for S3 access
            SparkSessionSingleton._instance = (
                SparkSession.builder.appName(f"{instance.BUCKET_NAME}-spark")
                .config("spark.hadoop.fs.s3a.access.key", instance.AWS_ACCESS_KEY_ID)
                .config(
                    "spark.hadoop.fs.s3a.secret.key", instance.AWS_SECRET_ACCESS_KEY
                )
                .config(
                    "spark.hadoop.fs.s3a.endpoint",
                    f"s3.{instance.AWS_REGION}.amazonaws.com",
                )
                .config(
                    "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
                # Replace local jars with Maven package
                .config(
                    "spark.jars.packages",
                    ",".join(
                        [
                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
                            "org.apache.kafka:kafka-clients:3.5.1",
                            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.5",
                        ]
                    ),
                )
                .config("spark.hadoop.fs.s3a.connection.maximum", "100")
                .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
                .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
                .config("spark.hadoop.fs.s3a.retry.limit", "3")
                .config("spark.hadoop.fs.s3a.fast.upload", "true")
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                .config("spark.sql.caseSensitive", "true")
                .getOrCreate()
            )
        return SparkSessionSingleton._instance
