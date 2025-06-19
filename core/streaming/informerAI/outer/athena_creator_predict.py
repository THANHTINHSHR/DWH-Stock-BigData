
from core.streaming.informerAI.outer.athena_ticker_predict import AthenaTickerPredict
import os
import boto3
import time

from dotenv import load_dotenv
load_dotenv()


class AthenaCreatorPredict:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(AthenaCreatorPredict, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.ATHENA_DB = os.getenv("ATHENA_DB")
        self.S3_STAGING_DIR = os.getenv("S3_STAGING_DIR")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES")

        self.athena_client = boto3.client(
            "athena",
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            region_name=self.AWS_DEFAULT_REGION,
        )
        self.athena_ticker_predict = AthenaTickerPredict(
            self.athena_client, self.S3_STAGING_DIR, self.BUCKET_NAME, self.ATHENA_DB)

    def get_client(self):
        return self.athena_client

    def run_query(self, query: str, database: str = None) -> bool:  # type: ignore
        params = {
            "QueryString": query,
            "ResultConfiguration": {
                "OutputLocation": f"{self.S3_STAGING_DIR}query-results/"
            },
        }
        if database:
            params["QueryExecutionContext"] = {"Database": database}

        response = self.get_client().start_query_execution(**params)
        query_execution_id = response["QueryExecutionId"]

        while True:
            result = self.get_client().get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = result["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(1)

        print(f"📌Query status : {status}")
        return status == "SUCCEEDED"

    def create_database(self):
        query = f"CREATE DATABASE IF NOT EXISTS {self.ATHENA_DB}"
        if self.run_query(query):
            print("✅ Database created successfully")
        else:
            print("❌ Failed to create database")

        if self.run_query(query, database=self.ATHENA_DB):  # type: ignore
            print("✅ BookTicker table created successfully")
        else:
            print("❌ Failed to create bookTicker table")

    def run_athena(self):
        self.create_database()
        self.athena_ticker_predict.run()
        # self.athena_bookticker.run()
