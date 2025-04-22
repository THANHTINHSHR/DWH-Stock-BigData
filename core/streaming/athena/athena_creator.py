from dotenv import load_dotenv
import os, boto3, time

load_dotenv()


class AthenaCreator:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(AthenaCreator, cls).__new__(cls, *args, **kwargs)
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

    def get_client(self):
        return self.athena_client

    def run_query(self, query: str, database: str = None) -> bool:
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

    def create_trades_table(self):
        query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS trade (
            trade_id BIGINT,
            event STRING,
            symbol STRING,
            price DOUBLE,
            quantity DOUBLE,
            trade_time TIMESTAMP,
            is_market_maker BOOLEAN
        )
        STORED AS PARQUET
        LOCATION 's3://{self.BUCKET_NAME}/trade/'
        """
        if self.run_query(query, database=self.ATHENA_DB):
            print("✅ Table created successfully")
        else:
            print("❌ Failed to create table")

    def create_ticker_table(self):
        query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS ticker (
            event STRING,
            event_time TIMESTAMP,
            symbol STRING,
            price_change DOUBLE,
            price_change_percent DOUBLE,
            weighted_avg_price DOUBLE,
            prev_close_price DOUBLE,
            last_price DOUBLE,
            last_qty DOUBLE,
            best_bid_price DOUBLE,
            best_bid_qty DOUBLE,
            best_ask_price DOUBLE,
            best_ask_qty DOUBLE,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            base_volume DOUBLE,
            quote_volume DOUBLE,
            open_time TIMESTAMP,
            close_time TIMESTAMP,
            first_trade_id BIGINT,
            last_trade_id BIGINT,
            trade_count BIGINT
        )
        STORED AS PARQUET
        LOCATION 's3://{self.BUCKET_NAME}/ticker/'
        """
        if self.run_query(query, database=self.ATHENA_DB):
            print("✅ Ticker table created successfully")
        else:
            print("❌ Failed to create ticker table")

    def create_bookticker_table(self):
        query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS bookTicker (
            update_id BIGINT,
            event_time TIMESTAMP,
            symbol STRING,
            best_bid_price DOUBLE,
            best_bid_qty DOUBLE,
            best_ask_price DOUBLE,
            best_ask_qty DOUBLE
        )
        STORED AS PARQUET
        LOCATION 's3://{self.BUCKET_NAME}/bookTicker/'
        """
        if self.run_query(query, database=self.ATHENA_DB):
            print("✅ BookTicker table created successfully")
        else:
            print("❌ Failed to create bookTicker table")

    def run_athena(self):
        self.create_database()
        self.create_trades_table()
        self.create_ticker_table()
        self.create_bookticker_table()


if __name__ == "__main__":
    athena = AthenaCreator()
    athena.run_athena()
