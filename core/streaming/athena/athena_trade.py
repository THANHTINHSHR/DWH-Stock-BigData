import time, logging


class AthenaTrade:
    def __init__(self, athena_client, s3_staging, s3_bucket_name, athena_db):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.athena_client = athena_client
        self.s3_staging = s3_staging
        self.s3_bucket_name = s3_bucket_name
        self.athena_db = athena_db
        self.tabke_name = "trade"

    def run_query(self, query: str, database: str = None) -> bool:
        params = {
            "QueryString": query,
            "ResultConfiguration": {
                "OutputLocation": f"{self.s3_staging}query-results/"
            },
        }
        if database:
            params["QueryExecutionContext"] = {"Database": database}

        response = self.athena_client.start_query_execution(**params)
        query_execution_id = response["QueryExecutionId"]

        while True:
            result = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = result["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(1)

        self.logger.info(f"📌Query status : {status}")
        return status == "SUCCEEDED"

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
        LOCATION 's3://{self.s3_bucket_name}/trade/'
        """
        if self.run_query(query, database=self.athena_db):
            print("✅ Table created successfully")
        else:
            print("❌ Failed to create table")

    def run(self):
        self.create_trades_table()
