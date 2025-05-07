import time, logging


class AthenaTicker:
    def __init__(self, athena_client, s3_staging, s3_bucket_name, athena_db):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.athena_client = athena_client
        self.s3_staging = s3_staging
        self.s3_bucket_name = s3_bucket_name
        self.athena_db = athena_db
        self.tabke_name = "ticker"

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
            LOCATION 's3://{self.s3_bucket_name}/ticker/'
            """
        if self.run_query(query, database=self.athena_db):
            print("✅ Ticker table created successfully")
        else:
            print("❌ Failed to create ticker table")

    def create_haeathmap_ticker_view(self):
        sql = """CREATE OR REPLACE VIEW heatmap_ticker AS
            SELECT symbol, close_time, price_change_percent,quote_volume,'all' AS constant 
            FROM (
                SELECT 
                    symbol, 
                    price_change_percent,
                    quote_volume,
                    close_time,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY close_time DESC) AS rn
                FROM binance.ticker
            ) t
            WHERE rn = 1;

                """
        if self.run_query(sql, self.athena_db):
            self.logger.info("✅ View created successfully")
        else:
            self.logger.error("❌ Failed to create View")

    def create_scatter_plot_ticker_view(self):
        query = """CREATE OR REPLACE VIEW scatter_plot_ticker AS 
WITH RankedTicker AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_time DESC) as rn
    FROM binance.ticker 
)
SELECT
    event_time,
    symbol,
    price_change_percent, 
    quote_volume,        
    trade_count,         
    last_price
FROM RankedTicker
WHERE rn = 1;
"""
        if self.run_query(query, self.athena_db):
            self.logger.info("✅ View created successfully")
        else:
            self.logger.error("❌ Failed to create View")

    def run(self):
        self.create_ticker_table()
        self.create_haeathmap_ticker_view()
        self.create_scatter_plot_ticker_view()
