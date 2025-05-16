from core.streaming.spark.pipeline_base import PipelineBase
from core.streaming.kafka.topic_creator import TopicCreator
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from core.streaming.influxDB.influxDB_creator import InfluxDBConnector
import time, logging
from dotenv import load_dotenv
from pyspark.sql.functions import current_timestamp

load_dotenv()


class BookTickerPipeline(PipelineBase):
    def __init__(self):
        super().__init__()
        self.type = "bookTicker"
        self.spark = super().get_spark_session("bookticker_pipeline")
        self.schema = super().get_schema(self.type)
        self.filter_condition = self.get_filter_condition(self.type)
        self.influxDB = InfluxDBConnector.get_instance()
        self.logger = logging.getLogger(self.__class__.__name__)

    def read_stream(self, symbol):
        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS)
            .option("startingOffsets", "earliest")
            .option("subscribe", f"{self.BINANCE_TOPIC}_{self.type}")
            .option("groupId", f"{symbol}")
            .load()
        )
        return {"df": df, "symbol": symbol}

    def transform_stream(self, data: dict):
        df = data["df"]
        symbol = data["symbol"]

        df_value = df.select(
            from_json(col("value").cast("string"), self.schema).alias("value_json")
        ).select("value_json.*")

        df_value = df_value.select(
            col("s").alias("symbol"),
            col("u").alias("update_id"),
            col("b").alias("best_bid_price"),
            col("B").alias("best_bid_qty"),
            col("a").alias("best_ask_price"),
            col("A").alias("best_ask_qty"),
        )

        df_value = df_value.withColumn("symbol", col("symbol").cast(StringType()))
        df_value = df_value.withColumn("update_id", col("update_id").cast(LongType()))
        df_value = df_value.withColumn(
            "best_bid_price", col("best_bid_price").cast(DoubleType())
        )
        df_value = df_value.withColumn(
            "best_bid_qty", col("best_bid_qty").cast(DoubleType())
        )
        df_value = df_value.withColumn(
            "best_ask_price", col("best_ask_price").cast(DoubleType())
        )
        df_value = df_value.withColumn(
            "best_ask_qty", col("best_ask_qty").cast(DoubleType())
        )

        df_value = df_value.filter(self.filter_condition)
        # Add column timestamp for df_value
        df_value = df_value.withColumn(
            "event_time", current_timestamp()
        )  # <-- Sửa ở đây

        return {"df": df_value, "symbol": symbol}

    def to_line_protocol(self, row: Row):
        measurement = self.type
        tag_set = f"symbol={row['symbol']}"
        field_set = (
            f"best_bid_price={row['best_bid_price']},"
            f"best_bid_qty={row['best_bid_qty']},"
            f"best_ask_price={row['best_ask_price']},"
            f"best_ask_qty={row['best_ask_qty']},"
            f"update_id={row['update_id']}"
        )
        timestamp = int(time.time() * 1_000_000_000)
        return f"{measurement},{tag_set} {field_set} {timestamp}"

    def load_to_InfluxDB(self, df):
        self.logger.info(f"✅ Sending data to influxDB: {self.type}")
        for row in df.toLocalIterator():
            self.influxDB.send_line_data(self.type, self.to_line_protocol(row))

    def run_streams(self):
        self.logger.info(
            f"✅ Starting {self.type} streams for {len(TopicCreator.TOPCOIN)} symbols."
        )
        queries = []
        for symbol in TopicCreator.TOPCOIN:
            self.logger.info(f"✅ Setting up stream for {self.type}: {symbol}")
            raw_data = self.read_stream(symbol)
            # Consider logging raw schema if needed
            transformed_data = self.transform_stream(raw_data)
            df_to_influx = transformed_data["df"].select("*")
            df_to_s3 = transformed_data["df"].select("*")

            query_influx = df_to_influx.writeStream.foreachBatch(
                lambda df, epoch_id: self.load_to_InfluxDB(df)
            ).start()

            query_s3 = df_to_s3.writeStream.foreachBatch(
                lambda df, epoch_id: self.load_to_S3(df, self.type)
            ).start()

            queries.append(query_s3)
            queries.append(query_influx)
        for query in queries:
            query.awaitTermination()


if __name__ == "__main__":
    book_ticker_pipeline = BookTickerPipeline()
    tc = TopicCreator()
    # No need to print type here, it's logged during stream setup
    book_ticker_pipeline.run_streams()
