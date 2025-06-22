# autopep8: off
import findspark  # type: ignore
findspark.init()
from pyspark.sql.types import * # type: ignore
from pyspark.sql.types import StringType, LongType, DoubleType  # type: ignore
from pyspark.sql.functions import from_json, col    # type: ignore
from pyspark.sql.functions import current_timestamp # type: ignore
from core.streaming.spark.pipeline_base import PipelineBase 
from core.streaming.kafka.topic_creator import TopicCreator
from core.streaming.influxDB.influxDB_creator import InfluxDBConnector
import time, logging
from dotenv import load_dotenv
# autopep8: on

load_dotenv()


class BookTickerPipeline(PipelineBase):
    def __init__(self):
        self.type = "bookTicker"
        super().__init__(type=self.type)
        self.schema = super().get_schema(self.type)
        self.filter_condition = self.get_filter_condition(self.type)
        self.influxDB = InfluxDBConnector.get_instance()
        self.logger = logging.getLogger(self.__class__.__name__)

    def transform_stream(self, data: dict):
        df = data["df"]
        symbol = data["symbol"]

        df_value = df.select(
            from_json(col("value").cast("string"),
                      self.schema).alias("value_json")
        ).select("value_json.*")

        df_value = df_value.select(
            col("s").alias("symbol"),
            col("u").alias("update_id"),
            col("b").alias("best_bid_price"),
            col("B").alias("best_bid_qty"),
            col("a").alias("best_ask_price"),
            col("A").alias("best_ask_qty"),
        )

        df_value = df_value.withColumn(
            "symbol", col("symbol").cast(StringType()))
        df_value = df_value.withColumn(
            "update_id", col("update_id").cast(LongType()))
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
        )

        return {"df": df_value, "symbol": symbol}

    def to_line_protocol(self, row: Row):  # type: ignore
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


if __name__ == "__main__":
    book_ticker_pipeline = BookTickerPipeline()
    tc = TopicCreator()
    # No need to print type here, it's logged during stream setup
    book_ticker_pipeline.run_streams()
