
# autopep8: off
import findspark  # type: ignore
findspark.init()
from pyspark.sql.types import * # type: ignore
from pyspark.sql.functions import from_json, col # type: ignore
from pyspark.sql.types import StringType,LongType,DoubleType,TimestampType,Row,BooleanType # type: ignore
from core.streaming.spark.pipeline_base import PipelineBase
from core.streaming.kafka.topic_creator import TopicCreator

from core.streaming.influxDB.influxDB_creator import InfluxDBConnector
import logging
from dotenv import load_dotenv
# autopep8: on
load_dotenv()


class TradePipeline(PipelineBase):

    def __init__(self):
        self.type = "trade"
        super().__init__(self.type)
        self.schema = super().get_schema(self.type)
        self.filter_condition = self.get_filter_condition(self.type)
        self.influxDB = InfluxDBConnector.get_instance()
        self.logger = logging.getLogger(self.__class__.__name__)

    def transform_stream(self, data: dict):
        df = data["df"]
        symbol = data["symbol"]
        # Get value from df
        df_value = df.select(
            from_json(col("value").cast("string"),
                      self.schema).alias("value_json"),
        ).select("value_json.*")
        df_value = df_value.select(
            col("t").alias("trade_id"),
            col("e").alias("event"),
            col("s").alias("symbol"),
            col("p").alias("price"),
            col("q").alias("quantity"),
            col("T").alias("trade_time"),
            col("m").alias("is_market_maker"),
        )
        # Cleaning--> Transform--> Filter
        # Cleaning: Cast
        df_value = df_value.withColumn(
            "trade_id", col("trade_id").cast(LongType()))
        df_value = df_value.withColumn(
            "event", col("event").cast(StringType()))
        df_value = df_value.withColumn(
            "symbol", col("symbol").cast(StringType()))
        df_value = df_value.withColumn(
            "price", col("price").cast(DoubleType()))
        df_value = df_value.withColumn(
            "quantity", col("quantity").cast(DoubleType()))
        # Minisecond to second milliseconds
        df_value = df_value.withColumn(
            "trade_time", (col("trade_time") / 1000).cast(TimestampType())
        )
        df_value = df_value.withColumn(
            "is_market_maker", col("is_market_maker").cast(BooleanType())
        )
        # Cleaning : Filter null, "",NaN
        condition = self.get_filter_condition(self.type)
        # Transform
        df_value = df_value.filter(condition)
        # df_value = df.select("value", "timestamp")

        return {"df": df_value, "symbol": symbol}

    def to_line_protocol(self, row: Row):
        """Convert to InfluxDB line protocol format
        <measurement>,<tag_set> <field_set> <timestamp>
        ex:  trade,symbol=SOLUSDT price=132.65,quantity=0.076 1744601341176000000
        """
        measurement = self.type
        tag_set = f"symbol={row['symbol']}"
        field_price = row["price"]
        field_quantity = row["quantity"]
        field_set = f"price={field_price},quantity={field_quantity}"
        # Convert timestamp to nanoseconds for InfluxDB
        # row["trade_time"].timestamp() returns seconds (float)
        # multiply by 1_000_000_000 to get nanoseconds
        timestamp = int(row["trade_time"].timestamp() * 1_000_000_000)
        return f"{measurement},{tag_set} {field_set} {timestamp}"


if __name__ == "__main__":
    trade_pipeline = TradePipeline()
    tc = TopicCreator()
    trade_pipeline.run_streams()
