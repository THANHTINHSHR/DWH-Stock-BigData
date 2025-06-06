from core.streaming.spark.pipeline_base import PipelineBase
from core.streaming.kafka.topic_creator import TopicCreator
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from core.streaming.influxDB.influxDB_creator import InfluxDBConnector
import logging
from dotenv import load_dotenv

load_dotenv()


class TickerPipeline(PipelineBase):

    def __init__(self):
        super().__init__()
        self.type = "ticker"
        self.spark = super().get_spark_session("ticker_pipeline")
        self.schema = super().get_schema(self.type)
        self.filter_condition = self.get_filter_condition(self.type)
        self.influxDB = InfluxDBConnector.get_instance()
        self.logger = logging.getLogger(self.__class__.__name__)

    def read_stream(self, symbol):
        subscribe_topic = f"{self.BINANCE_TOPIC}_{symbol}"
        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS)
            .option("startingOffsets", "earliest")  # "latest if deploy
            .option("subscribe", f"{self.BINANCE_TOPIC}_{self.type}")
            .option("groupId", f"{symbol}")
            .load()
        )

        return {"df": df, "symbol": symbol}

    def transform_stream(self, data: dict):
        df = data["df"]
        symbol = data["symbol"]
        # df.printSchema() # Consider logging schema if needed, e.g., self.logger.info(f"Schema for {symbol}: {df.schema.json()}")

        df_value = df.select(
            from_json(col("value").cast("string"), self.schema).alias("value_json"),
        ).select("value_json.*")

        df_value = df_value.select(
            col("e").alias("event"),
            col("E").alias("event_time"),
            col("s").alias("symbol"),
            col("p").alias("price_change"),
            col("P").alias("price_change_percent"),
            col("w").alias("weighted_avg_price"),
            col("x").alias("prev_close_price"),
            col("c").alias("last_price"),
            col("Q").alias("last_qty"),
            col("b").alias("best_bid_price"),
            col("B").alias("best_bid_qty"),
            col("a").alias("best_ask_price"),
            col("A").alias("best_ask_qty"),
            col("o").alias("open_price"),
            col("h").alias("high_price"),
            col("l").alias("low_price"),
            col("v").alias("base_volume"),
            col("q").alias("quote_volume"),
            col("O").alias("open_time"),
            col("C").alias("close_time"),
            col("F").alias("first_trade_id"),
            col("L").alias("last_trade_id"),
            col("n").alias("trade_count"),
        )

        # Cast types
        df_value = df_value.withColumn("event", col("event").cast(StringType()))
        df_value = df_value.withColumn(
            "event_time", (col("event_time") / 1000).cast(TimestampType())
        )
        df_value = df_value.withColumn("symbol", col("symbol").cast(StringType()))
        df_value = df_value.withColumn(
            "price_change", col("price_change").cast(DoubleType())
        )
        df_value = df_value.withColumn(
            "price_change_percent", col("price_change_percent").cast(DoubleType())
        )
        df_value = df_value.withColumn(
            "weighted_avg_price", col("weighted_avg_price").cast(DoubleType())
        )
        df_value = df_value.withColumn(
            "prev_close_price", col("prev_close_price").cast(DoubleType())
        )
        df_value = df_value.withColumn(
            "last_price", col("last_price").cast(DoubleType())
        )
        df_value = df_value.withColumn("last_qty", col("last_qty").cast(DoubleType()))
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
        df_value = df_value.withColumn(
            "open_price", col("open_price").cast(DoubleType())
        )
        df_value = df_value.withColumn(
            "high_price", col("high_price").cast(DoubleType())
        )
        df_value = df_value.withColumn("low_price", col("low_price").cast(DoubleType()))
        df_value = df_value.withColumn(
            "base_volume", col("base_volume").cast(DoubleType())
        )
        df_value = df_value.withColumn(
            "quote_volume", col("quote_volume").cast(DoubleType())
        )
        df_value = df_value.withColumn(
            "open_time", (col("open_time") / 1000).cast(TimestampType())
        )
        df_value = df_value.withColumn(
            "close_time", (col("close_time") / 1000).cast(TimestampType())
        )
        df_value = df_value.withColumn(
            "first_trade_id", col("first_trade_id").cast(LongType())
        )
        df_value = df_value.withColumn(
            "last_trade_id", col("last_trade_id").cast(LongType())
        )
        df_value = df_value.withColumn(
            "trade_count", col("trade_count").cast(LongType())
        )

        # Filter if needed
        condition = self.get_filter_condition(self.type)
        df_value = df_value.filter(condition)

        return {"df": df_value, "symbol": symbol}

    def to_line_protocol(self, row: Row):
        measurement = self.type
        tag_set = f"symbol={row['symbol']}"
        field_set = (
            f"price_change={row['price_change']},"
            f"price_change_percent={row['price_change_percent']},"
            f"weighted_avg_price={row['weighted_avg_price']},"
            f"prev_close_price={row['prev_close_price']},"
            f"last_price={row['last_price']},"
            f"last_qty={row['last_qty']},"
            f"best_bid_price={row['best_bid_price']},"
            f"best_bid_qty={row['best_bid_qty']},"
            f"best_ask_price={row['best_ask_price']},"
            f"best_ask_qty={row['best_ask_qty']},"
            f"open_price={row['open_price']},"
            f"high_price={row['high_price']},"
            f"low_price={row['low_price']},"
            f"base_volume={row['base_volume']},"
            f"quote_volume={row['quote_volume']},"
            f"first_trade_id={row['first_trade_id']},"
            f"last_trade_id={row['last_trade_id']},"
            f"trade_count={row['trade_count']}"
        )
        timestamp = int(row["close_time"].timestamp() * 1_000_000_000)
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
            transformed_data = self.transform_stream(raw_data)
            # self.show_df_stream(transformed_data)
            df_to_influx = transformed_data["df"].select("*")
            df_to_s3 = transformed_data["df"].select("*")
            symbol = transformed_data["symbol"]
            # To Influx
            query_influx = df_to_influx.writeStream.foreachBatch(
                lambda df, epoch_id: self.load_to_InfluxDB(df)
            ).start()
            # To s3
            query_s3 = df_to_s3.writeStream.foreachBatch(
                lambda df, epoch_id: self.load_to_S3(df, self.type)
            ).start()
            # queries.append(query_influx)
            queries.append(query_s3)
            queries.append(query_influx)
        for query in queries:
            query.awaitTermination()


if __name__ == "__main__":
    ticker_pipeline = TickerPipeline()
    tc = TopicCreator()
    # No need to print type here, it's logged during stream setup
    ticker_pipeline.run_streams()
