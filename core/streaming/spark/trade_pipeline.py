
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
        self.spark = super().get_spark_session("trade_pipeline")
        self.schema = super().get_schema(self.type)
        self.filter_condition = self.get_filter_condition(self.type)
        self.influxDB = InfluxDBConnector.get_instance()
        self.logger = logging.getLogger(self.__class__.__name__)

    def read_stream(self, symbol):
        subscribe_topic = f"{self.BINANCE_TOPIC}_{symbol}"
        try:
            df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS)
                .option("startingOffsets", "latest")  # "latest if deploy
                .option("subscribe", f"{self.BINANCE_TOPIC}_{self.type}")
                .option("groupId", f"{symbol}")
                .load()
            )
            self.logger.info(f"✅ Reading Stream for {self.type}")
        except Exception as e:
            self.logger.error(f"❌ Error reading streaming data: {e}")

        return {"df": df, "symbol": symbol}

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

    def load_to_InfluxDB(self, df):
        self.logger.info(f"✅ Sending data to influxDB: {self.type}")
        for row in df.toLocalIterator():
            self.influxDB.send_line_data(self.type, self.to_line_protocol(row))

    def run_streams(self):
        try:
            self.logger.info(
                f"✅ topcoin stream length: {len(TopicCreator.TOPCOIN)}")
            queries = []
            for symbol in TopicCreator.TOPCOIN:
                self.logger.info(f"✅ Topcoin : {symbol}")
                raw_data = self.read_stream(symbol)
                raw_df = raw_data["df"]

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
        except Exception as e:
            self.logger.error(f"❌ Error in streaming process:{e}")


if __name__ == "__main__":
    trade_pipeline = TradePipeline()
    tc = TopicCreator()
    trade_pipeline.run_streams()
