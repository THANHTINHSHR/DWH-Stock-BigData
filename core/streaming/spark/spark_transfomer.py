from core.streaming.spark.spark_session_singleton import SparkSessionSingleton
from core.extract.binance_wss.topic_creator import TopicCreator
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    TimestampType,
    Row,
)
from pyspark.sql.functions import from_json, col, isnan
from functools import reduce

from pathlib import Path


import os, json, logging
from dotenv import load_dotenv

load_dotenv()


class SparkTransformer:
    def __init__(self):
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.spark = SparkSessionSingleton.get_spark_session()
        self.streams = {}
        # DEBUG - stream counter
        self.counter = 0
        # Schema
        self.streams_schema = {}
        self.load_all_schemas()
        # Condition
        self.filter_conditions = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def avro_type_to_spark_type(self, avro_type):
        """Map basic Avro types to PySpark types"""
        if isinstance(avro_type, list):
            avro_type = [t for t in avro_type if t != "null"][0]
        if isinstance(avro_type, dict):
            return self.avro_to_struct(avro_type)
        mapping = {
            "string": StringType(),
            "int": IntegerType(),
            "long": LongType(),
            "float": FloatType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
        }
        return mapping.get(avro_type, StringType())

    def avro_to_struct(self, avro_schema):
        """Convert Avro schema dict to PySpark StructType"""
        fields = []
        for field in avro_schema.get("fields", []):
            name = field["name"]
            dtype = self.avro_type_to_spark_type(field["type"])
            fields.append(StructField(name, dtype, True))
        return StructType(fields)

    def load_all_schemas(self):
        """Load all .avro files into self.streams_schema"""

        base_dir = Path(__file__).resolve().parent
        parent_dir = base_dir.parent
        schema_dir = parent_dir / "kafka" / "schema_avsc"

        self.logger.info(f"📁 Schema dir: {schema_dir}")
        self.logger.info(f"📄 Files found: {list(schema_dir.glob('*.avsc'))}")
        for avro_file in schema_dir.glob("*.avsc"):
            self.logger.info(f"🔍 Loading schema file: {avro_file.name}")
            with open(avro_file, "r") as f:
                schema = json.load(f)
                struct = self.avro_to_struct(schema)
                self.streams_schema[avro_file.stem] = struct
        self.logger.info(f"✅ Loaded schemas: {list(self.streams_schema.keys())}")
        return self.streams_schema

    def get_schema(self, schema_name):
        return self.streams_schema[schema_name]

    def show_df_stream(self, data: dict):
        # df = df.selectExpr("cast(value as string)", "timestamp")
        df = data["df"]
        symbol = data["symbol"]
        stream_type = data["stream_type"]
        query = (
            df.writeStream.format("console")
            .outputMode("update")
            .option("truncate", False)
            .option("numRows", 10)
            .start()
        )
        query.awaitTermination()

    def read_stream(self, symbol, stream_type):
        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS)
            .option("startingOffsets", "earliest")  # "latest if deploy
            .option("subscribe", f"{self.BINANCE_TOPIC}_{symbol}")
            .option("groupId", f"{stream_type}")
            .load()
        )

        return {"df": df, "symbol": symbol, "stream_type": stream_type}

    def get_filter_condition(self, df, stream_type):
        # if self.filter_conditions.get(stream_type) is not None: # Logic cache có thể xem xét lại
        #     return self.filter_conditions[stream_type]

        conditions = []
        schema = self.get_schema(stream_type)
        for field in schema.fields:
            name = field.name
            dtype = field.dataType

            # Base: Not null
            cond = col(name).isNotNull()

            # String: not empty
            if isinstance(dtype, StringType):
                cond = cond & (col(name) != "")

            # Numeric: not NaN
            elif isinstance(dtype, (FloatType, DoubleType)):
                cond = cond & (~isnan(col(name)))

            # Boolean: is boolean (isNotNull is usually sufficient)
            elif isinstance(dtype, BooleanType):
                pass  # cond = cond (no additional check needed beyond isNotNull)

            conditions.append(cond)

        final_condition = reduce(lambda a, b: a & b, conditions) if conditions else None
        # self.filter_conditions[stream_type] = final_condition # Cache if needed
        return final_condition

    def transform_stream_generic(self, data: dict):
        df = data["df"]
        symbol = data["symbol"]
        stream_type = data["stream_type"]

        match stream_type:
            case "trade":
                return self.transform_trade_stream(data)
            case _:
                self.logger.warning(
                    f"No specific transformation for stream_type: {stream_type}. Returning raw data."
                )
        return data

    # def to_line_protocol(self, row: Row):
    #     """To influDB line protocol format
    #     ex: trade_data,symbol=BTC price=72000,volume=10 1712830200000000000"""
    #     # This method seems incomplete and uses undefined variables like 'df' and 'skip_col'
    #     # measurement = row["symbol"]
    #     # tags = row["event"]
    #     # # cols_to_keep = [col(c) for c in df.columns if c != skip_col] # 'df' and 'skip_col' are not defined here
    #     # fields = row["price"]
    #     # timestamp = (row["trade_time"].cast(DoubleType()) * 1000).cast("long") # Access row elements with []
    #     pass

    def load_stream(self, data: dict):

        # self.show_df_stream(data)
        pass

    def transform_trade_stream(self, data: dict):
        df = data["df"]
        symbol = data["symbol"]
        stream_type = data["stream_type"]
        # df.printSchema() # Consider logging schema
        # Get value from df
        df_value = df.select(
            from_json(
                col("value").cast("string"),
                self.get_schema(stream_type),  # Use get_schema here
            ).alias("value_json"),
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
        df_value = df_value.withColumn("trade_id", col("trade_id").cast(LongType()))
        df_value = df_value.withColumn("event", col("event").cast(StringType()))
        df_value = df_value.withColumn("symbol", col("symbol").cast(StringType()))
        df_value = df_value.withColumn("price", col("price").cast(DoubleType()))
        df_value = df_value.withColumn("quantity", col("quantity").cast(DoubleType()))
        # Minisecond to second milliseconds
        df_value = df_value.withColumn(
            "trade_time", (col("trade_time") / 1000).cast(TimestampType())
        )
        df_value = df_value.withColumn(
            "is_market_maker", col("is_market_maker").cast(BooleanType())
        )
        # Cleaning : Filter null, "",NaN
        condition = self.get_filter_condition(
            df_value, stream_type
        )  # Get the filter condition
        # Transform
        if condition:
            df_value = df_value.filter(condition)
        # df_value = df.select("value", "timestamp")
        return {"df": df_value, "symbol": symbol, "stream_type": stream_type}

    def run_stream(self, symbol, stream_type):
        dict = self.read_stream(symbol, stream_type)
        dict = self.transform_stream_generic(dict)
        self.logger.info(f"📡 Subscribing to topic: {self.BINANCE_TOPIC}_{symbol}")

        self.load_stream(dict)


if __name__ == "__main__":
    tc = TopicCreator()
    st = SparkTransformer()

    # st.show_test()
    # schema = st.get_schema("trade")
    # st.logger.info(schema)

    st.run_stream("btcusdt", "trade")
