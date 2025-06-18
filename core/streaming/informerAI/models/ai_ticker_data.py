# autopep8: off
import findspark  # type: ignore
findspark.init()

from pyspark.sql.functions import col, sin, lit, when, dayofweek, date_format # type:ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.functions import lit # type: ignore
from pyspark.sql import Window  # type: ignore
from pyspark.sql.functions import min as spark_min, max as spark_max # type: ignore
from math import pi # type: ignore
import numpy as np # type: ignore


from core.streaming.informerAI.models.data_processor import DataProcessor  # type: ignore
import logging # Import the logging module

# autopep8: on


class AITickerData(DataProcessor):

    def __init__(self):
        self.type = "AITickerData"
        super().__init__(self.type)
        # Selected columns
        self.start_cols = ["event_time", "symbol", "last_price",
                           "best_bid_price", "best_ask_price", "trade_count"]
        # Final columns
        self.final_cols = [
            "last_price",
            "best_bid_price", "best_ask_price", "trade_count",
            "month_sin", "day_sin", "weekday_sin",
            "hour_sin", "minute_sin", "second_sin"
        ]
        # Feature columns
        self.feature_cols = [
            "last_price", "best_bid_price", "best_ask_price", "trade_count"
        ]
        # Time columns
        self.time_cols = [
            "month_sin", "day_sin", "weekday_sin",
            "hour_sin", "minute_sin", "second_sin"
        ]

        # Cleaned dict Dataframe
        self.symbol_dict = {}

        # Log
        self.logger = logging.getLogger(self.__class__.__name__)

    def feature_selection(self, df: DataFrame) -> DataFrame:
        self.logger.info(f"⏳ Processing feature selection with {self.type}...")
        df = df.select(*self.start_cols)
        return df

    def timeHanding(self, df):
        """
        month, day, weekday, hour, minute, second	
        'embed': 'timeF',
            'freq': 's'
        """
        time_column = "event_time"

        self.logger.info(f"⏳ Processing time handing with {self.type}...")

        df = df.withColumn("year", date_format(
            col(time_column), "yyyy").cast("int"))
        df = df.withColumn("month", date_format(
            col(time_column), "MM").cast("int"))
        df = df.withColumn("day", date_format(
            col(time_column), "dd").cast("int"))
        df = df.withColumn("hour", date_format(
            col(time_column), "HH").cast("int"))
        df = df.withColumn("minute", date_format(
            col(time_column), "mm").cast("int"))
        df = df.withColumn("second", date_format(
            col(time_column), "ss").cast("int"))

        df = df.withColumn("dayInMonth",
                           when(col("month").isin(
                               [1, 3, 5, 7, 8, 10, 12]), lit(31))
                           .when(col("month").isin([4, 6, 9, 11]), lit(30))
                           .when((col("month") == 2) & (col("year") % 4 == 0), lit(29))
                           .otherwise(lit(28)))

        # Only keep sin features (remove cos for 'timeF')
        df = df.withColumn("month_sin", sin(2 * pi * col("month") / lit(12)))
        df = df.withColumn("day_sin", sin(
            2 * pi * col("day") / col("dayInMonth")))
        df = df.withColumn("weekday_sin", sin(
            2 * pi * dayofweek(col(time_column)) / lit(7)))
        df = df.withColumn("hour_sin", sin(2 * pi * col("hour") / lit(24)))
        df = df.withColumn("minute_sin", sin(2 * pi * col("minute") / lit(60)))
        df = df.withColumn("second_sin", sin(2 * pi * col("second") / lit(60)))

        df = df.dropDuplicates(["symbol", "event_time"])

        # Drop unused
        df = df.drop("year", "month", "day", "hour",
                     "minute", "second", "dayInMonth")
        # Return 12 column (6+6)
        """
        self.time_handing_cols = [
            "event_time", "symbol", "last_price",
            "best_bid_price", "best_ask_price", "trade_count",
            "month_sin", "day_sin", "weekday_sin",
            "hour_sin", "minute_sin", "second_sin"
        ]
        """
        return df

    def normalization(self, df: DataFrame) -> DataFrame:
        self.logger.info(f"⏳ Processing normalization with {self.type}...")

        feature_cols = self.feature_cols
        symbol_window = Window.partitionBy("symbol")

        symbols = df.select("symbol").distinct(
        ).rdd.flatMap(lambda x: x).collect()

        original_df = df

        for col_name in feature_cols:
            # 🔹 Save -Min,Max before normalization
            for symbol in symbols:
                row = original_df.filter(col("symbol") == symbol).agg(
                    spark_min(col(col_name)).alias("min_val"),
                    spark_max(col(col_name)).alias("max_val")
                ).first()

                if symbol not in self.symbol_min_max:
                    self.symbol_min_max[symbol] = {}

                self.symbol_min_max[symbol][col_name] = (
                    row["min_val"], row["max_val"])

            # Normalization
            min_col = f"{col_name}_min"
            max_col = f"{col_name}_max"
            df = df.withColumn(min_col, spark_min(
                col(col_name)).over(symbol_window))
            df = df.withColumn(max_col, spark_max(
                col(col_name)).over(symbol_window))

            df = df.withColumn(
                col_name,
                when(
                    col(max_col) != col(min_col),
                    (col(col_name) - col(min_col)) /
                    (col(max_col) - col(min_col))
                ).otherwise(lit(0.0))
            )

            df = df.drop(min_col, max_col)
            # Return 16 column : (12+0)
            """
            self.normalization_cols = [
                    
                "event_time", "symbol", "last_price",
                "best_bid_price", "best_ask_price", "trade_count",
                "month_sin", "day_sin", "weekday_sin",
                "hour_sin", "minute_sin", "second_sin"
            ]
                
            """
        return df

    def splitBySymbolAndClean(self, df: DataFrame) -> dict:
        self.logger.info(
            f"⏳ Processing sequence generation with {self.type}...")

        symbols = df.select("symbol").distinct(
        ).rdd.flatMap(lambda x: x).collect()

        # Save event_time
        for symbol in symbols:
            last_time = df.filter(df.symbol == symbol).orderBy(
                "event_time", ascending=False).select("event_time").first()[0]
            self.symbol_last_event_time[symbol] = last_time

        # Create Dict
        symbol_dict = {
            symbol: df.filter(df.symbol == symbol).orderBy(
                "event_time").drop("symbol", "event_time")
            for symbol in symbols
        }

        self.symbol_dict = symbol_dict
        self.logger.info(
            f"✅ Generated {len(symbol_dict)} DataFrames grouped by symbol")

        # Return 10 column : (12-2)
        """
        self.final_cols = [
            "last_price",
            "best_bid_price", "best_ask_price", "trade_count",
            "month_sin", "day_sin", "weekday_sin",
            "hour_sin", "minute_sin", "second_sin"
        ]
        """
        return symbol_dict

    def splitBatching(self, df: DataFrame):
        self.logger.info(
            f"⏳ Splitting: Train {self.TRAIN_RATIO*100}%, Val {self.VAL_RATIO*100}%, Test {round((1 - self.TRAIN_RATIO - self.VAL_RATIO)*100)}%...")
        self.logger.info(f"📌  len(df): {df.count()}")
        # Define Ration
        total_count = df.count()
        train_count = int(total_count * self.TRAIN_RATIO)
        val_count = int(total_count * self.VAL_RATIO)

        # Pagination
        train_df = df.limit(train_count)
        val_df = df.limit(train_count + val_count).subtract(train_df)
        test_df = df.subtract(train_df).subtract(val_df)

        self.logger.info(
            f"✅ Done! Train: {train_df.count()} | Val: {val_df.count()} | Test: {test_df.count()}")

        return train_df, val_df, test_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    ai_ticker_processor = AITickerData()
    ai_ticker_processor.run()
