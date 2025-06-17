# autopep8: off
import findspark  # type: ignore
findspark.init()
from pyspark.sql import DataFrame  # type: ignore
from datetime import datetime, timedelta
import logging,os
from dotenv import load_dotenv
from abc import ABC, abstractmethod
from core.streaming.informerAI.models.spark_loader import SparkLoader  # type: ignore
import numpy as np
from torch.utils.data import TensorDataset
import torch

# autopep8: on
load_dotenv()


class DataProcessor(ABC):
    def __init__(self, type: str = "DataProcessor"):
        self.type = type
        self.spark_loader = SparkLoader()
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1024))
        self.N_DAYS_AGO = int(os.getenv("N_DAYS_AGO", 1))
        self.MAX_DIRECTORIES = int(os.getenv("MAX_DIRECTORIES", 10))
        self.SEQUENCE_LENGTH = int(os.getenv("SEQUENCE_LENGTH", 18000))  # 5h
        self.PREDICTION_LENGTH = int(
            os.getenv("PREDICTION_LENGTH", 3600))  # 1h
        self.TRAIN_RATIO = float(os.getenv("TRAIN_RATIO", 0.7))
        self.VAL_RATIO = float(os.getenv("VAL_RATIO", 0.2))
        self.NUM_EPOCHS = int(os.getenv("NUM_EPOCHS", 200))
        # Saved Symbol
        self.symbol_labels = []
        # columns
        self.start_cols = []
        self.final_cols = []
        self.feature_cols = []
        self.time_cols = []

        # Cleaned Dict DataFrame
        self.symbol_dict = {}
        self.symbol_last_event_time = {}
        self.symbol_min_max = {}

        # Logging configuration
        self.logger = logging.getLogger(self.__class__.__name__)

    def run_test(self):
        # df = self.spark_loader.read_csv("1day598dir.csv")
        df = self.spark_loader.read_csv("1day1000dir.csv")

        symbol_dict = self.process_train(df)
        self.symbol_dict = symbol_dict
        return symbol_dict

    def run(self, stream_type: str):
        df = self.get_raw_data(
            stream_type, self.N_DAYS_AGO, self.MAX_DIRECTORIES)
        symbol_dict = self.process_train(df)
        self.symbol_dict = symbol_dict
        return symbol_dict

    def get_raw_data(self, stream_type, n_day_ago, max_directories) -> DataFrame:
        return self.spark_loader.read_s3(stream_type, n_day_ago, max_directories)

    def process_train(self, df: DataFrame):
        """
        Get data anđ train AI with 3 dataset
        """
        self.logger.info(f"⏳ Processing Train data with {self.type}...")
        df = self.feature_selection(df)
        df = self.timeHanding(df)
        df = self.normalization(df)
        self.splitBySymbolAndClean(df)
        for symbol, df_symbol in self.symbol_dict.items():
            df_train, df_val, df_test = self.splitBatching(df_symbol)
            # Save to csv to debug
            self.spark_loader.write_csv(df_train, f"{symbol}_train.csv")
            self.spark_loader.write_csv(df_val, f"{symbol}_val.csv")
            self.spark_loader.write_csv(df_test, f"{symbol}_test.csv")
            self.symbol_dict[symbol] = {
                "train": df_train,
                "val": df_val,
                "test": df_test
            }
        return self.convert_dict_spark_data_to_dict_tensor_data(
            self.symbol_dict)

    def process_predict(self, df: DataFrame):
        self.logger.info(f"⏳ Processing Predict data with {self.type}...")
        df = self.feature_selection(df)
        df = self.timeHanding(df)
        df = self.normalization(df)
        self.splitBySymbolAndClean(df)
        for symbol, df_symbol in self.symbol_dict.items():
            df_train, df_val, df_test = self.splitBatching(df_symbol)
            self.symbol_dict[symbol] = {
                "train": df_train,
                "val": df_val,
                "test": df_test
            }
        return self.convert_dict_spark_data_to_dict_tensor_data(
            symbol_dict=self.symbol_dict)

    @abstractmethod
    def feature_selection(self, df: DataFrame) -> DataFrame:
        return df

    @abstractmethod
    def timeHanding(self, df: DataFrame) -> DataFrame:
        return df

    @abstractmethod
    def normalization(self, df: DataFrame) -> DataFrame:
        return df

    @abstractmethod
    def splitBySymbolAndClean(self, df: DataFrame) -> dict:
        pass

    @abstractmethod
    def splitBatching(self, df: DataFrame) -> DataFrame:
        return df, df, df

    def convert_dict_spark_data_to_dict_tensor_data(self, symbol_dict: dict):
        for symbol, symbol_data in symbol_dict.items():
            self.logger.info(f"⏳ Converting df of symbol: {symbol}")
            symbol_dict[symbol] = {
                "train": self.convert_sparkDF_to_tensorDS(symbol_data["train"], self.feature_cols, self.time_cols),
                "val": self.convert_sparkDF_to_tensorDS(symbol_data["val"], self.feature_cols, self.time_cols),
                "test": self.convert_sparkDF_to_tensorDS(symbol_data["test"], self.feature_cols, self.time_cols),
            }
        return symbol_dict

    def convert_sparkDF_to_tensorDS(self, df: DataFrame, feature_cols: list, time_cols: list):
        feature_data = df.select(
            feature_cols).toPandas().astype("float32").values
        time_data = df.select(time_cols).toPandas().astype("float32").values

        seq_len = self.SEQUENCE_LENGTH
        pred_len = self.PREDICTION_LENGTH
        label_len = seq_len - pred_len

        num_features = feature_data.shape[1]
        data_len = len(feature_data)

        if data_len < seq_len + pred_len:
            self.logger.warning(
                f"🟡 Not enough data: {data_len} (required ≥ {seq_len + pred_len})"
            )
            return TensorDataset(torch.empty(0), torch.empty(0), torch.empty(0), torch.empty(0), torch.empty(0))

        x, x_mark_enc, x_dec, x_mark_dec, y = [], [], [], [], []

        for i in range(data_len - seq_len - pred_len + 1):
            x_seq = feature_data[i: i + seq_len]
            x_time_enc = time_data[i: i + seq_len]

            start_dec = i + seq_len - label_len
            dec_input_known = feature_data[start_dec: i + seq_len]
            zeros_pred = np.zeros((pred_len, num_features), dtype=np.float32)
            final_dec_input = np.concatenate(
                [dec_input_known, zeros_pred], axis=0)

            dec_time = time_data[start_dec: i + seq_len + pred_len]
            target = feature_data[i + seq_len: i + seq_len + pred_len]

            # Optional sanity check
            assert final_dec_input.shape[0] == label_len + pred_len
            assert dec_time.shape[0] == label_len + pred_len

            x.append(x_seq)
            x_mark_enc.append(x_time_enc)
            x_dec.append(final_dec_input)
            x_mark_dec.append(dec_time)
            y.append(target)

        return TensorDataset(
            torch.tensor(np.stack(x)),
            torch.tensor(np.stack(x_mark_enc)),
            torch.tensor(np.stack(x_dec)),
            torch.tensor(np.stack(x_mark_dec)),
            torch.tensor(np.stack(y))
        )
