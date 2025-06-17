# autopep8: off
import findspark  # type: ignore
findspark.init()
from core.streaming.informerAI.models.data_processor import DataProcessor  # type: ignore
from core.streaming.informerAI.models.spark_loader import SparkLoader  # type: ignore
from core.streaming.informerAI.input.tensor_encoder import TensorEncoder  # type: ignore
from abc import ABC, abstractmethod
from argparse import Namespace  # type: ignore
import logging, os, sys  # type: ignore
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from pyspark.sql import Row, DataFrame  # type: ignore
from datetime import timedelta
from pyspark.sql.types import TimestampType # type: ignore
from pyspark.sql.functions import col, sin, lit, when, dayofweek, date_format # type:ignore
from pyspark.sql.functions import monotonically_increasing_id # type: ignore
import pandas as pd
import numpy as np

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../Informer2020"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from Informer2020.exp.exp_informer import Informer
# autopep8: on


class TensorDecoder():
    def __init__(self, tensor_encoder: TensorEncoder):
        """
        A Decoder need to know what is encoder
        """
        self.tensor_encoder = tensor_encoder
        self.spark_loader = tensor_encoder.spark_loader
        self.spark = self.spark_loader.spark

        # Logging

        self.logger = logging.getLogger(self.__class__.__name__)

    def tensor_to_raw_sparkDF(self, tensor: torch.Tensor, symbol: str, feature_cols: list, time_cols: list,
                              symbol_last_event_time: dict, symbol_min_max: dict) -> DataFrame:
        """
        Convert prediction tensor to Spark DataFrame with decoded features and reconstructed event_time.
        """
        np_array = tensor.cpu().numpy()
        pdf = pd.DataFrame(np_array, columns=feature_cols + time_cols)

        # Add symbol
        pdf["symbol"] = symbol

        # Decode time to get event_time column
        pdf["event_time"] = self.decode_time_cols(
            pdf[time_cols],
            symbol_last_event_time[symbol],
            len(pdf)
        )

        # Decode feature columns using min-max dict
        pdf[feature_cols] = self.decode_feature_cols(
            pdf[feature_cols],
            symbol_min_max[symbol]
        )

        # Drop sin-cos time columns
        pdf.drop(columns=time_cols, inplace=True)
        # Spark DataFrame
        df = self.spark.createDataFrame(pdf)
        self.spark_loader.write_csv(df, f"{symbol}_decoded_")

        return df

    def decode_time_cols(self, time_df: pd.DataFrame, last_time_str: str, length: int) -> list:
        """
        Generate event_time list counting backward from last_time (1 second each step).
        """
        last_time = pd.to_datetime(last_time_str)
        delta = pd.to_timedelta(range(length)[::-1], unit="s")
        return (last_time - delta).astype(str).tolist()

    def decode_feature_cols(self, feature_df: pd.DataFrame, minmax_dict: dict) -> pd.DataFrame:
        """
        Reverse min-max normalization for each feature using symbol's min-max values.
        """
        for col in feature_df.columns:
            col_min = minmax_dict[col]["min"]
            col_max = minmax_dict[col]["max"]
            feature_df[col] = feature_df[col] * (col_max - col_min) + col_min
        return feature_df
