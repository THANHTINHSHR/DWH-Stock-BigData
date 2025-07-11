# autopep8: off
import findspark  # type: ignore
findspark.init()

from core.streaming.informerAI.models.tensor_encoder import TensorEncoder  # type: ignore
import logging, os, sys  # type: ignore
import pandas as pd
import torch
from pyspark.sql import DataFrame  # type: ignore
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
        # Ensure shape is 2D: (N * T, D)
        np_array = tensor.cpu().numpy().reshape(-1, len(feature_cols + time_cols))
        pdf = pd.DataFrame(np_array, columns=feature_cols + time_cols)

        # Add symbol column
        pdf["symbol"] = symbol

        # Decode time columns into event_time
        pdf["event_time"] = self.decode_time_cols(
            symbol_last_event_time[symbol], len(pdf))
        # Decode normalized features
        pdf[feature_cols] = self.decode_feature_cols(
            pdf[feature_cols],
            symbol_min_max[symbol]
        )

        # Remove original sin-cos time cols
        pdf.drop(columns=time_cols, inplace=True)

        # Create Spark DataFrame
        df = self.spark.createDataFrame(pdf)
        return df

    def decode_time_cols(self, last_time_str: str, length: int) -> list:
        """
        Generate event_time list counting forward from last_time (1 second each step).
        """
        last_time = pd.to_datetime(last_time_str)
        delta = pd.to_timedelta(range(1, length + 1), unit="s")
        return (last_time + delta).strftime("%Y-%m-%d %H:%M:%S.%f").tolist()

    def decode_feature_cols(self, feature_df: pd.DataFrame, minmax_dict: dict) -> pd.DataFrame:
        """
        Reverse min-max normalization for each feature using symbol's min-max tuple.
        """
        for col in feature_df.columns:
            col_min, col_max = minmax_dict[col]
            feature_df[col] = feature_df[col] * (col_max - col_min) + col_min

        return feature_df
