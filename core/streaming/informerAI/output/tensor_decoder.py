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
from pyspark.sql import SparkSession, DataFrame  # type: ignore
from datetime import timedelta
from pyspark.sql.types import TimestampType # type: ignore
from pyspark.sql import functions as F # type: ignore


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
        self.target_columns = self.tensor_encoder.data_processor.final_cols
        self.symbol_last_event_time = self.tensor_encoder.data_processor.symbol_last_event_time

        # Logging

        self.logger = logging.getLogger(self.__class__.__name__)

    def tensor_to_raw_sparkDF(self, tensor: torch.Tensor, symbol: str) -> DataFrame:
        pass

    def write_to_s3(self, df: DataFrame, symbol: str):
        self.spark_loader.write_csv(df, symbol)
