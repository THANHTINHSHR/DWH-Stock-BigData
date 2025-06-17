# autopep8: off
import findspark  # type: ignore
findspark.init()
from core.streaming.informerAI.models.data_processor import DataProcessor  # type: ignore
from core.streaming.informerAI.models.spark_loader import SparkLoader  # type: ignore
import logging, os, sys  # type: ignore
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from pyspark.sql import DataFrame  # type: ignore


current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../Informer2020"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from Informer2020.exp.exp_informer import Informer
# autopep8: on


class TensorEncoder():
    def __init__(self, data_processor: DataProcessor):
        self.data_processor = data_processor
        self.spark_loader = data_processor.spark_loader
        self.spark = self.spark_loader.spark
        self.n_days_ago = self.data_processor.N_DAYS_AGO
        self.max_dir = self.data_processor.MAX_DIRECTORIES
        self.symbol_last_event_time = {}
        self.tensor_ds = {}

        # Loggin
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_raw_df(self):
        df = self.data_processor.get_raw_data
        return df

    def raw_df_to_symbol_dict(self, raw_df: DataFrame):
        symbol_dict = self.data_processor.process_predict(raw_df)
        # Update last_event_time after process
        self.symbol_last_event_time = self.data_processor.symbol_last_event_time
        return symbol_dict

    def symmbol_dict_to_tensor_ds(self, symbol_dict: dict):
        tensor_ds = self.data_processor.convert_dict_spark_data_to_dict_tensor_data(
            symbol_dict)
        # tensor type : model(x_enc, x_mark_enc, dec_inp, x_mark_dec)
        return tensor_ds

    def run(self):
        raw_df = self.get_raw_df()
        symbol_dict = self.raw_df_to_symbol_dict(raw_df)
        tensor_ds = self.symmbol_dict_to_tensor_ds(symbol_dict)
        self.tensor_ds = tensor_ds
        return tensor_ds
