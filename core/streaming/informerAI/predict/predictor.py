# autopep8: off
import findspark  # type: ignore
findspark.init()
from pyspark.sql import DataFrame  # type: ignore

from core.streaming.informerAI.train.trainer import Trainer   # type: ignore
from core.streaming.informerAI.models.tensor_decoder import TensorDecoder  # type: ignore
from core.streaming.informerAI.models.tensor_decoder import TensorDecoder  # type: ignore
from abc import ABC
import logging, os, sys  # type: ignore
from torch import Tensor
from functools import reduce
from core.streaming.informerAI.outer.informer_outer import InformerOuter 

from torch.utils.data import DataLoader, TensorDataset
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../../Informer2020"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# autopep8: on


class Predictor(ABC):
    """
    Abstract base class for predictors.
    """

    def __init__(self, type: str, trainer: Trainer, tensor_decoder: TensorDecoder):
        # Type
        self.type = type

        # Class
        self.trainer = trainer
        self.tensor_decoder = tensor_decoder
        self.tensor_encoder = self.tensor_decoder.tensor_encoder
        self.data_processor = self.tensor_encoder.data_processor
        self.spark_loader = self.data_processor.spark_loader
        self.spark = self.spark_loader.spark

        # Dict
        self.symbol_dict = self.data_processor.symbol_dict
        self.symbol_last_event_time = self.data_processor.symbol_last_event_time
        self.symbol_min_max = self.data_processor.symbol_min_max
        self.symbol_labels = self.data_processor.symbol_labels
        self.tensor_dict = {}

        # ENV
        self.n_days_ago = self.data_processor.N_DAYS_AGO
        self.max_dir = self.data_processor.MAX_DIRECTORIES

        # Outer
        self.outer = InformerOuter()
        # Run outer creator
        self.outer.run_creator()

        # Loggin
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self) -> dict[str, DataFrame]:
        df = self.get_current_data()
        tensor_ds_dict = self.raw_DF_to_tensorDS(df)
        data_loader_dict = {}
        # TensorDataset to DataLoader
        for symbol, tensor_ds_in in tensor_ds_dict.items():
            data_loader_dict[symbol] = self.tensorDS_to_data_loader(
                tensor_ds_in)
        # Data Loader -> Use Predict -> Tensor
        torch_dict = {}
        for symbol, data_loader_in in data_loader_dict.items():
            torch_dict[symbol] = self.predict(data_loader_in, symbol)
        # Tensor -> Spark DF
        spark_df_dict = {}
        for symbol, torch_in in torch_dict.items():
            spark_df_dict[symbol] = self.torch_to_sparkDF(
                torch_in, symbol)
        # Spark DF -> S3
        self.upload_to_s3(spark_df_dict)
        # Spark DF -> InfluxDB
        self.upload_to_InfluxDB(spark_df_dict)

        return spark_df_dict

    def get_current_data(self) -> DataFrame:
        self.logger.info(
            f"⏳Processing get current data with type: {self.type} ...")
        return self.data_processor.get_current_data(self.type)

    def raw_DF_to_tensorDS(self, df: DataFrame) -> dict:
        self.logger.info(
            f"⏳⏳Processing convert raw data to tensor with type: {self.type} ...")

        dict_tensor_ds = self.tensor_encoder.raw_DF_to_tensorDS(df)
        return dict_tensor_ds

    def tensorDS_to_data_loader(self, tensor_ds: TensorDataset):
        self.logger.info(
            f"⏳Processing convert tensor dataset to dataloader with type: {self.type} ...")
        return self.tensor_encoder.get_data_loader_from_tensor_ds(
            tensor_ds)

    def predict(self, data_loader: DataLoader, symbol):
        self.logger.info(
            f"⏳Processing predict data with type: {self.type} ...")
        return self.trainer.predict(data_loader, model_path=symbol)

    def torch_to_sparkDF(self, torch: Tensor, symbol: str):
        self.logger.info(
            f"⏳Processing transfrom torch to spark dataframe data with type: {self.type} ...")
        return self.tensor_decoder.tensor_to_raw_sparkDF(torch, symbol, self.data_processor.feature_cols, self.data_processor.time_cols, self.symbol_last_event_time, self.symbol_min_max)

    def upload_to_s3(self, spark_df_dict: dict):
        self.logger.info(

            f"⏳Processing Upload predicted data to s3 with type: {self.type} ...")
        # Join Dataframe
        df = reduce(
            lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
            spark_df_dict.values()
        )
        self.spark_loader.upload_predict_data(self.type, df)

    def upload_to_InfluxDB(self, spark_df_dict: dict):
        self.logger.info(
            f"⏳Processing Upload predicted data to InfluxDB with type: {self.type} ...")
        # Join Dataframe
        df = reduce(
            lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
            spark_df_dict.values()
        )
        self.outer.send_predict_data(self.type, df)
