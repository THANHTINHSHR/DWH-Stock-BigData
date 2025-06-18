# autopep8: off
import findspark  # type: ignore
findspark.init()
from pyspark.sql import DataFrame  # type: ignore

from core.streaming.informerAI.train.trainer import Trainer   # type: ignore
from core.streaming.informerAI.input.tensor_encoder import TensorEncoder  # type: ignore
from core.streaming.informerAI.output.tensor_decoder import TensorDecoder  # type: ignore
from abc import ABC, abstractmethod
import logging, os, sys  # type: ignore
from torch import Tensor

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

        # Models path
        self.save_path = "./core/streaming/informerAI/files/models/"

        # Loggin
        self.logger = logging.getLogger(self.__class__.__name__)\


    def run(self):
        df = self.get_raw_data()
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
        for symbol, torch_in in torch_dict.items():
            self.tensor_dict[symbol] = self.torch_to_sparkDF(
                torch_in, symbol)

        return data_loader_dict

    def get_raw_data(self) -> DataFrame:
        return self.data_processor.get_raw_data(self.type)

    def raw_DF_to_tensorDS(self, df: DataFrame) -> dict:

        dict_tensor_ds = self.tensor_encoder.raw_DF_to_tensorDS(df)
        return dict_tensor_ds

    def tensorDS_to_data_loader(self, tensor_ds: TensorDataset):
        return self.tensor_encoder.get_data_loader_from_tensor_ds(
            tensor_ds)

    def predict(self, data_loader: DataLoader, symbol):
        self.logger.info(f"⏳📌⏳📌  min.max decode : {symbol}")
        for key, (min_val, max_val) in self.symbol_min_max[symbol].items():
            self.logger.info(f"    {key}: min={min_val}, max={max_val}")
        return self.trainer.predict(data_loader, model_path=symbol)

    def torch_to_sparkDF(self, torch: Tensor, symbol: str):
        return self.tensor_decoder.tensor_to_raw_sparkDF(torch, symbol, self.data_processor.feature_cols, self.data_processor.time_cols, self.symbol_last_event_time, self.symbol_min_max)
