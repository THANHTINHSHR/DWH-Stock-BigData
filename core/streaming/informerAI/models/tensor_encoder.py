# autopep8: off
import findspark  # type: ignore
findspark.init()
from core.streaming.informerAI.models.data_processor import DataProcessor  # type: ignore
import logging, os, sys  # type: ignore
from torch.utils.data import TensorDataset,DataLoader
from pyspark.sql import DataFrame  # type: ignore
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../Informer2020")) # type: ignore
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# autopep8: on


class TensorEncoder():
    def __init__(self, data_processor: DataProcessor):
        self.data_processor = data_processor
        self.spark_loader = data_processor.spark_loader
        self.symbol_last_event_time = {}
        self.tensor_ds = {}

        # Loggin
        self.logger = logging.getLogger(self.__class__.__name__)

    # [btcusdt:tensorDataset]
    def raw_DF_to_tensorDS(self, df: DataFrame) -> dict:
        # {[btcusdt:tensorDataset1],[buucusdt:tensorDataset2],...}
        return self.data_processor.process_predict(df)

    def get_data_loader_from_tensor_ds(self, tensor_ds: TensorDataset):
        data_loader = DataLoader(
            tensor_ds, batch_size=self.data_processor.BATCH_SIZE, shuffle=True)
        return data_loader
