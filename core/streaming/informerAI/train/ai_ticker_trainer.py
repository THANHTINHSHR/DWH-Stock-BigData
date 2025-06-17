# autopep8: off
import findspark  # type: ignore
findspark.init()

from core.streaming.informerAI.train.trainer import Trainer 
from core.streaming.informerAI.features.ai_ticker_data import AITickerData
from core.streaming.informerAI.input.tensor_encoder import TensorEncoder
from core.streaming.informerAI.output.tensor_decoder import TensorDecoder

import logging

# autopep8: on


class AITickerTrainer(Trainer):
    def __init__(self):
        # Name
        self.type = "AITickerTrainer"
        # Data Processor
        self.data_processor = AITickerData()
        # Tensor Encoder
        self.tensor_encoder = TensorEncoder(self.data_processor)
        # Tensor Decoder
        self.tensor_decoder = TensorDecoder(self.tensor_encoder)
        super().__init__(self.data_processor)

    def new_train(self):
        """
        Self Down load data from s3 and learn 
        """
        # self.data_processor.run(self.type)
        # Data test
        # Run data processor to load symbol dict to data_processor
        self.data_processor.run()
        self.symbol_dict = self.data_processor.symbol_dict
        # Load symbol dict to methol trainAll ( ressult will)
        self.trainAll()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    trainer = AITickerTrainer()
    trainer.new_train()
