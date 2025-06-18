
# autopep8: off
import findspark  # type: ignore
findspark.init()
from core.streaming.informerAI.train.trainer import Trainer
from core.streaming.informerAI.models.tensor_decoder import TensorDecoder
from core.streaming.informerAI.models.tensor_encoder import TensorEncoder
from core.streaming.informerAI.models.ai_ticker_data import AITickerData  # type: ignore
from core.streaming.informerAI.predict.predictor import Predictor  # type: ignore
import logging


# autopep8: on


class AITickerPredictor(Predictor):
    def __init__(self, trainer, tensor_decoder):
        super().__init__("ticker", trainer, tensor_decoder)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    ai_ticker_data = AITickerData()
    trainer = Trainer(ai_ticker_data)
    tensor_encoder = TensorEncoder(ai_ticker_data)
    tensor_decoder = TensorDecoder(tensor_encoder)
    predictor = AITickerPredictor(trainer, tensor_decoder)
    predictor.run()
