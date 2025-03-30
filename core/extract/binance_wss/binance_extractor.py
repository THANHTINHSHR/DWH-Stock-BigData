from core.extract.binance_wss.binance_producer import BinanceProducer
from core.extract.binance_wss.consumer_manager import ConsumerManager


class BinanceExtractor:
    def __init__(self):
        self.producer = BinanceProducer()
        self.consumer_manager = ConsumerManager()

    def extract(self):
        self.consumer_manager.start_consumers()
        self.producer.start_publish()
