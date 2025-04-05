from core.extract.binance_wss.binance_producer import BinanceProducer
from core.extract.binance_wss.consumer_manager import ConsumerManager
from core.extract.binance_wss.topic_creator import TopicCreator


class BinanceExtractor:
    def __init__(self):
        self.producer = BinanceProducer()
        self.consumer_manager = ConsumerManager()
        self.topic_creator = TopicCreator()

    def extract(self):
        self.topic_creator.create_topic()
        self.consumer_manager.start_consumers()
        self.producer.start_publish()
