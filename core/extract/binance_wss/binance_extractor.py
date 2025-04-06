from core.extract.binance_wss.producer_manager import ProducerManager
from core.extract.binance_wss.consumer_manager import ConsumerManager
from core.extract.binance_wss.topic_creator import TopicCreator
import asyncio, os
from dotenv import load_dotenv

load_dotenv()


class BinanceExtractor:
    def __init__(self):
        # Initialize the manager objects for producer, consumer, and topic creation
        self.topic_creator = TopicCreator()
        self.topic_creator.create_topic()
        self.producer_manager = ProducerManager()
        self.consumer_manager = ConsumerManager()

    async def extract(self):
        """Main method to run the extractor"""
        # Run the consumer and producer tasks concurrently using asyncio.gather
        await asyncio.gather(
            self.consumer_manager.start_listen(), self.producer_manager.start_publish()
        )


if __name__ == "__main__":
    be = BinanceExtractor()
    # Run the main async method using asyncio.run
    asyncio.run(be.extract())
