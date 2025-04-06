from core.extract.binance_wss.producer_manager import ProducerManager
from core.extract.binance_wss.consumer_manager import ConsumerManager
from core.extract.binance_wss.topic_creator import TopicCreator
import asyncio


class BinanceExtractor:
    def __init__(self):
        # Initialize the manager objects for producer, consumer, and topic creation
        self.producer_manager = ProducerManager()
        self.consumer_manager = ConsumerManager()
        self.topic_creator = TopicCreator()

    async def extract(self):
        # Create the topic asynchronously (assuming the method can be async)
        self.topic_creator.create_topic()
        # Run the consumer and producer tasks concurrently using asyncio.gather
        await asyncio.gather(
            self.consumer_manager.start_listen(), self.producer_manager.start_publish()
        )


if __name__ == "__main__":
    be = BinanceExtractor()
    # Run the main async method using asyncio.run
    asyncio.run(be.extract())
