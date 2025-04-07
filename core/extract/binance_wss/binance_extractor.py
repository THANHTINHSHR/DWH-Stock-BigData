import threading
import asyncio
from core.extract.binance_wss.producer_manager import ProducerManager
from core.extract.binance_wss.consumer_manager import ConsumerManager
from core.extract.binance_wss.topic_creator import TopicCreator
import os
from dotenv import load_dotenv

load_dotenv()


class BinanceExtractor:
    def __init__(self):
        # Initialize the manager objects for producer, consumer, and topic creation
        self.topic_creator = TopicCreator()
        self.topic_creator.create_topic()
        self.producer_manager = ProducerManager()
        self.consumer_manager = ConsumerManager()

    def start(self):
        """Start the threads for producer and consumer."""
        # Create and start the consumer thread
        consumer_thread = threading.Thread(target=self.consumer_manager.start_listen)
        consumer_thread.start()

        # Create and start the producer thread with asyncio
        producer_thread = threading.Thread(target=self.run_producer)
        producer_thread.start()

        # Wait for the threads to finish
        consumer_thread.join()
        producer_thread.join()

    def run_producer(self):
        """Run the producer async task in a new event loop."""
        loop = asyncio.new_event_loop()  # Create a new event loop for this thread
        asyncio.set_event_loop(loop)  # Set it as the current event loop
        loop.run_until_complete(
            self.producer_manager.start_publish()
        )  # Run the async task


if __name__ == "__main__":
    # Create an instance of BinanceExtractor
    be = BinanceExtractor()
    # Start the extractor (producer and consumer) in separate threads
    be.start()
