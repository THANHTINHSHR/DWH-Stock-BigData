from confluent_kafka import Consumer
import os
from dotenv import load_dotenv

load_dotenv()


class ConsumerFactory:
    def __init__(self):
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")

    def create_consumer(self, stream_type):
        """Create a Kafka consumer for the given stream_type"""
        config = {
            "bootstrap.servers": self.BOOTSTRAP_SERVERS,
            "group.id": stream_type,
            "auto.offset.reset": "earliest",  # Change to latest if needed
        }
        consumer = Consumer(config)
        consumer.subscribe([f"{self.BINANCE_TOPIC}_{stream_type}"])
        return consumer
