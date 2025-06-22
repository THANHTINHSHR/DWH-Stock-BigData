from core.streaming.kafka.topic_creator import TopicCreator
from confluent_kafka import Producer

# import io, requests, json # Unused imports
import os
import asyncio
import websockets
import logging
from dotenv import load_dotenv

load_dotenv()


class ProducerManager:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        # Load environment variables
        self.WSS_ENDPOINT = os.getenv("WSS_ENDPOINT")
        self.URL_TOP = os.getenv("URL_TOP")
        self.LIMIT = int(os.getenv("LIMIT"))
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
        # List of producers base on coin
        self.producers = {}
        if not TopicCreator.TOPCOIN:
            self.logger.warning(
                "TOPCOIN list is empty. ProducerManager might not function as expected."
            )
        else:
            self.load_top_coins_list()
        self.logger.info(f"📌 STREAM_TYPES: {self.STREAM_TYPES}")
        self.logger.info(f"TOPCOIN from TopicCreator: {TopicCreator.TOPCOIN}")

    def load_top_coins_list(self):
        for symbol in TopicCreator.TOPCOIN:
            self.get_producer(symbol)

    def get_producer(self, symbol):
        # Create producer base on symbol or return producer if exist
        if symbol not in self.producers:
            conf = {
                "bootstrap.servers": self.BOOTSTRAP_SERVERS,
            }
            self.producers[symbol] = Producer(conf)
            self.logger.info(f"✅ Producer created for symbol: {symbol}")
        return self.producers[symbol]

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(
                f"❌ Delivery failed for {msg.topic()} [{msg.partition()}]: {err}"
            )

    def send_message(self, producer: Producer, topic, key, message):
        try:
            producer.produce(
                topic=topic, key=str(key), value=message, callback=self.delivery_report
            )
            producer.poll(0)
            self.logger.debug(
                f"✅ Message produced to topic: {topic}, key: {key}"
            )  # DEBUG level might be more appropriate
        except Exception as e:
            self.logger.error(
                f"❌ Error producing message to topic {topic} with key {key}: {e}"
            )

    async def fetch_stream(self, stream_type, symbol):
        url = f"{self.WSS_ENDPOINT}/{symbol}@{stream_type}"
        while True:
            try:
                async with websockets.connect(url) as ws:
                    self.logger.info(f"📡 Connected to {url}")
                    while True:
                        message = await ws.recv()
                        # Process the message
                        producer = self.get_producer(symbol=symbol)
                        self.send_message(
                            producer,
                            f"{self.BINANCE_TOPIC}_{stream_type}",
                            symbol,
                            message.encode(
                                "utf-8") if isinstance(message, str) else message,
                        )
                        # limit One message per second
                        await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(
                    f"🔄 WebSocket error for {url}: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

    async def start_publish(self):
        """Start multiple WebSocket connections concurrently"""
        tasks = []
        for symbol in TopicCreator.TOPCOIN:
            for stream_type in self.STREAM_TYPES:
                self.logger.info(
                    f"📡 Preparing to start WebSocket stream for {symbol}@{stream_type}"
                )
                tasks.append(self.fetch_stream(stream_type, symbol))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    tc = TopicCreator()
    producer_manager = ProducerManager()
    # Start the publish process
    asyncio.run(producer_manager.start_publish())
