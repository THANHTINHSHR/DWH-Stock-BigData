from core.streaming.kafka.topic_creator import TopicCreator
from confluent_kafka import Producer
import io, os, requests, asyncio, json, websockets
from dotenv import load_dotenv

load_dotenv()


class ProducerManager:
    def __init__(self):

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
        self.load_top_coins_list()
        print(f"📌 STREAM_TYPES: {self.STREAM_TYPES}")
        print(f"TOPCOIN: {TopicCreator.TOPCOIN}")

    def load_top_coins_list(self):
        for symbol in TopicCreator.TOPCOIN:
            self.get_producer(symbol)
            print("✅ Producer created")

    def get_producer(self, symbol):
        # Create producer base on symbol or return producer if exist
        if symbol not in self.producers:
            conf = {
                "bootstrap.servers": self.BOOTSTRAP_SERVERS,
            }
            self.producers[symbol] = Producer(conf)
        return self.producers[symbol]

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"❌ Delivery failed: {err}")
        else:
            print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

    def send_message(self, producer: Producer, topic, key, message):

        try:
            producer.produce(
                topic=topic, key=str(key), value=message, callback=self.delivery_report
            )
            producer.poll(0)
            print(f"✅ Message produced to topic: {topic}")
        except Exception as e:
            print(f"❌ Error producing message: {e}")

    async def fetch_stream(self, stream_type, symbol):
        """Open WebSocket connection and handle reconnection"""
        url = f"{self.WSS_ENDPOINT}/{symbol}@{stream_type}"
        while True:
            try:
                async with websockets.connect(url) as ws:
                    print(f"📡 Establish wss streaming")
                    while True:
                        message = await ws.recv()
                        product = self.get_producer(symbol=symbol)
                        # Sending message to Kafka
                        self.send_message(
                            product,
                            f"{self.BINANCE_TOPIC}_{stream_type}",
                            symbol,
                            message,
                        )
            except Exception as e:
                print(f"🔄 Reconnecting WebSocket {url} due to error: {e}")
                await asyncio.sleep(3)

    async def start_publish(self):
        """Start multiple WebSocket connections concurrently"""
        tasks = []
        for symbol in TopicCreator.TOPCOIN:
            for stream_type in self.STREAM_TYPES:
                print(f"📡 starting WSS")
                tasks.append(self.fetch_stream(stream_type, symbol))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    tc = TopicCreator()
    producer_manager = ProducerManager()
    # Start the publish process
    asyncio.run(producer_manager.start_publish())
