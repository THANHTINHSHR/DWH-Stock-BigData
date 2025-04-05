from core.extract.binance_wss.producer_factory import ProducerFactory
from confluent_kafka import SerializingProducer

import io, os, requests, asyncio, json, websockets
from dotenv import load_dotenv

load_dotenv()


class ProducerManager:
    def __init__(self):
        self._producers = {}
        # Load environment variables
        self.WSS_ENDPOINT = os.getenv("WSS_ENDPOINT")
        self.URL_TOP = os.getenv("URL_TOP")
        self.LIMIT = int(os.getenv("LIMIT"))
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
        print(f"📌 STREAM_TYPES: {self.STREAM_TYPES}")

    def get_top_coins(self):
        response = requests.get(self.URL_TOP)
        data = response.json()
        top_coins = sorted(data, key=lambda x: float(x["quoteVolume"]), reverse=True)[
            : self.LIMIT
        ]
        return [coin["symbol"].lower() for coin in top_coins]

    def get_producer(self, stream_type):
        if stream_type not in self._producers:
            self._producers[stream_type] = ProducerFactory().create_producer(
                stream_type
            )
            print("✅ Producer created")

        return self._producers[stream_type]

    def send_message(self, producer: SerializingProducer, topic, key, message):
        try:
            print(f"✅ Sending message to Kafka: {message}")
            producer.produce(
                topic=topic,
                key=key,
                value=message,
                on_delivery=self.delivery_report,
            )
            print(f"✅ Message produced to topic: {topic}")
        except Exception as e:
            print(f"❌ Error producing message: {e}")

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print(f"❌ Delivery failed: {err}")
        else:
            print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

    async def fetch_stream(self, stream_type, symbol):
        """Open WebSocket connection and handle reconnection"""
        url = f"{self.WSS_ENDPOINT}/{symbol}@{stream_type}"
        while True:
            try:
                async with websockets.connect(url) as ws:
                    print(f"📡 Establish wss streaming")
                    while True:
                        message = await ws.recv()
                        data = json.loads(message)
                        print(f"✅ Received: {data}")
                        product = self.get_producer(stream_type)
                        # Sending message to Kafka with schema
                        self.send_message(product, self.BINANCE_TOPIC, symbol, data)

            except Exception as e:
                print(f"🔄 Reconnecting WebSocket {url} due to error: {e}")
                await asyncio.sleep(3)

    async def start_publish(self):
        """Start multiple WebSocket connections concurrently"""
        symbols = self.get_top_coins()
        tasks = []
        for symbol in symbols:
            for stream_type in self.STREAM_TYPES:
                tasks.append(self.fetch_stream(stream_type, symbol))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    producer_manager = ProducerManager()
    # Start the publish process
    asyncio.run(producer_manager.start_publish())
