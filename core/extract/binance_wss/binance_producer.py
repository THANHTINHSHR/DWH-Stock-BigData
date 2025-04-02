from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from core.extract.binance_wss.schema_avro.schema_registry_connector import (
    SchemaRegistryConnector,
)
import io, os
import asyncio, json, websockets, requests
from dotenv import load_dotenv

load_dotenv()


class BinanceProducer:
    def __init__(self):
        # Load environment variables
        self.WSS_ENDPOINT = os.getenv("WSS_ENDPOINT")
        self.URL_TOP = os.getenv("URL_TOP")
        self.LIMIT = int(os.getenv("LIMIT"))
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")
        self.STREAM_TYPES = os.getenv("STREAM_TYPES").split(",")
        self.BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
        self.SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
        print(f"📌 STREAM_TYPES: {self.STREAM_TYPES}")

        # Kafka Producer
        self.producer = AvroProducer(
            {
                "bootstrap.servers": self.BOOTSTRAP_SERVERS,
                "schema.registry.url": self.SCHEMA_REGISTRY_URL,
            }
        )
        # Schema Registry
        self.schema_registry = SchemaRegistryConnector()

    def get_top_coins(self):
        response = requests.get(self.URL_TOP)
        data = response.json()
        top_coins = sorted(data, key=lambda x: float(x["quoteVolume"]), reverse=True)[
            : self.LIMIT
        ]
        return [coin["symbol"].lower() for coin in top_coins]

    def send_message(self, topic, key, message, schema_name):
        """Use AvroSerializer to send message to Kafka"""
        schema = self.schema_registry.get_schema_by_name(schema_name)
        self.producer.produce(
            topic=topic, key=key.encode("utf-8"), value=message, value_schema=schema
        )
        print(f"✅ Sent to topic success")
        self.producer.flush()

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
                        # Sending message to Kafka with schema
                        self.send_message(self.BINANCE_TOPIC, symbol, data, stream_type)
                        print(f"✅ Sent to Kafka: SUCCESS")

            except Exception as e:
                print(f"🔄 Reconnecting WebSocket {url} due to error: {e}")
                return

    async def start_publish(self):
        """Start multiple WebSocket connections concurrently"""
        symbols = self.get_top_coins()
        tasks = []

        for symbol in symbols:
            for stream_type in self.STREAM_TYPES:
                tasks.append(self.fetch_stream(stream_type, symbol))

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    bp = BinanceProducer()
    asyncio.run(bp.start_publish())
