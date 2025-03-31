import asyncio
import websockets
import json
import os
import requests
import fastavro
import io
from fastavro.schema import load_schema
from confluent_kafka import Producer
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
        print(f"📌 STREAM_TYPES: {self.STREAM_TYPES}")

        # Kafka Producer
        self.producer = Producer(
            {
                "bootstrap.servers": self.BOOTSTRAP_SERVERS,
            }
        )
        # Load Avro Schema
        self.stream_schema = self.load_stream_schema()

    def get_top_coins(self):
        """Fetch the top coins based on trading volume"""
        response = requests.get(self.URL_TOP)
        data = response.json()
        top_coins = sorted(data, key=lambda x: float(x["quoteVolume"]), reverse=True)[
            : self.LIMIT
        ]
        return [coin["symbol"].lower() for coin in top_coins]

    def load_stream_schema(self):
        stream_schema = {}
        base_dir = os.path.dirname(os.path.abspath(__file__))
        for stream_type in self.STREAM_TYPES:
            # Create path to schema
            path_scm = os.path.join(base_dir, "schema_avro", f"{stream_type}.avsc")
            schema = load_schema(path_scm)
            stream_schema[stream_type] = schema
        return stream_schema

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
                        # self.send_message(self.BINANCE_TOPIC, symbol, data, stream_type)

            except Exception as e:
                print(f"🔄 Reconnecting WebSocket {url} due to error: {e}")
                # await asyncio.sleep(5)
                return

    def send_message(self, topic, key, message, schema_name):
        """Serialize the message using Avro and send it to Kafka"""
        bytes_writer = io.BytesIO()
        fastavro.writer(bytes_writer, self.stream_schema[schema_name], [message])
        avro_bytes = bytes_writer.getvalue()
        self.producer.produce(topic=topic, key=key.encode("utf-8"), value=avro_bytes)

    async def start_publish(self):
        """Start multiple WebSocket connections concurrently"""
        symbols = self.get_top_coins()
        tasks = []

        for symbol in symbols:
            for stream_type in self.STREAM_TYPES:
                tasks.append(self.fetch_stream(stream_type, symbol))

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    producer = BinanceProducer()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(producer.start_publish())
