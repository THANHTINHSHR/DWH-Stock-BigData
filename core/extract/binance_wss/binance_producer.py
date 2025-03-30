import asyncio
import websockets
import json
from dotenv import load_dotenv
import os
from .producer import Producer

# Load environment variables from .env file
load_dotenv()


class BinanceProducer(Producer):
    def __init__(self):
        # Explicitly call the constructor of each parent class
        Producer.__init__(self)
        self.BINANCE_TOPIC = os.getenv("BINANCE_TOPIC")

    async def start_publish(self):
        # Get the top coins and stream types from environment variables
        top_coins = self.get_top_coins()
        stream_types = self.STREAM_TYPES.split(",")

        # Construct the WebSocket stream URL
        # Merge the two lists into a single list of streams
        streams = [f"{coin}@{st}" for coin in top_coins for st in stream_types]
        url = self.WSS_ENDPOINT + "/stream?streams=" + "/".join(streams)

        print(f"✅ Connecting to {url}")

        while True:
            try:
                # Establish WebSocket connection
                async with websockets.connect(url) as websocket:
                    while True:
                        # Receive message from WebSocket
                        message = await websocket.recv()
                        data = json.loads(message)
                        stream_type = data["stream"].split("@")[1]
                        producer_topic = f"{self.BINANCE_TOPIC}-{stream_type}"  # example: binance-trade
                        # Send received data to Kafka topic
                        self.send_message(
                            producer_topic, json.dumps(data).encode("utf-8")
                        )
            except Exception as e:
                print(f"❌ WebSocket error: {e}")
                # Wait 5 seconds before reconnecting
                await asyncio.sleep(5)


if __name__ == "__main__":
    bl = BinanceProducer()
    print(f"Top coins: {bl.get_top_coins()}")
    asyncio.run(bl.start_publish())
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(bl.start_listen())
