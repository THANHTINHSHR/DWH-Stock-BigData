import asyncio
import websockets
import json
import requests

BINANCE_WSS = "wss://stream.binance.com:9443/stream?streams="
STREAM_TYPES = ["trade", "ticker", "depth", "markPrice"]


def get_top_coins(limit=10):

    url = "https://api.binance.com/api/v3/ticker/24hr"
    response = requests.get(url)
    data = response.json()

    top_coins = sorted(data, key=lambda x: float(x["quoteVolume"]), reverse=True)[
        :limit
    ]
    return [coin["symbol"].lower() for coin in top_coins]


async def listen_binance():
    top_coins = get_top_coins()

    # Create streams for top 10 coins
    streams = [f"{coin}@{stream}" for coin in top_coins for stream in STREAM_TYPES]
    url = BINANCE_WSS + "/".join(streams)  # Create URL WebSocket

    async with websockets.connect(url) as websocket:
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            # print(data)


if __name__ == "__main__":
    # asyncio.run(listen_binance())
    top_coins = get_top_coins()
    print(top_coins)
