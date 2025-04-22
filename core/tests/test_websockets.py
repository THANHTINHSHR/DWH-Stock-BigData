import asyncio
import websockets


async def test_connection():
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    try:
        async with websockets.connect(url) as ws:
            message = await ws.recv()
            print("Connected! First message:", message)
    except Exception as e:
        print("Error:", e)


asyncio.run(test_connection())
