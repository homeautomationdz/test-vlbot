# file name: multiplex_ta.py

import asyncio
from binance import AsyncClient, BinanceSocketManager
import collect_data as cd
import VolumeGetValue as VGV
import VolumeFields as VF
from config import streams

async def create_client():
    return await AsyncClient.create()

data_objs = {stream: cd.GetData(stream) for stream in streams}

def filter_data(kline):
    data = [
        kline["t"],
        float(kline["o"]),
        float(kline["h"]),
        float(kline["l"]),
        float(kline["c"]),
        float(kline["v"]),
        kline["T"],
        round(float(kline["q"]), 2),
        round(float(kline["n"]), 2),
        round(float(kline["V"]), 2),
        round(float(kline["Q"]), 2),
        kline["B"],
    ]
    return data

async def analyse_data(data):
    # Check if the kline is closed
    if data["data"]["k"]["x"] == True:
        kline = filter_data(data["data"]["k"])
        stream = data["stream"]

        # Use the pre-created GetData instance for the stream
        data_obj = data_objs[stream]

        # Calculate average buy and sell sizes
        avg_buy_size = VGV.GetValue(data_obj.df, VF.VolumeAnalysisResultItem.AverageBuySize)
        avg_sell_size = VGV.GetValue(data_obj.df, VF.VolumeAnalysisResultItem.AverageSellSize)

        # Print the stream name for debugging
        print(f"Processing stream: {data_obj.name}")

        # Update the DataFrame with the new kline data
        df = data_obj.calculate_indicators(kline, avg_buy_size, avg_sell_size)

        # Print the updated DataFrame for debugging
        print("Updated DataFrame:")
        print(df)
async def kline_listen(client):
    bsm = BinanceSocketManager(client)
    async with bsm.multiplex_socket(streams) as stream:
        while True:
            response = await stream.recv()
            asyncio.create_task(analyse_data(response))

async def main():
    client = await create_client()
    await kline_listen(client)

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(main())
