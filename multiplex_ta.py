import asyncio
from binance import AsyncClient, BinanceSocketManager
import collect_data as cd
import VolumeGetValue as VGV
import VolumeFields as VF
from config import streams  # Import streams from config.py

# Initialize the Binance asynchronous client
async def create_client():
    return await AsyncClient.create()

# Create instances of GetData for each stream
data_objs = {stream: cd.GetData(stream) for stream in streams}

def filter_data(kline):
    # Extract relevant fields from kline data
    data = [
        kline["t"],  # Timestamp
        kline["o"],  # Open
        kline["h"],  # High
        kline["l"],  # Low
        kline["c"],  # Close
        kline["v"],  # Volume
        kline["T"],  # Close time
        kline["q"],  # Quote asset volume
        kline["n"],  # Number of trades
        kline["V"],  # Taker buy base asset volume
        kline["Q"],  # Taker buy quote asset volume
        kline["B"],  # Ignore
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
    # Create a BinanceSocketManager instance
    bsm = BinanceSocketManager(client)
    async with bsm.multiplex_socket(streams) as stream:
        while True:
            response = await stream.recv()
            # Schedule the analyse_data function to run
            asyncio.create_task(analyse_data(response))

async def main():
    # Create the Binance client and start listening for kline data
    client = await create_client()
    await kline_listen(client)

# Create and run the event loop
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(main())
