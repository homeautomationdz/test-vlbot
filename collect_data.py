# file name: collect_data.py

import pandas as pd
import pandas_ta as ta

class GetData:

    columns = ["Date", "Open", "High", "Low", "Close", "Volume", "Ignore", "Quote_Volume", "Trades_Count", "Buy_Vol", "Buy_Vol_Val", "x"]

    def __init__(self, event):
        self.name = event
        self.df = pd.DataFrame(columns=self.columns)
        self.df.set_index("Date", inplace=True)

        # Initialize variables to track the last 4-hour high and low
        self.last_high_4h = None
        self.last_low_4h = None

    def calculate_indicators(self, kline):
        # Create a new DataFrame for the new row
        new_row = pd.Series(kline, index=self.columns)

        # Use pd.concat to add the new row to the DataFrame
        self.df = pd.concat([self.df, new_row.to_frame().T], ignore_index=True)

        # Convert the 'Date' column to datetime and set it as the index
        self.df["Date"] = pd.to_datetime(self.df["Date"], unit="ms")
        self.df.set_index("Date", inplace=True)

        # Convert the 'Close', 'High', 'Low' columns to numeric
        self.df["Close"] = pd.to_numeric(self.df["Close"])
        self.df["High"] = pd.to_numeric(self.df["High"])
        self.df["Low"] = pd.to_numeric(self.df["Low"])

        # Calculate the high and low for the last 4 hours (240 minutes)
        four_hours_ago = self.df.index[-1] - pd.Timedelta(hours=4)
        recent_data = self.df[self.df.index >= four_hours_ago]

        if not recent_data.empty:
            high_4h = recent_data["High"].max()
            low_4h = recent_data["Low"].min()
        else:
            high_4h = None
            low_4h = None

        # Check if the high and low values have changed
        if high_4h != self.last_high_4h or low_4h != self.last_low_4h:
            self.last_high_4h = high_4h
            self.last_low_4h = low_4h

            # Print the new high and low values
            print(f"New 4-hour High: {self.last_high_4h}, New 4-hour Low: {self.last_low_4h}")

        # Calculate buy and sell volume for the last candle
        if not recent_data.empty:
            last_candle_buy_vol = recent_data["Buy_Vol"].iloc[-1] if "Buy_Vol" in recent_data.columns else 0
            last_candle_total_vol = recent_data["Volume"].iloc[-1] if "Volume" in recent_data.columns else 0
            last_candle_sell_vol = last_candle_total_vol - last_candle_buy_vol

            # Calculate the difference between buy and sell volume
            volume_diff = last_candle_buy_vol - last_candle_sell_vol

            # Round the values to 2 decimal places
            last_candle_buy_vol = round(last_candle_buy_vol, 2)
            last_candle_sell_vol = round(last_candle_sell_vol, 2)
            volume_diff = round(volume_diff, 2)

            # Print the results
            print(f"Last Candle Buy Volume: {last_candle_buy_vol}")
            print(f"Last Candle Sell Volume: {last_candle_sell_vol}")
            print(f"Volume Difference: {volume_diff}")

        # Round the DataFrame values to 2 decimal places
        self.df = self.df.round(2)

        # Remove the "x" and "Ignore" columns if they exist
        if "x" in self.df.columns:
            del self.df["x"]
        if "Ignore" in self.df.columns:
            del self.df["Ignore"]

        # Calculate technical indicators
        if "icpusdt" in self.name:
            self.df["sma_10"] = ta.sma(close=self.df["Close"], length=10)
            self.df["rsi_10"] = ta.rsi(close=self.df["Close"], length=10)
        elif "adausdt" in self.name:
            self.df["sma_20"] = ta.sma(close=self.df["Close"], length=20)
            self.df["rsi_20"] = ta.rsi(close=self.df["Close"], length=20)

        # Print the DataFrame after appending new kline data for debugging
        print("DataFrame after appending new kline data:")
        print(self.df.head())

        return self.df
