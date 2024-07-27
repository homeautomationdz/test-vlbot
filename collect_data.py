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

    def calculate_indicators(self, kline, avg_buy_size, avg_sell_size):
        # Create a new DataFrame for the new row
        new_row = pd.Series(kline, index=self.columns)

        # Use pd.concat to add the new row to the DataFrame
        self.df = pd.concat([self.df, new_row.to_frame().T], ignore_index=True)

        # Convert the 'Date' column to datetime and set it as the index
        self.df["Date"] = pd.to_datetime(self.df["Date"], unit="ms")
        self.df.set_index("Date", inplace=True)

        # Convert the 'Close', 'High', 'Low', 'ABS', and 'ASS' columns to numeric
        self.df["Close"] = pd.to_numeric(self.df["Close"])
        self.df["High"] = pd.to_numeric(self.df["High"])
        self.df["Low"] = pd.to_numeric(self.df["Low"])
        self.df["ABS"] = pd.to_numeric(avg_buy_size)  # Assuming avg_buy_size is a single value
        self.df["ASS"] = pd.to_numeric(avg_sell_size)  # Assuming avg_sell_size is a single value

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

            # Check for "big wick" condition based on max high or min low
            last_candle_open = recent_data["Open"].iloc[-1]
            last_candle_close = recent_data["Close"].iloc[-1]
            body_size = abs(last_candle_open - last_candle_close)

            # If the last candle is at a maximum high
            if last_candle_close == high_4h:
                wick_size = last_candle_close - high_4h
                if wick_size > body_size:  # Check for big wick at the top
                    print("kahf (big wick at top)")

            # If the last candle is at a minimum low
            elif last_candle_close == low_4h:
                wick_size = low_4h - last_candle_open if last_candle_open < low_4h else low_4h - last_candle_close
                if wick_size > body_size:  # Check for big wick at the bottom
                    print("kahf (big wick at bottom)")

        # Calculate standard deviation and volatility indicators
        self.df["std"] = self.df["ABS"].rolling(window=99).std()
        self.df["std_s"] = self.df["ASS"].rolling(window=99).std()
        self.df["vi"] = self.df["ABS"] / self.df["std"]
        self.df["vi_ass"] = self.df["ASS"] / self.df["std_s"]

        # Round the values to 2 decimal places
        self.df = self.df.round(2)

        # Remove the "x" and "Ignore" columns
        del self.df["x"]
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
