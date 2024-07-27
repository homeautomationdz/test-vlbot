import pandas as pd
import pandas_ta as ta

class GetData:
    columns = ["Date", "Open", "High", "Low", "Close", "Volume", "Ignore", "Quote_Volume", "Trades_Count", "Buy_Vol", "Buy_Vol_Val", "x"]
    
    def __init__(self, event):
        self.name = event 
        self.df = pd.DataFrame(columns=self.columns)
        self.df.set_index("Date", inplace=True)

    def calculate_indicators(self, kline, avg_buy_size, avg_sell_size):
        # Create a new DataFrame for the new row
        new_row = pd.Series(kline, index=self.columns)

        # Use pd.concat to add the new row to the DataFrame
        self.df = pd.concat([self.df, new_row.to_frame().T], ignore_index=True)

        # Convert the 'Date' column to datetime and set it as the index
        self.df["Date"] = pd.to_datetime(self.df["Date"], unit="ms")
        self.df.set_index("Date", inplace=True)

        # Calculate technical indicators
        if "icpusdt" in self.name:
            self.df["sma_10"] = ta.sma(close=self.df["Close"], length=10)
            self.df["rsi_10"] = ta.rsi(close=self.df["Close"], length=10)
        elif "adausdt" in self.name:
            self.df["sma_20"] = ta.sma(close=self.df["Close"], length=20)
            self.df["rsi_20"] = ta.rsi(close=self.df["Close"], length=20)

        # Add average buy and sell sizes
        self.df["ABS"] = avg_buy_size  # Average Buy Size
        self.df["ASS"] = avg_sell_size  # Average Sell Size

        # Print the DataFrame after appending new kline data for debugging
        print("DataFrame after appending new kline data:")
        print(self.df.head())

        return self.df
