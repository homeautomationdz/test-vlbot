# file name: VolumeGetValue.py

import VolumeFields as VF

resulItem = VF.VolumeAnalysisResultItem

def GetValue(data, item):
    if data.empty:
        return 0  # Return 0 if the DataFrame is empty

    if item == resulItem.AverageBuySize:
        return data["Buy_Vol"].iloc[-1] if "Buy_Vol" in data.columns else 0
    elif item == resulItem.AverageSellSize:
        return data["Sell_Vol"].iloc[-1] if "Sell_Vol" in data.columns else 0
    else:
        return None
