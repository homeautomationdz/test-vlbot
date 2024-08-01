import VolumeFields as VF

resulItem = VF.VolumeAnalysisResultItem

def GetValue(data, item):
    if item == resulItem.AverageBuySize:
        return data["Buy_Vol"].iloc[-1] if "Buy_Vol" in data.columns and not data.empty else 0
    elif item == resulItem.AverageSellSize:
        return data["Sell_Vol"].iloc[-1] if "Sell_Vol" in data.columns and not data.empty else 0
    else:
        return None
