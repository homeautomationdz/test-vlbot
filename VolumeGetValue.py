import VolumeFields as VF 

resulItem = VF.VolumeAnalysisResultItem

def GetValue(data, item):
    if item == resulItem.AverageBuySize:
        if "ABS" in data.columns:
            return data["ABS"].iloc[-1]
        else:
            return 0  # or some default value
    elif item == resulItem.AverageSellSize:
        if "ASS" in data.columns:
            return data["ASS"].iloc[-1]
        else:
            return 0  # or some default value
    else:
        return None
