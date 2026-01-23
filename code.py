##
#  This program loads, transforms and processes the data needed
##

##
#  This section imports libraries needed
#
import pandas as pd
import numpy as np

##
#  This section uploads, transforms and cleans the salmon price time series data
#  @dataset Fish Pool Index 4-6 kg Norwegian salmon price
#  
fileName = "Fish_Pool_Data.xls"

xls         = pd.ExcelFile(fileName)
sheetNames  = np.flip(np.array(xls.sheet_names))
datasetList = []

for i in range(0, len(sheetNames)):
    data            = f"data{sheetNames[i]}"
    globals()[data] = pd.read_excel(fileName, sheet_name = sheetNames[i], skiprows= 1)
    datasetList.append(data)

datasets = [globals()[name] for name in datasetList]
dataAll  = pd.concat(datasets, ignore_index = True)


print(dataAll.tail())
