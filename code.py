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
#  From 1st week of january 2006 to 3rd week of january 2026
#  @dataset Fish Pool Index 3-6 kg Norwegian salmon price
#  
def loadFishPoolData(fileName):

    xls         = pd.ExcelFile(fileName)
    sheetNames  = np.flip(np.array(xls.sheet_names))
    datasetList = []

    for i in range(0, len(sheetNames)):
        data            = f"data{sheetNames[i]}"
        globals()[data] = pd.read_excel(fileName, sheet_name = sheetNames[i], skiprows= 1)
        datasetList.append(data)

    datasets = [globals()[name] for name in datasetList]
    dataAll  = pd.concat(datasets, ignore_index = True)

    return dataAll

fileName     = "Fish_Pool_Data.xls"
dataFishPool = loadFishPoolData(fileName)

##
#  This section uploads, transforms and cleans the salmon price time series data
#  From January 1932 to December 2025
#  @dataset SSB Norwegian CPI
#
fileName = "Consumer_Price_Index_Data.xlsx"

data          = pd.read_excel(fileName)
data          = data[:-2]
data          = data.iloc[::-1]

dataY         = data.iloc[:,:2]
dataY.columns = ["Year", "CPI_Annual"]

dataM         = data.drop(columns = "Årsgj.snitt2")
dataM.rename(columns = {dataM.columns[0]: "Year"}, inplace = True) 

years  = dataM["Year"].values.tolist()
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

nyears  = len(years)
nmonths = len(months)

dates  = []
values = []

for i in years:
    for j in months:
        dates.append(f"{i}-{j}-01")

print(values)
