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
#  @return weekly salmon price per kg, in NOK and EUR
#  
def loadFishPoolData(fileName):

    xls         = pd.ExcelFile(fileName)
    sheetNames  = np.flip(np.array(xls.sheet_names))
    datasetList = []

    for i in range(0, len(sheetNames)):
        data            = f"data{sheetNames[i]}"
        globals()[data] = pd.read_excel(fileName, sheet_name = sheetNames[i], skiprows= 1)
        datasetList.append(data)

    datasets           = [globals()[name] for name in datasetList]
    dataClean          = pd.concat(datasets, ignore_index = True)
    dataClean["Month"] = pd.to_datetime(dataClean["Month"], format = "%B").dt.month

    return dataClean

fileName     = "Fish_Pool_Data.xls"
dataFishPool = loadFishPoolData(fileName)

##
#  This section uploads, transforms and cleans the CPI time series data
#  From January 1932 to December 2025
#  @dataset SSB Norwegian CPI
#  @return Monthly or Annual CPI in percentage (%)
#
def loadCPIData(fileName, frequency):

    data          = pd.read_excel(fileName)
    data          = data[:-2]
    data          = data.iloc[::-1]

    if frequency == "Annual":

        dataClean         = data.iloc[:,:2]
        dataClean.columns = ["Year", "CPI_Annual"]
        dataClean         = dataClean.reset_index(drop = True)

    elif frequency == "Monthly":

        dataM         = data.drop(columns = "Årsgj.snitt2")
        months        = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
        dataM.columns = ["Year"] + months

        years  = dataM["Year"].values.tolist()

        nyears  = len(years)
        nmonths = len(months)

        dates  = []
        for i in years:
            for j in months:
                dates.append(f"{i}-{j}-01")

        values = dataM[months].to_numpy().ravel().tolist()

        monthlyData = {"Date"       : dates,
                       "CPI_Monthly": values}

        dataClean         = pd.DataFrame(monthlyData)
        dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")

    else:

        dataClean = "Select a valid frequency: Monthly or annual"

    return dataClean

fileName = "Consumer_Price_Index_Data.xlsx"
dataCPI  = loadCPIData(fileName, frequency="Annual")

##
#  This section uploads, transforms and cleans the EURNOK time series data
#  From 24 January 2000 to 22 January 2026
#  @dataset Norges Bank EURNOK spot price
#  @return daily EURNOK spot price
#
def loadEURNOKData(fileName):

    data              = pd.read_excel(fileName, skiprows= 21, header= None)
    dataClean         = data.T
    dataClean.columns = ["Date", "EURNOK_Daily"]
    dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")

    return dataClean

fileName   = "EURNOK_Data.xlsx"
dataEURNOK = loadEURNOKData(fileName)

##
#  This section uploads, transforms and cleans the Salmon Export Price time series data
#  From week 1 January 2000 to week 3 January 2026
#  @dataset SSB exported salmon tons and price 
#  @return weekly exported salmon tons and price per kilogram in NOK
#
def loadSSBPriceData(fileName):

    data         = pd.read_excel(fileName, header = None)
    data         = data.loc[3:,1:]
    data         = data.loc[:data.dropna(how = "all").index[-1]]
    data.columns = ["Date", "Exported_Tons", "NOK/kg"]

    dataClean          = data.reset_index(drop = True)
    dataClean["Year"]  = dataClean["Date"].astype(str).str[:4].astype(int)
    dataClean["Week"]  = dataClean["Date"].astype(str).str[5:].astype(int)
    dataClean["Month"] = pd.to_datetime(dataClean["Date"].astype(str).str[:4] + "-W" + dataClean["Date"].astype(str).str[5:] + "-1",
                                        format= "%Y-W%W-%w").dt.month
    
    dataClean = dataClean.drop(columns = ["Date"])
    dataClean = dataClean[["Year", "Week", "Month", "Exported_Tons", "NOK/kg"]]

    return dataClean

fileName     = "SSB_Price_Data.xlsx"
dataSSBPrice = loadSSBPriceData(fileName)