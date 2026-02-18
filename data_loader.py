##
#  This module constructs the DataLoader class
##

##
#  Imports libraries needed
#
import pandas as pd
import numpy as np

##
# This class loads, processes the several salmon data time series
# addressed in the Thesis. For each dataset, returns the clean version.
#
class DataLoader:

    FISH_POOL     = "Data/Fish_Pool_Data.xls"
    CPI           = "Data/Consumer_Price_Index_Data.xlsx"
    EURNOK        = "Data/EURNOK_Data.xlsx"
    EXPORT_SALMON = "Data/SSB_Price_Data.xlsx"
    ESCAPES       = "Data/Escapes_Data.xlsx"
    BIOMASS       = "Data/Biomass_Data.xlsx"
    PIGPRICE      = "Data/German_Pig_Price_Data.xlsx"

    def __init__(self):
        pass

    ##
    #  Uploads, transforms and cleans the salmon price time series data
    #  From 1st week of january 2006 to 3rd week of january 2026
    #  @dataset Fish Pool Index 3-6 kg Norwegian salmon price
    #  @return weekly salmon price per kg, in NOK and EUR
    #  
    def loadFishPoolData(self):

        _fileName    = self.FISH_POOL
        _xls         = pd.ExcelFile(_fileName)
        _sheetNames  = np.flip(np.array(_xls.sheet_names))
        _datasetList = []

        for sheet in _sheetNames:
            _data = pd.read_excel(_fileName, sheet_name=sheet, skiprows=1)
            _datasetList.append(_data)

        dataClean = pd.concat(_datasetList, ignore_index=True)
        dataClean["Month"] = pd.to_datetime(dataClean["Month"], format="%B").dt.month
        dataClean.rename(columns={"NOK/kg": "NOK_kg",
                                "EUR/kg": "EUR_kg"}, inplace=True)
        
        return dataClean

    ##
    #  Uploads, transforms and cleans the CPI time series data
    #  From January 1932 to December 2025
    #  @dataset SSB Norwegian CPI
    #  @return Monthly or Annual CPI in percentage (%)
    #
    def loadCPIData(self, frequency = "Monthly"):

        _fileName      = self.CPI
        _data          = pd.read_excel(_fileName)
        _data          = _data[:-2]
        _data          = _data.iloc[::-1]

        if frequency == "Annual":

            dataClean         = _data.iloc[:,:2]
            dataClean.columns = ["Year", "CPI_Annual"]
            dataClean         = dataClean.reset_index(drop = True)

        elif frequency == "Monthly":

            _dataM         = _data.drop(columns = "Årsgj.snitt2")
            _months        = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
            _dataM.columns = ["Year"] + _months

            _years  = _dataM["Year"].values.tolist()
            _dates  = []
            
            for i in _years:
                for j in _months:
                    _dates.append(f"{i}-{j}-01")

            _values = _dataM[_months].to_numpy().ravel().tolist()

            _monthlyData = {"Date"       : _dates,
                           "CPI_Monthly": _values}

            dataClean         = pd.DataFrame(_monthlyData)
            dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")

        else:

            dataClean = "Select a valid frequency: Monthly or Annual"

        return dataClean

    ##
    #  Uploads, transforms and cleans the EURNOK time series data
    #  From 24 January 2000 to 22 January 2026
    #  @dataset Norges Bank EURNOK spot price
    #  @return daily EURNOK spot price
    #
    def loadEURNOKData(self):

        _fileName         = self.EURNOK
        _data             = pd.read_excel(_fileName, skiprows= 21, header= None)
        dataClean         = _data.T
        dataClean.columns = ["Date", "EURNOK_Daily"]
        dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")

        return dataClean

    ##
    #  Uploads, transforms and cleans the Salmon Export Price time series data
    #  From week 1 January 2000 to week 3 January 2026
    #  @dataset SSB exported salmon tons and price 
    #  @return weekly exported salmon tons and price per kilogram in NOK
    #
    def loadSSBPriceData(self):

        _fileName     = self.EXPORT_SALMON
        _data         = pd.read_excel(_fileName, header = None)
        _data         = _data.loc[3:,1:]
        _data         = _data.loc[:_data.dropna(how = "all").index[-1]]
        _data.columns = ["Date", "Exported_Tons", "NOK_kg"]

        dataClean          = _data.reset_index(drop = True)
        dataClean["Year"]  = dataClean["Date"].astype(str).str[:4].astype(int)
        dataClean["Week"]  = dataClean["Date"].astype(str).str[5:].astype(int)
        dataClean["Month"] = pd.to_datetime(dataClean["Date"].astype(str).str[:4] + "-W" + dataClean["Date"].astype(str).str[5:] + "-1",
                                            format= "%Y-W%W-%w").dt.month
        
        dataClean = dataClean.drop(columns = ["Date"])
        dataClean = dataClean[["Year", "Week", "Month", "Exported_Tons", "NOK_kg"]]

        return dataClean

    ##
    #  Uploads, transforms and cleans the Escapes time series data
    #  From week 12 January 2006 to 19 January 2026
    #  @dataset Directory of fisheries reported escapes per species
    #  @return "event" reported escapes per species, region, and company
    #
    def loadEscapesData(self):

        _fileName                 = self.ESCAPES
        _data                     = pd.read_excel(_fileName)
        _selectColumns            = ["Dato", "Lokalitets- navn", "Lokalitets- nummer", "Fylke", 
                                    "Selskap", "Art", "Rømmings- estimat", "Rapportert rømt",
                                    "Snittvekt (gram)", "Gjenfangst"]
        dataClean                = _data[_selectColumns]
        dataClean.loc[:,"Dato"]  = pd.to_datetime(dataClean["Dato"], format = "%m/%d/%Y").dt.date   
        dataClean                = dataClean.sort_values("Dato", ascending=True)
        dataClean                = dataClean.reset_index(drop = True)                      
        _columnNames             = ["Date", "Site_Name", "Site_Number", "County", "Company", 
                                    "Species", "Est_Num_Escaped", "Rep_Escaped", "Avg_Wt_Grams",
                                    "Recapture"]
        dataClean.columns        = _columnNames
        dataClean                = dataClean[dataClean["Species"] == "Laks"]
        dataClean                = dataClean.reset_index(drop = True) 

        return dataClean

    ##
    #  Uploads, transforms and cleans the Biomass time series data
    #  From October 2017 to December 2025
    #  @dataset Directory of fisheries detailed biomass data
    #  @return  "panel" monthly production-area-level aquaculture data on stock, biomass,
    #           feed, harvest, and losses
    #
    def loadBiomassData(self): 

        _fileName      = self.BIOMASS
        _data          = pd.read_excel(_fileName, sheet_name="Biomasse-prod-omr", skiprows=5)
        _selectColumns = ["ÅR", " MÅNED_KODE", " PO_KODE", " PO_NAVN", " ARTSID",
                        " BEHFISK_STK", " BIOMASSE_KG", " UTSETT_SMOLT_STK",
                        " FORFORBRUK_KG", " UTTAK_KG", " UTTAK_STK", " DØDFISK_STK",
                        " UTKAST_STK", " RØMMING_STK", " ANDRE_STK"]
        dataClean      = _data[_selectColumns]
        _columnNames   = ["Year", "Month", "Prod_Area_Code", "Prod_Area_Name", "Species", "Fish_Stock",
                        "Biomass_Kg", "Smolt_Stock", "Feed_Kg", "Harvest_Kg", "Harvest_N",
                        "Mortality_N", "Discard_N", "Escape_N", "Other_Loss_N"]
        dataClean.columns = _columnNames
        dataClean         = dataClean[dataClean["Species"] == "LAKS"]
        dataClean         = dataClean.reset_index(drop = True) 

        return dataClean

    ##
    #  Uploads, transforms and cleans the German pig price time series data
    #  From 30 December 2013 to 26 January 2026
    #  @dataset """"
    #  @return  weekly German pig prices ############
    #
    def loadPigPriceData(self):

        _fileName     = self.PIGPRICE
        _data         = pd.read_excel(_fileName, skiprows=1)
        _columnNames  = ["Date", "Price"]
        _data.columns = _columnNames
        _data["Date"] = pd.to_datetime(_data["Date"])
        dataClean     = _data.sort_values("Date", ascending=True).reset_index(drop=True)
        
        return dataClean
