##
#  This class loads, transforms and processes the data needed
##

##
#  This section imports libraries needed
#
import pandas as pd
import numpy as np

# This class loads, processes the diverse salmon data time series
# presented. For each dataset, returns the clean version.
#
class DataLoader:

    def __init__(self):
        self._fishPool     = "Data/Fish_Pool_Data.xls"
        self._cpi          = "Data/Consumer_Price_Index_Data.xlsx"
        self._eurNok       = "Data/EURNOK_Data.xlsx"
        self._exportSalmon = "Data/SSB_Price_Data.xlsx"
        self._escapes      = "Data/Escapes_Data.xlsx"
        self._biomass      = "Data/Biomass_Data.xlsx"
        self._pigPrice     = "Data/German_Pig_Price_Data.xlsx"

    
    ##
    #  This section uploads, transforms and cleans the salmon price time series data
    #  From 1st week of january 2006 to 3rd week of january 2026
    #  @dataset Fish Pool Index 3-6 kg Norwegian salmon price
    #  @return weekly salmon price per kg, in NOK and EUR
    #  
    def loadFishPoolData(self):

        fileName    = self._fishPool
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
        dataClean.rename(columns = {"NOK/kg": "NOK_kg",
                                    "EUR/kg": "EUR_kg"}, inplace = True)

        return dataClean

    #fileName     = "Fish_Pool_Data.xls"
    #dataFishPool = loadFishPoolData(fileName)

    ##
    #  This section uploads, transforms and cleans the CPI time series data
    #  From January 1932 to December 2025
    #  @dataset SSB Norwegian CPI
    #  @return Monthly or Annual CPI in percentage (%)
    #
    def loadCPIData(self, frequency = "Monthly"):

        fileName      = self._cpi
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

            dataClean = "Select a valid frequency: Monthly or Annual"

        return dataClean

    #fileName = "Consumer_Price_Index_Data.xlsx"
    #dataCPI  = loadCPIData(fileName, frequency="Annual")

    ##
    #  This section uploads, transforms and cleans the EURNOK time series data
    #  From 24 January 2000 to 22 January 2026
    #  @dataset Norges Bank EURNOK spot price
    #  @return daily EURNOK spot price
    #
    def loadEURNOKData(self):

        fileName          = self._eurNok
        data              = pd.read_excel(fileName, skiprows= 21, header= None)
        dataClean         = data.T
        dataClean.columns = ["Date", "EURNOK_Daily"]
        dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")

        return dataClean

    #fileName   = "EURNOK_Data.xlsx"
    #dataEURNOK = loadEURNOKData(fileName)

    ##
    #  This section uploads, transforms and cleans the Salmon Export Price time series data
    #  From week 1 January 2000 to week 3 January 2026
    #  @dataset SSB exported salmon tons and price 
    #  @return weekly exported salmon tons and price per kilogram in NOK
    #
    def loadSSBPriceData(self):

        fileName     = self._exportSalmon
        data         = pd.read_excel(fileName, header = None)
        data         = data.loc[3:,1:]
        data         = data.loc[:data.dropna(how = "all").index[-1]]
        data.columns = ["Date", "Exported_Tons", "NOK_kg"]

        dataClean          = data.reset_index(drop = True)
        dataClean["Year"]  = dataClean["Date"].astype(str).str[:4].astype(int)
        dataClean["Week"]  = dataClean["Date"].astype(str).str[5:].astype(int)
        dataClean["Month"] = pd.to_datetime(dataClean["Date"].astype(str).str[:4] + "-W" + dataClean["Date"].astype(str).str[5:] + "-1",
                                            format= "%Y-W%W-%w").dt.month
        
        dataClean = dataClean.drop(columns = ["Date"])
        dataClean = dataClean[["Year", "Week", "Month", "Exported_Tons", "NOK_kg"]]

        return dataClean

    #fileName     = "SSB_Price_Data.xlsx"
    #dataSSBPrice = loadSSBPriceData(fileName)

    ##
    #  This section uploads, transforms and cleans the Escapes time series data
    #  From week 12 January 2006 to 19 January 2026
    #  @dataset Directory of fisheries reported escapes per species
    #  @return "event" reported escapes per species, region, and company
    #
    def loadEscapesData(self):

        fileName                 = self._escapes
        data                     = pd.read_excel(fileName)
        selectColumns            = ["Dato", "Lokalitets- navn", "Lokalitets- nummer", "Fylke", 
                                    "Selskap", "Art", "Rømmings- estimat", "Rapportert rømt",
                                    "Snittvekt (gram)", "Gjenfangst"]
        dataClean                = data[selectColumns]
        dataClean.loc[:,"Dato"]  = pd.to_datetime(dataClean["Dato"], format = "%m/%d/%Y").dt.date   
        dataClean                = dataClean.sort_values("Dato", ascending=True)
        dataClean                = dataClean.reset_index(drop = True)                      
        columnNames              = ["Date", "Site_Name", "Site_Number", "County", "Company", 
                                    "Species", "Est_Num_Escaped", "Rep_Escaped", "Avg_Wt_Grams",
                                    "Recapture"]
        dataClean.columns        = columnNames
        dataClean                = dataClean[dataClean["Species"] == "Laks"]
        dataClean                = dataClean.reset_index(drop = True) 

        return dataClean

    #fileName = "Escapes_Data.xlsx"
    #dataEscapes = loadEscapesData(fileName)

    ##
    #  This section uploads, transforms and cleans the Biomass time series data
    #  From October 2017 to December 2025
    #  @dataset Directory of fisheries detailed biomass data
    #  @return  "panel" monthly production-area-level aquaculture data on stock, biomass,
    #           feed, harvest, and losses
    #
    def loadBiomassData(self): 

        fileName      = self._biomass
        data          = pd.read_excel(fileName, sheet_name="Biomasse-prod-omr", skiprows=5)
        selectColumns = ["ÅR", " MÅNED_KODE", " PO_KODE", " PO_NAVN", " ARTSID",
                        " BEHFISK_STK", " BIOMASSE_KG", " UTSETT_SMOLT_STK",
                        " FORFORBRUK_KG", " UTTAK_KG", " UTTAK_STK", " DØDFISK_STK",
                        " UTKAST_STK", " RØMMING_STK", " ANDRE_STK"]
        dataClean     = data[selectColumns]
        columnNames   = ["Year", "Month", "Prod_Area_Code", "Prod_Area_Name", "Species", "Fish_Stock",
                        "Biomass_Kg", "Smolt_Stock", "Feed_Kg", "Harvest_Kg", "Harvest_N",
                        "Mortality_N", "Discard_N", "Escape_N", "Other_Loss_N"]
        dataClean.columns = columnNames
        dataClean         = dataClean[dataClean["Species"] == "LAKS"]
        dataClean         = dataClean.reset_index(drop = True) 

        return dataClean

    #fileName = "Biomass_Data.xlsx"
    #dataBiomass = loadBiomassData(fileName)

    ##
    #  This section uploads, transforms and cleans the German pig price time series data
    #  From 30 December 2013 to 26 January 2026
    #  @dataset """"
    #  @return  weekly German pig prices ############
    #
    def loadPigPriceData(self):

        fileName     = self._pigPrice
        data         = pd.read_excel(fileName, skiprows=1)
        columnNames  = ["Date", "Price"]
        data.columns = columnNames
        data["Date"] = pd.to_datetime(data["Date"])
        dataClean    = data.sort_values("Date", ascending=True).reset_index(drop=True)
        
        return dataClean

    #fileName = "German_Pig_Price_Data.xlsx"
    #dataGermanPigPrice = loadPigPriceData(fileName)
