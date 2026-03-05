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
    EXPORT        = "Data/Export_Data.xlsx"

    def __init__(self):
        pass

    ##                                                 ##
    # Upload the raw files and gets rid of the noise of #
    # unnecesary columns and formats                    #
    #                                                   #

    ##
    #  Uploads, cleans and transforms the salmon price time series data
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
        dataClean.rename(columns={"NOK/kg": "NOK_kg_FP_Weekly",
                                "EUR/kg": "EUR_kg_FP_Weekly"}, inplace=True)
        
        return dataClean

    ##
    #  Uploads, cleans and transforms the CPI time series data
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
                           "CPI_SSB_Monthly": _values}

            dataClean         = pd.DataFrame(_monthlyData)
            dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")

        else:

            dataClean = "Select a valid frequency: Monthly or Annual"

        return dataClean

    ##
    #  Uploads, cleans and transforms the EURNOK time series data
    #  From 24 January 2000 to 22 January 2026
    #  @dataset Norges Bank EURNOK spot price
    #  @return weekly EURNOK mid price
    #
    def loadEURNOKData(self):

        _fileName                  = self.EURNOK
        _data                      = pd.read_excel(_fileName, skiprows= 27, header= 0)
        dataClean                  = _data.iloc[:,:3]
        dataClean["EURNOK_RE_Weekly"] = (dataClean["Bid"] + dataClean["Ask"]) / 2
        _selectColumns             = ["Exchange Date", "EURNOK_RE_Weekly"]
        dataClean                  = dataClean[_selectColumns].rename(columns = {"Exchange Date": "Date"})
        dataClean["Date"]          = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")
        dataClean                  = dataClean.sort_values("Date", ascending=True)
        dataClean                  = dataClean.reset_index(drop = True)

        return dataClean

    ##
    #  Uploads, cleans and transforms the Salmon Export Price time series data
    #  From week 1 January 2000 to week 3 January 2026
    #  @dataset SSB exported salmon tons and price 
    #  @return weekly exported salmon tons and price per kilogram in NOK
    #
    def loadSSBPriceData(self):

        _fileName     = self.EXPORT_SALMON
        _data         = pd.read_excel(_fileName, header = None)
        _data         = _data.loc[3:,1:]
        _data         = _data.loc[:_data.dropna(how = "all").index[-1]]
        _data.columns = ["Date", "Exported_Tons_SSB_Weekly", "NOK_kg_SSB_Weekly"]

        dataClean          = _data.reset_index(drop = True)
        dataClean["Year"]  = dataClean["Date"].astype(str).str[:4].astype(int)
        dataClean["Week"]  = dataClean["Date"].astype(str).str[5:].astype(int)
        dataClean["Month"] = pd.to_datetime(dataClean["Date"].astype(str).str[:4] + "-W" + dataClean["Date"].astype(str).str[5:] + "-1",
                                            format= "%Y-W%W-%w").dt.month
        
        dataClean = dataClean.drop(columns = ["Date"])
        dataClean = dataClean[["Year", "Week", "Month", "Exported_Tons_SSB_Weekly", "NOK_kg_SSB_Weekly"]]

        return dataClean

    ##
    #  Uploads, cleans and transforms the Escapes time series data
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
    #  Uploads, cleans and transforms the Biomass time series data
    #  From October 2017 to December 2025
    #  @dataset Directory of fisheries detailed biomass data
    #  @return  "panel" monthly production-area-level aquaculture data on stock, biomass,
    #           feed, harvest, and losses
    #
    def loadBiomassData(self): 

        _fileName      = self.BIOMASS
        _data          = pd.read_excel(_fileName, sheet_name="Biomasse-flk", skiprows=5)
        _selectColumns = ["ÅR", " MÅNED_KODE", " FYLKE", " ARTSID",
                        " BEHFISK_STK", " BIOMASSE_KG", " UTSETT_SMOLT_STK",
                        " FORFORBRUK_KG", " UTTAK_KG", " UTTAK_STK", " DØDFISK_STK",
                        " UTKAST_STK", " RØMMING_STK", " ANDRE_STK"]
        dataClean      = _data[_selectColumns]
        _columnNames   = ["Year", "Month", "County", "Species", "Fish_Stock",
                        "Biomass_Kg", "Smolt_Stock", "Feed_Kg", "Harvest_Kg", "Harvest_N",
                        "Mortality_N", "Discard_N", "Escape_N", "Other_Loss_N"]
        dataClean.columns = _columnNames
        dataClean         = dataClean[dataClean["Species"] == "LAKS"]
        dataClean         = dataClean.reset_index(drop = True) 

        return dataClean

    ##
    #  Uploads, cleans and transforms the German pig price time series data
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
    
    ##
    #  Uploads, cleans and transforms the salmon export  time series data
    #  From January 2005 to December 2025
    #  @dataset """"
    #  @return  monthly salmon exports in weight and value in USD ############
    #
    def loadExportData(self):

        _fileName         = self.EXPORT
        _data             = pd.read_excel(_fileName, sheet_name= "Sheet1")
        _selectColumns    = ["refPeriodId", "netWgt", "primaryValueUSD", "AvgValueKg"]
        dataClean         = _data[_selectColumns].copy()
        _columnNames      = ["Date", "Net_Weight_Kg_Export_Monthly", "Value_USD_Export_Monthly", "Average_Price_USD_Kg_Export_Monthly"]
        dataClean.columns = _columnNames
        dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y%m%d")

        return dataClean
    
    ##                                                               ##
    # Transforms and makes the frequency, and object type adjustments #
    # to the variables                                                #
    #                                                                 #

    ##
    # Validates frequency match in weeks and object data types
    # @dataset from loadFishPoolData
    # @return weekly dates as integers and prices as float
    #
    def SalmonPriceFP(self):

        _data         = self.loadFishPoolData()
        dataTransform = _data.astype({
                          "Year"            : "int64",
                          "Week"            : "int64",
                          "Month"           : "int64",
                          "NOK_kg_FP_Weekly": "float64",
                          "EUR_kg_FP_Weekly": "float64"
                           }) 
        
        return dataTransform

    ##
    # Validates frequency match in months and object data types
    # @dataset from loadCPIData
    # @return monthly dates as integers and prices as float
    #
    def Cpi(self):

        _data                              = self.loadCPIData()
        _data["Year"]                      = _data["Date"].dt.year
        _data["Month"]                     = _data["Date"].dt.month
        dataTransform                      = _data.drop(columns = ["Date"])
        dataTransform                      = dataTransform[["Year", "Month"] 
                                                       + list(dataTransform.columns.drop(["Year", "Month"]))]
        dataTransform                      = dataTransform.astype({
                                           "Year"                     : "int64",
                                           "Month"                    : "int64",
                                           "CPI_SSB_Monthly"          : "float64",
                                           })
         
        return dataTransform
    
    ##
    # Validates frequency match in weeks and object data types
    # @dataset from loadEURNOKData
    # @return weekly dates as integers and EURNOK as float
    #
    def Eurnok(self):

        _data          = self.loadEURNOKData()
        _data["Year"]  = _data["Date"].dt.isocalendar().year
        _data["Week"]  = _data["Date"].dt.isocalendar().week
        _data["Month"] = _data["Date"].dt.month
        dataTransform  = _data.drop(columns = ["Date"])
        dataTransform  = dataTransform[["Year", "Week", "Month"]
                                       + list(dataTransform.columns.drop(["Year", "Week", "Month"]))]
        dataTransform  = dataTransform.astype({
                         "Year"            : "int64",
                         "Week"            : "int64",
                         "Month"           : "int64",
                         "EURNOK_RE_Weekly": "float64"
                         })

        return dataTransform

    ##
    # Validates frequency match in weeks and object data types
    # @dataset from loadSSBPriceData
    # @return weekly dates as integers and prices as float
    #
    def SalmonPriceSSB(self):

        _data         = self.loadSSBPriceData()
        dataTransform = _data.astype({
                         "Year"                    : "int64",
                         "Week"                    : "int64",
                         "Month"                   : "int64",
                         "Exported_Tons_SSB_Weekly": "float64",
                         "NOK_kg_SSB_Weekly"       : "float64"
                         })
        
        return dataTransform
    
    ##
    # Validates frequency match in weeks and object data types
    # @dataset from loadEscapesData (event driven)
    # @return weekly dates as integers and aggregates of observations per week
    #
    def Escapes(self):

        _data         = self.loadEscapesData()
        _data["Date"] = pd.to_datetime(_data["Date"])
        _data         = _data.set_index("Date")
        _data.loc[_data["Rep_Escaped"] == "E:10 - 100", "Rep_Escaped"] = "55"
        _data["Rep_Escaped"] = (
        _data["Rep_Escaped"]
        .fillna("0")
        .str.replace("E:", "", regex=False)
        )
        _data["Rep_Escaped"] = pd.to_numeric(_data["Rep_Escaped"], errors="coerce")
        dataTransform        = _data.resample("W").agg({
                              "Rep_Escaped" : "sum",
                              "Avg_Wt_Grams": "mean",
                              "Recapture"   : "sum"
                              })
        
        dataTransform          = dataTransform.reset_index()
        _colNames               = ["Date", "Rep_Escaped_DF_Weekly", "Avg_Wt_Grams_DF_Weekly", "Recapture_DF_Weekly" ]
        dataTransform.columns  = _colNames
        dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
        dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform          = dataTransform.drop(columns = ["Date"])
        dataTransform          = dataTransform[["Year", "Week", "Month"]
                                       + list(dataTransform.columns.drop(["Year", "Week", "Month"]))]
        dataTransform          = dataTransform.astype({
                                 "Year": "int64",
                                 "Week": "int64",
                                 "Month": "int64",
                                 "Rep_Escaped_DF_Weekly": "float64",
                                 "Avg_Wt_Grams_DF_Weekly": "float64",
                                 "Recapture_DF_Weekly": "float64"
        })

        return dataTransform

    ##
    # Validates frequency match in months and object data types
    # @dataset from loadBiomass (monthly)
    # @return monthly dates as integers and aggregates of observations per week
    #
    def Biomass(self):

        _data = self.loadBiomassData()
        dataTransform = (
                         _data
                         .groupby(["Year", "Month"], as_index = False)
                         .sum(numeric_only = True)
        )
        for col in dataTransform.columns:
            if col not in ["Year", "Month"]:
                dataTransform[col] = dataTransform[col].astype("float64")

        _colRename = dataTransform.columns.difference(["Year", "Month"])

        dataTransform = dataTransform.rename(
            columns={col: col + "_DF_Monthly" for col in _colRename}
        )

        return dataTransform
    

    ###
    #   Merge everything
    #   @datasets retrieved from various providers in weekly and monthly conventions
    #   @return weekly observations per feature, containing full information
    #
    def Data(self):
        _data      = self.SalmonPriceFP()
        _cpi       = self.Cpi()
        _eurnok    = self.Eurnok()
        _salmon    = self.SalmonPriceSSB()
        _escapes   = self.Escapes()
        _biomass   = self.Biomass()

        data       = _data.copy()

        # Weekly merges
        for w in [_salmon, _eurnok, _escapes]:
            w    = w.drop(columns=["Month"], errors="ignore")
            data = data.merge(w, on=["Year", "Week"], how="left", validate="one_to_one")

        # Monthly merges
        for m in [_cpi, _biomass]:
            data = data.merge(m, on=["Year", "Month"], how="left")
        
        data = data.iloc[1:-3].reset_index(drop = True)
        data.insert(0, "t", range(len(data)))
        data.insert(0, "Date",
                    pd.to_datetime(
                        data["Year"].astype(str) + "-W" + data["Week"].astype(str) + "-1",
                        format = "%G-W%V-%u"
                    ).dt.to_period("W")
                    )

        return data