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

    _salmon       = "Data/Salmon"
    _salmonEquity = "/Equity/"
    _salmonMarket = "/Market/"
    _protein      = "Data/Protein/"
    _currency     = "Data/Currency/"
    _commodity    = "Data/Commodity/"

    SALMON_PRICE_FISHPOOL  = _salmon + _salmonMarket + "Price_FishPool.xls"
    SALMON_PRICE_SSB       = _salmon + _salmonMarket + "Price_SSB.xlsx"
    SALMON_PRICE_BLOOMBERG = _salmon + _salmonMarket + "Price_Bloomberg.xlsx"
    SALMON_EXPORTS         = _salmon + _salmonMarket + "Exports.xlsx"
    SALMON_BIOMASS         = _salmon + _salmonMarket + "Biomass.xlsx"
    SALMON_ESCAPES         = _salmon + _salmonMarket + "Escapes.xlsx"
    CPI_NORWAY             = _salmon + _salmonMarket + "CPI.xlsx"

    PROTEIN_CPI_MEAT       = _protein + "CPI_Meat.xlsx"
    PROTEIN_PRICE_BROILER  = _protein + "Price_Broiler.xlsx"
    PROTEIN_PRICE_PIG      = _protein + "Price_Pig.xlsx"
   
    CURRENCY_EURNOK        = _currency + "EURNOK.xlsx"
    CURRENCY_USDNOK        = _currency + "USDNOK.xlsx"

    COMMODITY_BRENT        = _commodity + "Price_Brent.xlsx"
    COMMODITY_WHEAT        = _commodity + "Price_Wheat.xlsx"
    COMMODITY_SOYBEAN      = _commodity + "Price_Soybean.xlsx"
    COMMODITY_RAPSEED      = _commodity + "Price_Rapseed.xlsx"

    EQUITY_PRICE_MOWI      = _salmon + _salmonEquity + "Price_MOWI.xlsx"
    EQUITY_PRICE_SALMAR    = _salmon + _salmonEquity + "Price_SALMAR.xlsx"

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
    def SalmonPriceFishPool(self):

        ## Clean: Load file and format
        _fileName    = self.SALMON_PRICE_FISHPOOL
        _xls         = pd.ExcelFile(_fileName)
        _sheetNames  = np.flip(np.array(_xls.sheet_names))
        _datasetList = []

        for sheet in _sheetNames:
            _data = pd.read_excel(_fileName, sheet_name=sheet, skiprows=1)
            _datasetList.append(_data)

        dataClean = pd.concat(_datasetList, ignore_index=True)
        dataClean["Month"] = pd.to_datetime(dataClean["Month"], format="%B").dt.month
        dataClean.rename(columns={"NOK/kg": "Salmon_NOK_kg_FP_Weekly",
                                "EUR/kg": "Salmon_EUR_kg_FP_Weekly"}, inplace=True)
        
        ## Transform: validate datatypes and frequency match
        dataTransform = dataClean.copy()
        dataTransform = dataTransform.astype({
                          "Year"                   : "int64",
                          "Week"                   : "int64",
                          "Month"                  : "int64",
                          "Salmon_NOK_kg_FP_Weekly": "float64",
                          "Salmon_EUR_kg_FP_Weekly": "float64"
                           }) 
        
        return dataTransform
    
    ##
    #  Uploads, cleans and transforms the Salmon Export Price time series data
    #  From week 1 January 2000 to week 3 January 2026
    #  @dataset SSB exported salmon tons and price 
    #  @return weekly exported salmon tons and price per kilogram in NOK
    #
    def SalmonPriceSSB(self):

        ## Clean  
        _fileName     = self.SALMON_PRICE_SSB
        _data         = pd.read_excel(_fileName, header = None)
        _data         = _data.loc[3:,1:]
        _data         = _data.loc[:_data.dropna(how = "all").index[-1]]
        _data.columns = ["Date", "Salmon_Exported_Tons_SSB_Weekly", "Salmon_NOK_kg_SSB_Weekly"]

        dataClean          = _data.reset_index(drop = True)
        dataClean["Year"]  = dataClean["Date"].astype(str).str[:4].astype(int)
        dataClean["Week"]  = dataClean["Date"].astype(str).str[5:].astype(int)
        dataClean["Month"] = pd.to_datetime(dataClean["Date"].astype(str).str[:4] + "-W" + dataClean["Date"].astype(str).str[5:] + "-1",
                                            format= "%G-W%V-%u").dt.month
        
        dataClean = dataClean.drop(columns = ["Date"])
        dataClean = dataClean[["Year", "Week", "Month", "Salmon_Exported_Tons_SSB_Weekly", "Salmon_NOK_kg_SSB_Weekly"]]

        ## Transform
        dataTransform = dataClean.copy()
        dataTransform = dataTransform.astype({
                         "Year"                           : "int64",
                         "Week"                           : "int64",
                         "Month"                          : "int64",
                         "Salmon_Exported_Tons_SSB_Weekly": "float64",
                         "Salmon_NOK_kg_SSB_Weekly"       : "float64"
                         })
        
        return dataTransform
    
    ##
    #  Uploads, cleans and transforms the Salmon Export Fresh Price time series data
    #  From 5 March 2000 to week 3 January 2026
    #  @dataset from bloomberg ticker NOSMFSVL, exports fresh price, source SSB
    #  @return weekly exported salmon price per kilogram in NOK
    #
    def SalmonPriceBloomberg(self):

        return self._loadWeekly(
                                self.SALMON_PRICE_BLOOMBERG,
                                "Salmon_NOK_kg_BB_Weekly"
                                )

    ##
    #  Uploads, cleans and transforms the salmon export  time series data
    #  From January 2005 to December 2025
    #  @dataset """"
    #  @return  monthly salmon exports in weight and value in USD ############
    #
    def SalmonExport(self):
        
        ## Clean
        _fileName         = self.SALMON_EXPORTS
        _data             = pd.read_excel(_fileName, sheet_name= "Sheet1")
        _selectColumns    = ["refPeriodId", "netWgt", "primaryValueUSD", "AvgValueKg"]
        dataClean         = _data[_selectColumns].copy()
        _columnNames      = ["Date", "Salmon_Export_Net_Weight_Kg_Monthly", "Salmon_Export_Value_USD_Monthly"
                             , "Salmon_Export_Avg_Price_USD_Kg_Monthly"]
        dataClean.columns = _columnNames
        dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y%m%d")

        ## Transform
        dataTransform          = dataClean.copy()
        dataTransform["Year"]  = dataTransform["Date"].dt.year
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform          = dataTransform.drop(columns = ["Date"])
        dataTransform          = dataTransform[["Year", "Month"]
                                       + list(dataTransform.columns.drop(["Year", "Month"]))]
        dataTransform          = dataTransform.astype({
                                "Year"                                  : "int64",
                                "Month"                                 : "int64",
                                "Salmon_Export_Net_Weight_Kg_Monthly"   : "float64",
                                "Salmon_Export_Value_USD_Monthly"       : "float64",
                                "Salmon_Export_Avg_Price_USD_Kg_Monthly": "float64"
                                })

        return dataTransform

    ##
    #  Uploads, cleans and transforms the Biomass time series data
    #  From January 2000 to January 2026
    #  @dataset Directory of fisheries detailed biomass data
    #  @return  "panel" monthly production-area-level aquaculture data on stock, biomass,
    #           feed, harvest, and losses
    #
    def SalmonBiomass(self): 

        ## Cleans
        _fileName      = self.SALMON_BIOMASS
        _data          = pd.read_excel(_fileName, sheet_name="Biomasse-flk", skiprows=5)
        _selectColumns = ["ÅR", " MÅNED_KODE", " FYLKE", " ARTSID",
                        " BEHFISK_STK", " BIOMASSE_KG", " UTSETT_SMOLT_STK",
                        " FORFORBRUK_KG", " UTTAK_KG", " UTTAK_STK", " DØDFISK_STK",
                        " UTKAST_STK", " RØMMING_STK", " ANDRE_STK"]
        dataClean      = _data[_selectColumns]
        _columnNames   = ["Year", "Month", "Salmon_Biomass_County", "Salmon_Biomass_Species", "Salmon_Biomass_Fish_Stock",
                        "Salmon_Biomass_Kg", "Salmon_Biomass_Smolt_Stock", "Salmon_Biomass_Feed_Kg", "Salmon_Biomass_Harvest_Kg", 
                        "Salmon_Biomass_Harvest_N", "Salmon_Biomass_Mortality_N", "Salmon_Biomass_Discard_N", 
                        "Salmon_Biomass_Escape_N", "Salmon_Biomass_Other_Loss_N"]
        dataClean.columns = _columnNames
        dataClean         = dataClean[dataClean["Salmon_Biomass_Species"] == "LAKS"]
        dataClean         = dataClean.reset_index(drop = True) 

        ## Transform
        dataTransform = dataClean.copy()
        dataTransform = (
                         dataTransform
                         .groupby(["Year", "Month"], as_index = False)
                         .sum(numeric_only = True)
        )
        for col in dataTransform.columns:
            if col not in ["Year", "Month"]:
                dataTransform[col] = dataTransform[col].astype("float64")

        _colRename = dataTransform.columns.difference(["Year", "Month"])

        dataTransform = dataTransform.rename(
            columns={col: col + "_Monthly" for col in _colRename}
        )

        return dataTransform

    ##
    #  Uploads, cleans and transforms the Escapes time series data
    #  From week 12 January 2006 to 19 January 2026
    #  @dataset Directory of fisheries reported escapes per species
    #  @return "event" reported escapes per species, region, and company
    #
    def SalmonEscapes(self):
        
        ## Clean
        _fileName                 = self.SALMON_ESCAPES
        _data                     = pd.read_excel(_fileName)
        _selectColumns            = ["Dato", "Lokalitets- navn", "Lokalitets- nummer", "Fylke", 
                                    "Selskap", "Art", "Rømmings- estimat", "Rapportert rømt",
                                    "Snittvekt (gram)", "Gjenfangst"]
        dataClean                = _data[_selectColumns]
        dataClean.loc[:,"Dato"]  = pd.to_datetime(dataClean["Dato"], format = "%m/%d/%Y").dt.date   
        dataClean                = dataClean.sort_values("Dato", ascending=True)
        dataClean                = dataClean.reset_index(drop = True)                      
        _columnNames             = ["Date", "Salmon_Escapes_Site_Name", "Salmon_Escapes_Site_Number", 
                                    "Salmon_Escapes_County", "Salmon_Escapes_Company", "Salmon_Escapes_Species", 
                                    "Salmon_Escapes_Est_Num_Escaped", "Salmon_Escapes_Rep_Escaped", "Salmon_Escapes_Avg_Wt_Grams",
                                    "Salmon_Escapes_Recapture"]
        dataClean.columns        = _columnNames
        dataClean                = dataClean[dataClean["Salmon_Escapes_Species"] == "Laks"]
        dataClean                = dataClean.reset_index(drop = True) 

        ## Transform
        dataTransform = dataClean.copy()
        dataTransform["Date"] = pd.to_datetime(dataTransform["Date"])
        dataTransform         = dataTransform.set_index("Date")
        dataTransform.loc[dataTransform["Salmon_Escapes_Rep_Escaped"] == "E:10 - 100", "Salmon_Escapes_Rep_Escaped"] = "55"
        dataTransform["Salmon_Escapes_Rep_Escaped"] = (
                                dataTransform["Salmon_Escapes_Rep_Escaped"]
                                .astype(str)
                                .fillna("0")
                                .str.replace("E:", "", regex=False)
                                )
        dataTransform["Salmon_Escapes_Rep_Escaped"] = pd.to_numeric(dataTransform["Salmon_Escapes_Rep_Escaped"], errors="coerce")
        dataTransform        = dataTransform.resample("W-MON").agg({
                              "Salmon_Escapes_Rep_Escaped" : "sum",
                              "Salmon_Escapes_Avg_Wt_Grams": "mean",
                              "Salmon_Escapes_Recapture"   : "sum"
                              })
        
        dataTransform          = dataTransform.reset_index()
        _colNames              = ["Date", "Salmon_Escapes_Rep_Escaped_Weekly", "Salmon_Escapes_Avg_Wt_Grams_Weekly", "Salmon_Escapes_Recapture_Weekly" ]
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
                                 "Salmon_Escapes_Rep_Escaped_Weekly" : "float64",
                                 "Salmon_Escapes_Avg_Wt_Grams_Weekly": "float64",
                                 "Salmon_Escapes_Recapture_Weekly"   : "float64"
        })

        return dataTransform

    ##
    #  Uploads, cleans and transforms the CPI time series data
    #  From January 1932 to December 2025
    #  @dataset SSB Norwegian CPI
    #  @return Monthly or Annual CPI in percentage (%)
    #
    def CPINorway(self):

        ## Clean
        _fileName      = self.CPI_NORWAY
        _data          = pd.read_excel(_fileName)
        _data          = _data[:-2]
        _data          = _data.iloc[::-1]

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
                           "CPI_Norway_Monthly": _values}

        dataClean         = pd.DataFrame(_monthlyData)
        dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")

        ## Transform
        dataTransform          = dataClean.copy()
        dataTransform["Year"]  = dataTransform["Date"].dt.year
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform  = dataTransform.drop(columns = ["Date"])
        dataTransform  = dataTransform[["Year", "Month"] 
                         + list(dataTransform.columns.drop(["Year", "Month"]))]
        dataTransform  = dataTransform.astype({
                        "Year"               : "int64",
                        "Month"              : "int64",
                        "CPI_Norway_Monthly" : "float64",
                        })
        
        return dataTransform

    ##
    #  Uploads, cleans and transforms the CPI EU Meat time series data
    #  From 31 January 2012 to 09 February 2026
    #  @dataset from bloomberg ticker CP12EAYY, source Eurostat
    #  @return  monthly  index of consumer prices based on meat
    #
    def ProteinCPIMeat(self):
        
        ## Clean
        _fileName         = self.PROTEIN_CPI_MEAT
        _data             = pd.read_excel(_fileName, header = 0)
        dataClean         = _data.copy()
        dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")
        dataClean         = dataClean.sort_values("Date", ascending=True)
        dataClean         = dataClean.rename(columns = {"Last Price" : "Protein_CPI_Meat_Monthly"})
        dataClean         = dataClean.reset_index(drop = True)

        ## Transform
        dataTransform          = dataClean.copy()
        dataTransform["Year"]  = dataTransform["Date"].dt.year
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform          = dataTransform.drop(columns = ["Date"])
        dataTransform          = dataTransform[["Year", "Month"]
                                       + list(dataTransform.columns.drop(["Year", "Month"]))]
        dataTransform          = dataTransform.astype({
                                "Year"                   : "int64",
                                "Month"                  : "int64",
                                "Protein_CPI_Meat_Monthly": "float64"
                                })

        return dataTransform
    
    ##
    #  Uploads, cleans and transforms the Broiler Price EU time series data
    #  From 06 January 2012 to 09 February 2026
    #  @dataset from bloomberg ticker POPRBREU, source European Comission
    #  @return  weekly price in EUR per 100 kg
    #
    def ProteinBroilerPrice(self):

        return self._loadWeekly(
            self.PROTEIN_PRICE_BROILER,
            "Protein_Broiler_EUR_100_kg_Weekly"
        )
    
    ##
    #  Uploads, cleans and transforms the Pig Price EU time series data
    #  From 03 January 2014 to 27 February 2026
    #  @dataset from bloomberg ticker EUPGCEDE, source European Comission
    #  @return  weekly price in EUR per 100 kg
    #
    def ProteinPigPrice(self):

        return self._loadWeekly(
            self.PROTEIN_PRICE_PIG,
            "Protein_Pig_EUR_100_kg_Weekly"
        )

    ##
    #  Uploads, cleans and transforms the EURNOK time series data
    #  From 07 January 2000 to 05 March 2026
    #  @dataset EURNOK Last price, source Bloomberg
    #  @return weekly EURNOK last price
    #
    def EURNOK(self):

     return self._loadWeekly(
                            self.CURRENCY_EURNOK,
                            "EURNOK_Weekly"
                            )

    ##
    #  Uploads, cleans and transforms the USDNOK time series data
    #  From 07 January 2000 to 05 March 2026
    #  @dataset USDNOK Last price, source Bloomberg
    #  @return weekly USDNOK last price
    #
    def USDNOK(self):

        return self._loadWeekly(
                                self.CURRENCY_USDNOK,
                                "USDNOK_Weekly"
                               )
    
    
    ##
    #  Uploads, cleans and transforms the Brent time series data
    #  From 07 January 2000 to 05 March 2026
    #  @dataset Bloomberg ticker CO1, source ICE
    #  @return weekly Brent last price per barrel in NOK
    #
    def CommodityBrentPrice(self):
        
        return self._loadWeekly(
            self.COMMODITY_BRENT,
            "Commodity_Brent_NOK_bbl_Weekly"
        )

    ##
    #  Uploads, cleans and transforms the Wheat time series data
    #  From 07 January 2000 to 05 March 2026
    #  @dataset Bloomberg ticker CA2, source EOP-Euronext Derivatives
    #  @return weekly wheat last price per metric tone in NOK
    #
    def CommodityWheatPrice(self):

        return self._loadWeekly(
            self.COMMODITY_WHEAT,
            "Commodity_Wheat_NOK_mt_Weekly"
        )

    ##
    #  Uploads, cleans and transforms the Soybean time series data
    #  From 07 January 2000 to 05 March 2026
    #  @dataset Bloomberg ticker SM1, source CBOT
    #  @return weekly soybean last price per short ton in NOK
    #
    def CommoditySoybeanPrice(self):

        return self._loadWeekly(
            self.COMMODITY_SOYBEAN,
            "Commodity_Soybean_NOK_st_Weekly"
        )
    
    ##
    #  Uploads, cleans and transforms the Rapseed time series data
    #  From 07 January 2000 to 05 March 2026
    #  @dataset Bloomberg ticker IJ1, source EOP-Euronext Derivatives
    #  @return weekly rapseed last price per metric ton in NOK
    #
    def CommodityRapseedPrice(self):

        return self._loadWeekly(
            self.COMMODITY_RAPSEED,
            "Commodity_Rapseed_NOK_mt_Weekly"
        )
        
    ##
    #  Uploads, cleans and transforms the MOWI time series data
    #  From 07 January 2000 to 05 March 2026
    #  @dataset Bloomberg ticker MOWI, source Bloomberg
    #  @return weekly MOWI last price in NOK
    #
    def EquityMOWIPrice(self):

        return self._loadWeekly(
            self.EQUITY_PRICE_MOWI,
            "Equity_MOWI_NOK_Weekly"
        )

    ##
    #  Uploads, cleans and transforms the SALMAR time series data
    #  From 07 January 2007 to 05 March 2026
    #  @dataset Bloomberg ticker SALMAR, source EOP-Euronext Derivatives
    #  @return weekly SALMAR last price in NOK
    #
    def EquitySALMARPrice(self):

        return self._loadWeekly(
            self.EQUITY_PRICE_SALMAR,
            "Equity_SALMAR_NOK_Weekly"
        )

    ##
    #  Generic loader for Bloomberg-style time series
    #  Converts daily data to weekly frequency aligned to Monday
    #
    def _loadWeekly(self, fileName, columnName):

        ## Clean
        _data = pd.read_excel(fileName, header=0)

        dataClean = _data.copy()
        dataClean["Date"] = pd.to_datetime(dataClean["Date"], format="%Y-%m-%d")
        dataClean = dataClean.sort_values("Date", ascending=True)
        dataClean = dataClean.rename(columns={"Last Price": columnName})

        ## Transform
        dataTransform = (
            dataClean
            .set_index("Date")
            .resample("W-MON")
            .last()
            .reset_index()
        )

        dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
        dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform          = dataTransform[["Year", "Week", "Month"]
                                       + list(dataTransform.columns.drop(["Year", "Week", "Month"]))]

        dataTransform = dataTransform.drop(columns=["Date"])

        dataTransform = dataTransform.astype({
            "Year": "int64",
            "Week": "int64",
            "Month": "int64",
            columnName: "float64"
        })

        return dataTransform

    ##
    #   Merge everything
    #   @datasets retrieved from various providers in weekly and monthly conventions
    #   @return weekly observations per feature, containing full information
    #
    def Data(self):

        ## Load datasets
        _data        = self.SalmonPriceFishPool()

        _salmonsb    = self.SalmonPriceSSB()
        _salmonbb    = self.SalmonPriceBloomberg()
        _escapes     = self.SalmonEscapes()

        _broiler     = self.ProteinBroilerPrice()
        _pig         = self.ProteinPigPrice()

        _eurnok      = self.EURNOK()
        _usdnok      = self.USDNOK()

        _brent       = self.CommodityBrentPrice()
        _wheat       = self.CommodityWheatPrice()
        _soybean     = self.CommoditySoybeanPrice()
        _rapseed     = self.CommodityRapseedPrice()

        _mowi        = self.EquityMOWIPrice()
        _salmar      = self.EquitySALMARPrice()

        _cpi         = self.CPINorway()
        _cpimeat     = self.ProteinCPIMeat()
        _biomass     = self.SalmonBiomass()
        _exports     = self.SalmonExport()

        ## Create Date from FishPool
        _data["Date"] = pd.to_datetime(
            _data["Year"].astype(str)
            + "-W"
            + _data["Week"].astype(str).str.zfill(2)
            + "-1",
            format="%G-W%V-%u"
        )

        ## Create continuous weekly calendar
        start = _data["Date"].min()
        end   = _data["Date"].max()

        calendar = pd.DataFrame({
            "Date": pd.date_range(start=start, end=end, freq="W-MON")
        })

        calendar["Year"]  = calendar["Date"].dt.isocalendar().year
        calendar["Week"]  = calendar["Date"].dt.isocalendar().week
        calendar["Month"] = calendar["Date"].dt.month

        ## Base dataset
        data = calendar.merge(
            _data.drop(columns=["Date"], errors="ignore"),
            on=["Year","Week","Month"],
            how="left"
        )

        ## Weekly merges (aligned by Date)
        for w in [
            _salmonsb, _salmonbb, _escapes,
            _broiler, _pig,
            _eurnok, _usdnok,
            _brent, _wheat, _soybean, _rapseed,
            _mowi, _salmar
        ]:

            w = w.copy()

            w["Date"] = pd.to_datetime(
                w["Year"].astype(str)
                + "-W"
                + w["Week"].astype(str).str.zfill(2)
                + "-1",
                format="%G-W%V-%u"
            )

            w = w.drop(columns=["Year","Week","Month"], errors="ignore")

            data = data.merge(
                w,
                on="Date",
                how="left"
            )

        ## Monthly merges
        for m in [_cpi, _cpimeat, _biomass, _exports]:

            m = m.groupby(["Year","Month"], as_index=False).first()

            data = data.merge(
                m,
                on=["Year","Month"],
                how="left",
                validate="many_to_one"
            )

        ## Sort dataset
        data = data.sort_values("Date").reset_index(drop=True)


        ## Forwards fill only market variables
        _fillCols = data.columns[
            data.columns.str.contains(
                "Weekly|Commodity|Equity|EURNOK|USDNOK|Salmon_NOK_kg"
            )
        ]

        ## Exclude biomass and escapes
        _fillCols = _fillCols[
            ~_fillCols.str.contains("Biomass|Escapes")
        ]

        data[_fillCols] = data[_fillCols].ffill()

        ## Time index
        data.insert(0, "t", range(len(data)))

        ## Convert Date to weekly period
        data["Date"] = data["Date"].dt.to_period("W")

        _cutoff = pd.Period("2025-12-28", freq="W")

        data = data[data["Date"] <= _cutoff]

        return data
    
    ##
    #   Validates merged dataset integrity and data quality
    #   @param data dataframe returned by Data()
    #   @return diagnostic output and validation assertions
    #
    def ValidateData(self, data):

        ## Structural checks
        print("\n--- DATASET STRUCTURE ---")
        print("Rows   :", len(data))
        print("Cols   :", len(data.columns))

        assert "Year" in data.columns
        assert "Week" in data.columns
        assert "Month" in data.columns
        assert "Date" in data.columns

        ## Time ordering
        print("\n--- TIME ORDER CHECK ---")

        assert data["Date"].is_monotonic_increasing
        print("Date ordering: OK")

        ## Key uniqueness
        print("\n--- KEY UNIQUENESS ---")

        _dupYW = data.duplicated(["Year","Week"]).sum()
        _dupD  = data["Date"].duplicated().sum()

        print("Duplicate Year-Week :", _dupYW)
        print("Duplicate Date      :", _dupD)

        assert _dupYW == 0
        assert _dupD  == 0

        ## Missing values
        print("\n--- MISSING VALUES (%) ---")

        _missing = (data.isna().mean()*100).sort_values(ascending=False)

        print(_missing[_missing > 0].head(20))

        ## Monthly merge consistency
        print("\n--- MONTHLY MERGE CONSISTENCY ---")

        _monthlyCols = data.filter(like="_Monthly").columns

        if len(_monthlyCols) > 0:

            _check = data.groupby(["Year","Month"])[_monthlyCols].nunique()

            _max = _check.max().max()

            print("Max unique values per month:", _max)

            assert _max <= 1

            print("Monthly merge consistency: OK")

        ## Numeric summary
        print("\n--- NUMERIC SUMMARY ---")

        _numeric = data.select_dtypes(include="number")

        print(_numeric.describe().T.head(10))

        ## Extreme values
        print("\n--- EXTREME VALUE CHECK ---")

        _extreme = (_numeric.abs() > 1e6).sum()

        print(_extreme[_extreme > 0])

        ## Time continuity
        print("\n--- WEEK CONTINUITY ---")

        _weekDiff = data["Date"].diff().dropna()

        print(_weekDiff.value_counts().head())

        ## Column duplication
        print("\n--- COLUMN DUPLICATION ---")

        _dupCols = data.columns[data.columns.duplicated()]

        print("Duplicate columns:", list(_dupCols))

        assert len(_dupCols) == 0

        print("\nDATA VALIDATION PASSED")