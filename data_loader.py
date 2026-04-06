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
    SALMON_LICE            = _salmon + _salmonMarket + "sealice_norway_weekly.xlsx"
    SALMON_ILA             = _salmon + "/ila_pd.csv"
    
    CPI_NORWAY             = _salmon + _salmonMarket + "CPI_YOY.xlsx"
    PROTEIN_SHRIMP_PRICE   = _protein + "GlobalShrimpPrice.xlsx"
    PROTEIN_CPI_MEAT       = _protein + "CPI_Meat.xlsx"
    PROTEIN_PRICE_BROILER  = _protein + "Price_Broiler.xlsx"
    PROTEIN_PRICE_PIG      = _protein + "Price_Pig.xlsx"
   
    CURRENCY_EURNOK        = _currency + "EURNOK.xlsx"
    CURRENCY_USDNOK        = _currency + "USDNOK.xlsx"

    COMMODITY_BRENT        = _commodity + "Price_Brent.xlsx"
    COMMODITY_WHEAT        = _commodity + "Price_Wheat.xlsx"
    COMMODITY_SOYBEAN      = _commodity + "Price_Soybean.xlsx"
    COMMODITY_RAPSEED      = _commodity + "Price_Rapseed.xlsx"
    COMMODITY_CARBON       = _commodity + "Price_Carbon.xlsx"
    COMMODITY_FISHMEAL     = _commodity + "Price_Fishmeal.xlsx"

    EQUITY_PRICE_MOWI      = _salmon + _salmonEquity + "Price_MOWI.xlsx"
    EQUITY_PRICE_SALMAR    = _salmon + _salmonEquity + "Price_SALMAR.xlsx"

    SALMON_FORWARD_OLD     = _salmon + _salmonMarket + "Forwardprices_20062024.csv"
    SALMON_FORWARD_NEW     = _salmon + _salmonMarket + "Forwardprices_20252026.csv"
    SALMON_FORWARD_BBG     = _salmon + _salmonMarket + "Forward_Prices_Bloomberg.xlsx"
    SALMON_NIBOR           = _salmon + _salmonMarket + "NIBOR.xlsx"
    SALMON_CHILE_EXPORTS   = _salmon + _salmonMarket + "Exports_Chile.xlsx"

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

        assert not dataTransform.duplicated(subset=["Year", "Week"]).any(), \
            "FishPool has duplicate (Year, Week) rows with conflicting values"

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
    #def SalmonPriceBloomberg(self):

        #return self._loadWeekly(
        #                        self.SALMON_PRICE_BLOOMBERG,
         #                       "Salmon_NOK_kg_BB_Weekly"
          #                      )

    ##
    #  Uploads, cleans and transforms the salmon export  time series data
    #  From January 2005 to December 2025
    #  @dataset """"
    #  @return  monthly salmon exports in weight and value in USD ############
    #
    def SalmonExport(self):

        ## Clean
        ## Source: UN Comtrade — Norway exports of Atlantic salmon (cmdCode 030212)
        ## ReleaseDate column contains the actual publication date per observation,
        ## enabling precise publication lag computation for the FeatureEngineer registry
        _fileName         = self.SALMON_EXPORTS
        _data             = pd.read_excel(_fileName, sheet_name= "Sheet1")
        _selectColumns    = ["refPeriodId", "ReleaseDate", "netWgt", "primaryValueUSD", "AvgValueKg"]
        dataClean         = _data[_selectColumns].copy()
        _columnNames      = ["Date", "ReleaseDate", "Salmon_Export_Net_Weight_Kg_Monthly",
                             "Salmon_Export_Value_USD_Monthly", "Salmon_Export_Avg_Price_USD_Kg_Monthly"]
        dataClean.columns = _columnNames
        dataClean["Date"]        = pd.to_datetime(dataClean["Date"],        format="%Y%m%d")
        dataClean["ReleaseDate"] = pd.to_datetime(dataClean["ReleaseDate"], format="%Y%m%d")

        ## Publication lag: days from end of reference month to release date
        ## This gives the actual UN Comtrade publication lag per observation.
        ## Median lag in weeks is stored as a class constant for the FeatureEngineer registry
        _refMonthEnd                = dataClean["Date"] + pd.offsets.MonthEnd(0)
        _pub_lag_days               = (dataClean["ReleaseDate"] - _refMonthEnd).dt.days
        self.export_pub_lag_weeks   = int(round(_pub_lag_days.median() / 7))

        ## Transform
        dataTransform          = dataClean.drop(columns=["ReleaseDate"]).copy()
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

        dataTransform = dataTransform.drop_duplicates(subset=["Year", "Month"]).reset_index(drop=True)

        return dataTransform

    ##
    #  Uploads, cleans and transforms the Biomass time series data
    #  From January 2000 to January 2026
    #  @dataset Directory of fisheries detailed biomass data
    #  @return  "panel" monthly production-area-level aquaculture data on stock, biomass,
    #           feed, harvest, and losses
    #  @note    UTSETT_SMOLT_STK = smolts RELEASED into sea cages this month (flow variable,
    #           fish <250g), not a stock count. Named Smolt_Releases accordingly
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
                        "Salmon_Biomass_Kg", "Salmon_Biomass_Smolt_Releases", "Salmon_Biomass_Feed_Kg", "Salmon_Biomass_Harvest_Kg",
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
    #  Uploads, cleans and transforms the Biomass data by age cohort
    #  From January 2000 to January 2026
    #  @dataset Directory of fisheries detailed biomass data
    #  @return  monthly national biomass and fish stock stratified by
    #           fish age (0, 1, 2+) derived from observation year - release year
    #
    def SalmonBiomassCohort(self):

        ## Clean
        _fileName = self.SALMON_BIOMASS
        _data     = pd.read_excel(_fileName, sheet_name="Biomasse-flk", skiprows=5)
        _data     = _data[_data[" ARTSID"] == "LAKS"].copy()
        _data     = _data.reset_index(drop=True)

        ## Compute fish age and bucket
        _data["Age"] = _data["ÅR"] - _data[" UTSETTSÅR"]
        _data        = _data[_data["Age"] >= 0]
        _data["AgeBucket"] = _data["Age"].clip(upper=2).astype(int)

        ## Aggregate: sum across counties per (Year, Month, AgeBucket)
        _agg = (
            _data
            .groupby(["ÅR", " MÅNED_KODE", "AgeBucket"], as_index=False)
            .agg({" BEHFISK_STK": "sum", " BIOMASSE_KG": "sum"})
        )

        _agg.columns = ["Year", "Month", "AgeBucket", "Fish_Stock", "Biomass_Kg"]

        ## Pivot age buckets into columns
        _ageLabels = {0: "Age0", 1: "Age1", 2: "Age2Plus"}
        _agg["AgeBucket"] = _agg["AgeBucket"].map(_ageLabels)

        dataTransform = _agg.pivot_table(
            index=["Year", "Month"],
            columns="AgeBucket",
            values=["Biomass_Kg", "Fish_Stock"],
            aggfunc="sum",
            fill_value=0
        )

        ## Flatten column names
        dataTransform.columns = [
            f"Salmon_Biomass_{val}_{age}_Monthly"
            for val, age in dataTransform.columns
        ]

        dataTransform = dataTransform.reset_index()
        dataTransform = dataTransform.astype({
            "Year": "int64", "Month": "int64"
        })

        for col in dataTransform.columns:
            if col not in ["Year", "Month"]:
                dataTransform[col] = dataTransform[col].astype("float64")

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
        ## Note: "Rømmings- estimat" (escape estimate) is excluded — it contains range strings
        ## (e.g. "1-10", "E:10-100") for preliminary estimates before official counts are filed.
        ## "Rapportert rømt" is the official reported count and is the correct value to use.
        ## Rømmings-estimat is retained as a potential future gap-filler (see pipeline notes).
        _selectColumns            = ["Dato", "Lokalitets- navn", "Lokalitets- nummer", "Fylke",
                                    "Selskap", "Art", "Rapportert rømt",
                                    "Snittvekt (gram)", "Gjenfangst"]
        dataClean                = _data[_selectColumns]
        dataClean.loc[:,"Dato"]  = pd.to_datetime(dataClean["Dato"], format = "%m/%d/%Y").dt.date
        dataClean                = dataClean.sort_values("Dato", ascending=True)
        dataClean                = dataClean.reset_index(drop = True)
        _columnNames             = ["Date", "Salmon_Escapes_Site_Name", "Salmon_Escapes_Site_Number",
                                    "Salmon_Escapes_County", "Salmon_Escapes_Company", "Salmon_Escapes_Species",
                                    "Salmon_Escapes_Rep_Escaped", "Salmon_Escapes_Avg_Wt_Grams",
                                    "Salmon_Escapes_Recapture"]
        dataClean.columns        = _columnNames
        dataClean                = dataClean[dataClean["Salmon_Escapes_Species"] == "Laks"]
        dataClean                = dataClean.reset_index(drop = True) 

        ## Transform
        dataTransform = dataClean.copy()
        dataTransform["Date"] = pd.to_datetime(dataTransform["Date"])
        dataTransform         = dataTransform.set_index("Date")
        _rep = dataTransform["Salmon_Escapes_Rep_Escaped"].astype(str)
        _range = _rep.str.extract(r"E:\s*(\d+)\s*-\s*(\d+)")
        _midpoint = (_range[0].astype(float) + _range[1].astype(float)) / 2
        _rep = _rep.str.replace("E:", "", regex=False)
        _rep = pd.to_numeric(_rep, errors="coerce")
        dataTransform["Salmon_Escapes_Rep_Escaped"] = _rep.fillna(_midpoint)
        dataTransform["Salmon_Escapes_Rep_Escaped"] = pd.to_numeric(dataTransform["Salmon_Escapes_Rep_Escaped"], errors="coerce")
        dataTransform["Salmon_Escapes_Avg_Wt_Grams"] = pd.to_numeric(dataTransform["Salmon_Escapes_Avg_Wt_Grams"], errors="coerce")

        ## Weighted average weight: weight each event's avg_wt by its escaped count
        ## Exclude events with missing weight so they don't inflate the denominator
        _hasWt = dataTransform["Salmon_Escapes_Avg_Wt_Grams"].notna()
        dataTransform["_rep_with_wt"] = dataTransform["Salmon_Escapes_Rep_Escaped"].where(_hasWt, 0)
        dataTransform["_wt_x_escaped"] = (
            dataTransform["Salmon_Escapes_Avg_Wt_Grams"] * dataTransform["Salmon_Escapes_Rep_Escaped"]
        )

        dataTransform        = dataTransform.resample("W-WED").agg({
                              "Salmon_Escapes_Rep_Escaped" : "sum",
                              "_rep_with_wt"               : "sum",
                              "_wt_x_escaped"              : "sum",
                              "Salmon_Escapes_Recapture"   : "sum"
                              })

        dataTransform["Salmon_Escapes_Avg_Wt_Grams"] = (
            dataTransform["_wt_x_escaped"] / dataTransform["_rep_with_wt"]
        )
        dataTransform = dataTransform.drop(columns=["_wt_x_escaped", "_rep_with_wt"])
        

        dataTransform          = dataTransform.reset_index()
        _colNames              = ["Date", "Salmon_Escapes_Rep_Escaped_Weekly", "Salmon_Escapes_Recapture_Weekly", "Salmon_Escapes_Avg_Wt_Grams_Weekly"]
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
    #  Uploads, cleans and transforms the national weekly sea lice time series
    #  From week 1 2012 to week 14 2026
    #  @dataset BarentsWatch national aggregation of site-level lice counts
    #           (source: Fiskeridirektoratet / Mattilsynet via BarentsWatch)
    #  @return  weekly national average female lice per fish, sea temperature at 3m,
    #           % sites above 0.5 limit, % sites with any treatment
    #
    def SalmonLice(self):

        ## Clean
        _fileName   = self.SALMON_LICE
        _data       = pd.read_excel(_fileName)
        # ICA column may not exist in older Excel files — add it as zero if missing
        if "ICA Count (ISA)" not in _data.columns:
            _data["ICA Count (ISA)"] = 0
        _selectCols = ["Year", "Week",
                       "Localities Reporting",
                       "Avg Adult Female Lice",
                       "Avg Sea Temp 3m (°C)",
                       "% Above Limit (0.5)",
                       "% Any Treatment",
                       "ICA Count (ISA)"]
        dataClean         = _data[_selectCols].copy()
        dataClean.columns = ["Year", "Week",
                             "Salmon_Lice_LocalitiesReporting_Weekly",
                             "Salmon_Lice_AvgFemale_Weekly",
                             "Salmon_SeaTemp_3m_Weekly",
                             "Salmon_Lice_PctAboveLimit_Weekly",
                             "Salmon_Lice_PctTreated_Weekly",
                             "Salmon_Lice_ICA_Count_Weekly"]

        ## Derive Month from ISO week
        dataClean["Month"] = pd.to_datetime(
            dataClean["Year"].astype(str)
            + "-W"
            + dataClean["Week"].astype(str).str.zfill(2)
            + "-1",
            format="%G-W%V-%u"
        ).dt.month

        ## Transform
        dataTransform = dataClean[["Year", "Week", "Month",
                                   "Salmon_Lice_LocalitiesReporting_Weekly",
                                   "Salmon_Lice_AvgFemale_Weekly",
                                   "Salmon_SeaTemp_3m_Weekly",
                                   "Salmon_Lice_PctAboveLimit_Weekly",
                                   "Salmon_Lice_PctTreated_Weekly",
                                   "Salmon_Lice_ICA_Count_Weekly"]].copy()
        dataTransform = dataTransform.astype({
            "Year"                                   : "int64",
            "Week"                                   : "int64",
            "Month"                                  : "int64",
            "Salmon_Lice_LocalitiesReporting_Weekly" : "int64",
            "Salmon_Lice_AvgFemale_Weekly"           : "float64",
            "Salmon_SeaTemp_3m_Weekly"               : "float64",
            "Salmon_Lice_PctAboveLimit_Weekly"        : "float64",
            "Salmon_Lice_PctTreated_Weekly"          : "float64",
            "Salmon_Lice_ICA_Count_Weekly"           : "float64",
        })

        assert not dataTransform.duplicated(subset=["Year", "Week"]).any(), \
            "SalmonLice has duplicate (Year, Week) rows"

        return dataTransform

    ##
    #  Counts the number of unique salmon localities with an active ILA
    #  (Infectious Salmon Anemia) case in each ISO week.
    #  A locality is "active" in week W if:
    #      Fra dato  ≤  last day of week W
    #      AND (Til dato is NaN  OR  Til dato ≥ first day of week W)
    #  @dataset BarentsWatch / Mattilsynet — ila_pd.csv (full event log)
    #  @return  weekly count of unique ILA localities (Year, Week, Month,
    #           Salmon_ILA_ActiveLocalities_Weekly)
    #
    def SalmonILA(self):

        _data = pd.read_csv(self.SALMON_ILA, sep=",", encoding="utf-8-sig", low_memory=False)

        ## Filter ILA only
        _ila = _data[_data["Sykdom"] == "ILA"].copy()

        ## Parse dates — Fra dato is always present; Til dato may be NaN (still active)
        _ila["Fra dato"] = pd.to_datetime(_ila["Fra dato"], errors="coerce")
        _ila["Til dato"] = pd.to_datetime(_ila["Til dato"], errors="coerce")

        ## Build a weekly calendar from the earliest event to today (Wednesday anchors)
        _minDate = _ila["Fra dato"].min()
        _weeks   = pd.date_range(
            start = _minDate - pd.Timedelta(days=_minDate.weekday()),  # back to Monday
            end   = pd.Timestamp.today(),
            freq  = "W-WED"
        )

        ## For each Wednesday, count unique localities active that week
        _records = []
        for _wed in _weeks:
            _mon = _wed - pd.Timedelta(days=2)   # Monday of the same ISO week
            _sun = _wed + pd.Timedelta(days=4)   # Sunday of the same ISO week

            _active = _ila[
                (_ila["Fra dato"] <= _sun) &
                (_ila["Til dato"].isna() | (_ila["Til dato"] >= _mon))
            ]["Lokalitetsnummer"].nunique()

            _iso   = _wed.isocalendar()
            _records.append({
                "Year":  int(_iso[0]),
                "Week":  int(_iso[1]),
                "Month": int(_wed.month),
                "Salmon_ILA_ActiveLocalities_Weekly": int(_active)
            })

        dataTransform = pd.DataFrame(_records)
        dataTransform = dataTransform.astype({
            "Year":  "int64",
            "Week":  "int64",
            "Month": "int64",
            "Salmon_ILA_ActiveLocalities_Weekly": "float64"
        })

        assert not dataTransform.duplicated(subset=["Year", "Week"]).any(), \
            "SalmonILA has duplicate (Year, Week) rows"

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
        _data          = _data.iloc[::-1]

        _dataM         = _data.drop(columns=_data.columns[1])
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
    #  @return  monthly  year-over-year % change in EU meat prices
    #
    def ProteinCPIMeat(self):
        
        ## Clean
        _fileName         = self.PROTEIN_CPI_MEAT
        _data             = pd.read_excel(_fileName, header = 0)
        dataClean         = _data.copy()
        dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")
        dataClean         = dataClean.sort_values("Date", ascending=True)
        dataClean         = dataClean.rename(columns = {"Last Price" : "Protein_Meat_Inflation_YoY_Monthly"})
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
                                "Protein_Meat_Inflation_YoY_Monthly": "float64"
                                })

        return dataTransform
    
    ##
    #  Uploads, cleans and transforms the Broiler Price EU time series data
    #  From 06 January 2012 to 09 February 2026
    #  @dataset from bloomberg ticker POPRBREU, source European Comission
    #  @return  weekly price in NOK per 100 kg (converted at actual observation-date
    #           EURNOK rate before resampling to Wednesday)
    #
    def ProteinBroilerPrice(self):

        _data = pd.read_excel(self.PROTEIN_PRICE_BROILER, header=0)
        _data["Date"] = pd.to_datetime(_data["Date"])
        _data = _data.sort_values("Date").reset_index(drop=True)
        _data = _data.rename(columns={"Last Price": "Broiler_EUR"})

        _eurnok = pd.read_excel(self.CURRENCY_EURNOK, header=0)
        _eurnok["Date"] = pd.to_datetime(_eurnok["Date"])
        _eurnok = _eurnok.sort_values("Date").reset_index(drop=True)
        _eurnok.columns = ["Date", "EURNOK"]

        _data = pd.merge_asof(_data, _eurnok, on="Date", direction="nearest")
        _data["Protein_Broiler_EUR_100_kg_Weekly"] = _data["Broiler_EUR"] * _data["EURNOK"]

        dataTransform = (
            _data[["Date", "Protein_Broiler_EUR_100_kg_Weekly"]]
            .set_index("Date")
            .resample("W-WED")
            .last()
            .reset_index()
        )

        dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
        dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform = dataTransform[["Year", "Week", "Month", "Protein_Broiler_EUR_100_kg_Weekly"]]
        dataTransform = dataTransform.astype({
            "Year": "int64", "Week": "int64", "Month": "int64",
            "Protein_Broiler_EUR_100_kg_Weekly": "float64"
        })

        return dataTransform

    ##
    #  Uploads, cleans and transforms the Pig Price EU time series data
    #  From 03 January 2014 to 27 February 2026
    #  @dataset from bloomberg ticker EUPGCEDE, source European Comission
    #  @return  weekly price in NOK per 100 kg (converted at actual observation-date
    #           EURNOK rate before resampling to Wednesday)
    #
    def ProteinPigPrice(self):

        _data = pd.read_excel(self.PROTEIN_PRICE_PIG, header=0)
        _data["Date"] = pd.to_datetime(_data["Date"])
        _data = _data.sort_values("Date").reset_index(drop=True)
        _data = _data.rename(columns={"Last Price": "Pig_EUR"})

        _eurnok = pd.read_excel(self.CURRENCY_EURNOK, header=0)
        _eurnok["Date"] = pd.to_datetime(_eurnok["Date"])
        _eurnok = _eurnok.sort_values("Date").reset_index(drop=True)
        _eurnok.columns = ["Date", "EURNOK"]

        _data = pd.merge_asof(_data, _eurnok, on="Date", direction="nearest")
        _data["Protein_Pig_EUR_100_kg_Weekly"] = _data["Pig_EUR"] * _data["EURNOK"]

        dataTransform = (
            _data[["Date", "Protein_Pig_EUR_100_kg_Weekly"]]
            .set_index("Date")
            .resample("W-WED")
            .last()
            .reset_index()
        )

        dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
        dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform = dataTransform[["Year", "Week", "Month", "Protein_Pig_EUR_100_kg_Weekly"]]
        dataTransform = dataTransform.astype({
            "Year": "int64", "Week": "int64", "Month": "int64",
            "Protein_Pig_EUR_100_kg_Weekly": "float64"
        })

        return dataTransform

    ##
    #  Uploads, cleans and transforms the EURNOK time series data
    #  From 07 January 2000 to 05 March 2026
    #  @dataset EURNOK Last price, source Bloomberg
    #  @return weekly EURNOK last price
    #
    def EURNOK(self):

        return self._loadWeekly(
                            self.CURRENCY_EURNOK,
                            "EURNOK_Weekly", freq = "weekly"
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
                                "USDNOK_Weekly", freq = "weekly"
                               )
    
    
    ##
    #  Uploads, cleans and transforms the Brent time series data
    #  From 07 January 2000 to 05 March 2026 ****
    #  @dataset Bloomberg ticker CO1, source ICE
    #  @return weekkly Brent last price per barrel in NOK
    #
    def CommodityBrentPrice(self):
        
        return self._loadWeekly(
            self.COMMODITY_BRENT,
            ["Commodity_Brent_COA_NOK_bbl_Weekly",
            "Commodity_Brent_CO1_NOK_bbl_Weekly",
            "Commodity_Brent_CO2_NOK_bbl_Weekly",
            "Commodity_Brent_CO3_NOK_bbl_Weekly",
            "Commodity_Brent_CO4_NOK_bbl_Weekly",
            "Commodity_Brent_CO5_NOK_bbl_Weekly",
            "Commodity_Brent_CO6_NOK_bbl_Weekly",
            "Commodity_Brent_CO12_NOK_bbl_Weekly"], freq= "daily"
        )

    ##
    #  Uploads, cleans and transforms the Wheat time series data
    #  From 07 January 2000 to 05 March 2026 ***
    #  @dataset Bloomberg ticker CA2, source EOP-Euronext Derivatives
    #  @return daily wheat last price per metric tone in NOK
    #
    def CommodityWheatPrice(self):

        return self._loadWeekly(
            self.COMMODITY_WHEAT,
            ["Commodity_Wheat_CAA_NOK_mt_Weekly",
            "Commodity_Wheat_CA1_NOK_mt_Weekly",
            "Commodity_Wheat_CA2_NOK_mt_Weekly",
            "Commodity_Wheat_CA3_NOK_mt_Weekly",
            "Commodity_Wheat_CA4_NOK_mt_Weekly",
            "Commodity_Wheat_CA5_NOK_mt_Weekly",
            "Commodity_Wheat_CA6_NOK_mt_Weekly",
            "Commodity_Wheat_CA12_NOK_mt_Weekly"], freq= "daily"
        )

    ##
    #  Uploads, cleans and transforms the Soybean time series data
    #  From 07 January 2000 to 05 March 2026 *****
    #  @dataset Bloomberg ticker SM1, source CBOT
    #  @return weekly soybean last price per short ton in NOK
    #
    def CommoditySoybeanPrice(self):

        return self._loadWeekly(
            self.COMMODITY_SOYBEAN,
            ["Commodity_Soybean_SMA_NOK_st_Weekly",
            "Commodity_Soybean_SM1_NOK_st_Weekly",
            "Commodity_Soybean_SM2_NOK_st_Weekly",
            "Commodity_Soybean_SM3_NOK_st_Weekly",
            "Commodity_Soybean_SM4_NOK_st_Weekly",
            "Commodity_Soybean_SM5_NOK_st_Weekly",
            "Commodity_Soybean_SM6_NOK_st_Weekly",
            "Commodity_Soybean_SM12_NOK_st_Weekly"], freq = "daily"
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
            ["Commodity_Rapeseed_IJA_NOK_mt_Weekly",
            "Commodity_Rapeseed_IJ1_NOK_mt_Weekly",
            "Commodity_Rapeseed_IJ2_NOK_mt_Weekly",
            "Commodity_Rapeseed_IJ3_NOK_mt_Weekly",
            "Commodity_Rapeseed_IJ4_NOK_mt_Weekly",
            "Commodity_Rapeseed_IJ5_NOK_mt_Weekly",
            "Commodity_Rapeseed_IJ6_NOK_mt_Weekly",
            "Commodity_Rapeseed_IJ12_NOK_mt_Weekly"], freq = "daily"
        )
    
    ##
    #  Uploads, cleans and transforms the Carbon time series data
    #  From 07 January 2000 to 05 March 2026
    #  @dataset Bloomberg ticker MOA, source EUA ETS System
    #  @return weekly carbon last price per metric ton in NOK
    #
    def CommodityCarbonPrice(self):

        return self._loadWeekly(
            self.COMMODITY_CARBON,
            ["Commodity_Carbon_MOA_NOK_mt_Weekly",
            "Commodity_Carbon_MO1_NOK_mt_Weekly",
            "Commodity_Carbon_MO2_NOK_mt_Weekly",
            "Commodity_Carbon_MO3_NOK_mt_Weekly",
            "Commodity_Carbon_MO4_NOK_mt_Weekly",
            "Commodity_Carbon_MO5_NOK_mt_Weekly",
            "Commodity_Carbon_MO6_NOK_mt_Weekly"], freq = "daily"
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
            "Equity_MOWI_NOK_Weekly", freq = "daily"
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
            "Equity_SALMAR_NOK_Weekly", freq = "daily"
        )

    ##
    #  Uploads, cleans and transforms the NIBOR 3m rate
    #  From January 2000 onwards — daily, no publication lag
    #  @dataset Bloomberg — NIBOR 3m interbank rate
    #  @return  weekly Wednesday NIBOR 3m rate (raw %)
    #
    def NIBOR3m(self):

        _data = pd.read_excel(self.SALMON_NIBOR, sheet_name="NIBOR3M", header=0)
        _data["Date"] = pd.to_datetime(_data["Date"])
        _data = _data.sort_values("Date").set_index("Date")
        _data = _data.rename(columns={"Last Price": "NIBOR_3m_Weekly"})

        dataTransform = (
            _data
            .resample("W-WED")
            .last()
            .reset_index()
        )

        dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
        dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform = dataTransform[["Year", "Week", "Month", "NIBOR_3m_Weekly"]]
        dataTransform = dataTransform.astype({
            "Year": "int64", "Week": "int64", "Month": "int64",
            "NIBOR_3m_Weekly": "float64"
        })

        return dataTransform

    ##
    #  Uploads, cleans and transforms Chilean salmon export volume
    #  Calendar-dated (~every 8 days), 1-week publication lag
    #  @dataset Chilean customs / Bloomberg
    #  @return  weekly export value in NOK millions (converted at actual observation-date
    #           FX rate before resampling to Wednesday, so price and FX always share the
    #           same raw date)
    #
    def SalmonChileExports(self):

        _data = pd.read_excel(self.SALMON_CHILE_EXPORTS, header=0)
        _data["Date"] = pd.to_datetime(_data["Date"])
        _data = _data.sort_values("Date").reset_index(drop=True)
        _data = _data.rename(columns={"Last Price": "Chile_USD"})

        ## Load USDNOK and convert at the actual observation date
        _usdnok = pd.read_excel(self.CURRENCY_USDNOK, header=0)
        _usdnok["Date"] = pd.to_datetime(_usdnok["Date"])
        _usdnok = _usdnok.sort_values("Date").reset_index(drop=True)
        _usdnok.columns = ["Date", "USDNOK"]

        _data = pd.merge_asof(
            _data.sort_values("Date"),
            _usdnok.sort_values("Date"),
            on="Date",
            direction="nearest"
        )
        _data["Salmon_Chile_Export_Volume_Weekly"] = _data["Chile_USD"] * _data["USDNOK"]

        dataTransform = (
            _data[["Date", "Salmon_Chile_Export_Volume_Weekly"]]
            .set_index("Date")
            .resample("W-WED")
            .last()
            .ffill(limit=1)
            .reset_index()
        )

        dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
        dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform = dataTransform[["Year", "Week", "Month", "Salmon_Chile_Export_Volume_Weekly"]]
        dataTransform = dataTransform.astype({
            "Year" : "int64",
            "Week" : "int64",
            "Month": "int64",
            "Salmon_Chile_Export_Volume_Weekly": "float64"
        })

        return dataTransform

    ##
    #  Uploads, cleans and transforms the Fishmeal Price time series
    #  From January 2000 to February 2026
    #  @dataset Bloomberg — monthly fishmeal spot price (end-of-month)
    #  @return  weekly fishmeal price in NOK/mt (converted at publication-date FX,
    #           then forward-filled so the NOK price stays flat within each month)
    #
    def CommodityFishmeelPrice(self):

        ## Clean
        _fileName = self.COMMODITY_FISHMEAL
        _data     = pd.read_excel(_fileName, header=0)
        dataClean = _data.copy()
        dataClean["Date"] = pd.to_datetime(dataClean["Date"])
        dataClean = dataClean.sort_values("Date").reset_index(drop=True)
        dataClean = dataClean.rename(columns={"Last Price": "Fishmeal_USD"})

        ## Load USDNOK and take the rate on the publication date (end-of-month)
        _usdnok = pd.read_excel(self.CURRENCY_USDNOK, header=0)
        _usdnok["Date"] = pd.to_datetime(_usdnok["Date"])
        _usdnok = _usdnok.sort_values("Date").reset_index(drop=True)
        _usdnok.columns = ["Date", "USDNOK"]

        ## Convert to NOK at the publication date using merge_asof
        dataClean = pd.merge_asof(
            dataClean.sort_values("Date"),
            _usdnok.sort_values("Date"),
            on="Date",
            direction="nearest"
        )
        dataClean["Commodity_Fishmeal_USD_mt_Weekly"] = dataClean["Fishmeal_USD"] * dataClean["USDNOK"]

        ## Transform: forward-fill monthly NOK price to weekly Wednesday
        dataTransform = (
            dataClean[["Date", "Commodity_Fishmeal_USD_mt_Weekly"]]
            .set_index("Date")
            .resample("W-WED")
            .ffill()
            .reset_index()
        )

        dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
        dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform = dataTransform[["Year", "Week", "Month", "Commodity_Fishmeal_USD_mt_Weekly"]]
        dataTransform = dataTransform.astype({
            "Year" : "int64",
            "Week" : "int64",
            "Month": "int64",
            "Commodity_Fishmeal_USD_mt_Weekly": "float64"
        })

        return dataTransform

    ##
    #  Uploads, cleans and transforms the Global Shrimp Price time series
    #  From January 1992 to February 2026
    #  @dataset FRED series PSHRIUSDM — IMF Primary Commodity Prices, Shrimp
    #  @return  weekly shrimp price in NOK/mt (converted at publication-date FX,
    #           then forward-filled so the NOK price stays flat within each month)
    #
    def ProteinGlobalShrimpPrice(self):

        ## Clean
        _fileName = self.PROTEIN_SHRIMP_PRICE
        _data     = pd.read_excel(_fileName, sheet_name="Monthly", header=0)
        dataClean = _data.copy()
        dataClean["observation_date"] = pd.to_datetime(dataClean["observation_date"])
        dataClean = dataClean.sort_values("observation_date").reset_index(drop=True)
        dataClean = dataClean.rename(columns={
            "observation_date": "Date",
            "PSHRIUSDM"       : "Shrimp_USD"
        })

        ## Load USDNOK and take the rate on the publication date
        _usdnok = pd.read_excel(self.CURRENCY_USDNOK, header=0)
        _usdnok["Date"] = pd.to_datetime(_usdnok["Date"])
        _usdnok = _usdnok.sort_values("Date").reset_index(drop=True)
        _usdnok.columns = ["Date", "USDNOK"]

        ## Convert to NOK at the publication date using merge_asof
        dataClean = pd.merge_asof(
            dataClean.sort_values("Date"),
            _usdnok.sort_values("Date"),
            on="Date",
            direction="nearest"
        )
        dataClean["Protein_Shrimp_USD_mt_Weekly"] = dataClean["Shrimp_USD"] * dataClean["USDNOK"]

        ## Transform: forward-fill monthly NOK price to weekly Wednesday
        dataTransform = (
            dataClean[["Date", "Protein_Shrimp_USD_mt_Weekly"]]
            .set_index("Date")
            .resample("W-WED")
            .ffill()
            .reset_index()
        )

        dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
        dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform = dataTransform[["Year", "Week", "Month", "Protein_Shrimp_USD_mt_Weekly"]]
        dataTransform = dataTransform.astype({
            "Year" : "int64",
            "Week" : "int64",
            "Month": "int64",
            "Protein_Shrimp_USD_mt_Weekly": "float64"
        })

        return dataTransform

    ##
    #  Uploads, cleans and transforms FishPool salmon forward prices
    #  Two source files: 2006-2025 (NOK/kg) and 2024-2026 (EUR/tonne).
    #  The EUR/tonne file is converted to NOK/kg using the EURNOK rate on each
    #  closing date, then the two files are merged into a single series.
    #  Old file takes precedence where both overlap (Sep 2024 – Jan 2025).
    #  @dataset FishPool — All Fish Pool forward prices
    #  @return  weekly M+1, M+3, M+6, M+12 salmon forward prices in NOK/kg
    #
    def SalmonForwardPrice(self):

        _horizons = {1: "M1", 3: "M3", 6: "M6", 12: "M12"}

        def _extractHorizons(df, value_col):
            df = df.copy()
            df["ClosingDate"] = pd.to_datetime(df["Closing Date"])
            df["Year"]        = df["Year"].astype(int)
            df["Month"]       = df["Month"].astype(int)
            df[value_col]     = pd.to_numeric(df[value_col], errors="coerce")

            df["Horizon"] = (
                (df["Year"]  - df["ClosingDate"].dt.year)  * 12
                + (df["Month"] - df["ClosingDate"].dt.month)
            )
            df = df[df["Horizon"].isin(_horizons.keys())]

            pivot = df.pivot_table(
                index="ClosingDate",
                columns="Horizon",
                values=value_col,
                aggfunc="last"
            )
            pivot.columns = [f"Salmon_Forward_{_horizons[h]}_Weekly" for h in pivot.columns]
            return pivot.reset_index()

        ## --- Load daily EURNOK rates for EUR/tonne → NOK/kg conversion ---
        _eurnok = pd.read_excel(self.CURRENCY_EURNOK, sheet_name="Sheet1", header=0)
        _eurnok["Date"] = pd.to_datetime(_eurnok["Date"])
        _eurnok = _eurnok.sort_values("Date").rename(columns={"Last Price": "EURNOK"})

        ## --- Old file: 2006–Jan 2025, values already in NOK/kg ---
        _dfOld = pd.read_csv(self.SALMON_FORWARD_OLD,
                             sep=";", skiprows=1, decimal=",", index_col=False)
        _dfOld = _dfOld.dropna(axis=1, how="all")
        _oldResult = _extractHorizons(_dfOld, "NOK Value")

        ## --- New file: Sep 2024–Mar 2026, values in EUR/tonne ---
        ##     Convert to NOK/kg: EUR/tonne × EURNOK / 1000
        _dfNew = pd.read_csv(self.SALMON_FORWARD_NEW,
                             sep=";", skiprows=1, decimal=",", index_col=False)
        _dfNew = _dfNew.dropna(axis=1, how="all")
        _dfNew["ClosingDate"] = pd.to_datetime(_dfNew["Closing Date"])
        _dfNew["Value"]       = pd.to_numeric(_dfNew["Value"], errors="coerce")

        _closingRates = pd.merge_asof(
            _dfNew[["ClosingDate"]].drop_duplicates().sort_values("ClosingDate"),
            _eurnok[["Date", "EURNOK"]],
            left_on="ClosingDate", right_on="Date", direction="backward"
        ).drop(columns="Date")

        _dfNew = _dfNew.merge(_closingRates, on="ClosingDate")
        _dfNew["Value"] = _dfNew["Value"] * _dfNew["EURNOK"] / 1000
        _newResult = _extractHorizons(_dfNew, "Value")

        ## --- Bloomberg file: M3 and M6 gap-fill (Jul 2024 onward, EUR/tonne) ---
        _xlBbg     = pd.read_excel(self.SALMON_FORWARD_BBG, sheet_name=None, header=0)
        _bbgResult = None
        _bbgSheets = {"M3": "JJCQ6", "M6": "JJCX6"}
        _bbgFrames = []
        for horizon, sheet in _bbgSheets.items():
            if sheet in _xlBbg:
                _s = _xlBbg[sheet].copy()
                _s["ClosingDate"] = pd.to_datetime(_s["Date"])
                _s["Value"]       = pd.to_numeric(_s["Last Price"], errors="coerce")
                _rates = pd.merge_asof(
                    _s[["ClosingDate"]].drop_duplicates().sort_values("ClosingDate"),
                    _eurnok[["Date", "EURNOK"]],
                    left_on="ClosingDate", right_on="Date", direction="backward"
                ).drop(columns="Date")
                _s = _s.merge(_rates, on="ClosingDate")
                _s[f"Salmon_Forward_{horizon}_Weekly"] = _s["Value"] * _s["EURNOK"] / 1000
                _bbgFrames.append(_s[["ClosingDate", f"Salmon_Forward_{horizon}_Weekly"]])
        if _bbgFrames:
            _bbgResult = _bbgFrames[0]
            for _f in _bbgFrames[1:]:
                _bbgResult = _bbgResult.merge(_f, on="ClosingDate", how="outer")

        ## --- Combine: old file → new CSV → Bloomberg (priority order) ---
        _fwdCols = [f"Salmon_Forward_{l}_Weekly" for l in ["M1", "M3", "M6", "M12"]]

        dataDaily = pd.merge(_oldResult, _newResult,
                             on="ClosingDate", how="outer",
                             suffixes=("_old", "_new"))
        dataDaily = dataDaily.sort_values("ClosingDate")

        for col in _fwdCols:
            dataDaily[col] = dataDaily[f"{col}_old"].fillna(dataDaily[f"{col}_new"])

        if _bbgResult is not None:
            dataDaily = dataDaily.merge(_bbgResult, on="ClosingDate", how="outer",
                                        suffixes=("", "_bbg"))
            for col in ["Salmon_Forward_M3_Weekly", "Salmon_Forward_M6_Weekly"]:
                if f"{col}_bbg" in dataDaily.columns:
                    dataDaily[col] = dataDaily[col].fillna(dataDaily[f"{col}_bbg"])

        dataDaily = dataDaily[["ClosingDate"] + _fwdCols]

        ## --- Resample to weekly Wednesday ---
        dataTransform = (
            dataDaily
            .set_index("ClosingDate")
            .resample("W-WED")
            .last()
            .reset_index()
        )
        dataTransform = dataTransform.rename(columns={"ClosingDate": "Date"})
        dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
        dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
        dataTransform["Month"] = dataTransform["Date"].dt.month

        dataTransform = dataTransform[["Year", "Week", "Month"] + _fwdCols]
        dataTransform = dataTransform.astype({
            "Year" : "int64",
            "Week" : "int64",
            "Month": "int64"
        })
        for col in _fwdCols:
            dataTransform[col] = dataTransform[col].astype("float64")

        return dataTransform

    ##
    #  Generic loader for Bloomberg-style time series
    #  Converts data to weekly frequency aligned to Wednesday
    #
    def _loadWeekly(self, fileName, columnName, freq="daily"):

        ## Read sheets
        _data = pd.read_excel(fileName, sheet_name=None, header=0)

        dataSheets = []

        for i, sheetData in enumerate(_data.values()):

            ## Clean
            dataClean = sheetData.copy()
            dataClean["Date"] = pd.to_datetime(dataClean["Date"], format="%Y-%m-%d")
            dataClean = dataClean.sort_values("Date", ascending=True)

            ## Column naming
            if isinstance(columnName, list):
                assert len(columnName) == len(_data), "Column names do not match number of sheets"
                columnCurrent = columnName[i]
            else:
                columnCurrent = columnName

            dataClean = dataClean.rename(columns={"Last Price": columnCurrent})

            ## Transform: align to weekly Wednesday
            if freq in ("daily", "weekly"):

                dataTransform = (
                    dataClean
                    .set_index("Date")
                    .resample("W-WED")
                    .last()
                    .reset_index()
                )

            elif freq == "monthly":

                dataTransform = (
                    dataClean
                    .set_index("Date")
                    .resample("W-WED")
                    .ffill()
                    .reset_index()
                )

            dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
            dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
            dataTransform["Month"] = dataTransform["Date"].dt.month

            dataTransform = dataTransform[
                ["Year", "Week", "Month"]
                + list(dataTransform.columns.drop(["Year", "Week", "Month"]))
            ]

            dataTransform = dataTransform.drop(columns=["Date"])

            dataTransform = dataTransform.astype({
                "Year": "int64",
                "Week": "int64",
                "Month": "int64",
                columnCurrent: "float64"
            })

            dataSheets.append(dataTransform)

        ## Merge sheets
        dataFinal = dataSheets[0]

        for df in dataSheets[1:]:
            dataFinal = pd.merge(
                dataFinal,
                df,
                on=["Year", "Week", "Month"],
                how="outer"
            )

        return dataFinal

    ##
    #   Merge everything
    #   @datasets retrieved from various providers in weekly and monthly conventions
    #   @return weekly observations per feature, containing full information
    #
    def Data(self):

        ## Load datasets
        _data        = self.SalmonPriceFishPool()
        _salmonsb    = self.SalmonPriceSSB()
        _escapes     = self.SalmonEscapes()

        _eurnok      = self.EURNOK()
        _usdnok      = self.USDNOK()

        _brent       = self.CommodityBrentPrice()
        _wheat       = self.CommodityWheatPrice()
        _soybean     = self.CommoditySoybeanPrice()
        _rapseed     = self.CommodityRapseedPrice()
        _carbon      = self.CommodityCarbonPrice()

        _mowi        = self.EquityMOWIPrice()
        _salmar      = self.EquitySALMARPrice()

        _cpi         = self.CPINorway()
        _broiler     = self.ProteinBroilerPrice()
        _pig         = self.ProteinPigPrice()
        _cpimeat     = self.ProteinCPIMeat()
        _biomass     = self.SalmonBiomass()
        _biomassCoh  = self.SalmonBiomassCohort()
        _exports     = self.SalmonExport()
        _lice        = self.SalmonLice()
        _ila         = self.SalmonILA()
        _shrimp      = self.ProteinGlobalShrimpPrice()
        _fishmeal    = self.CommodityFishmeelPrice()
        _nibor       = self.NIBOR3m()
        _chile       = self.SalmonChileExports()
        _forward     = self.SalmonForwardPrice()

        ## Create Date from FishPool (Wednesday of ISO week)
        _data["Date"] = pd.to_datetime(
            _data["Year"].astype(str)
            + "-W"
            + _data["Week"].astype(str).str.zfill(2)
            + "-3",
            format="%G-W%V-%u"
        )

        ## Determine earliest Biomass observation
        _biomassStart = pd.to_datetime(
            _biomass["Year"].astype(str)
            + "-"
            + _biomass["Month"].astype(str).str.zfill(2)
            + "-01"
        ).min()

        ## Create continuous weekly calendar.
        ## Start 4 weeks before 2000-01-05 so _lagByPublication can shift
        ## pre-calendar shrimp values (1992-1999) into the Jan 2000 rows.
        ## The warm-up rows are trimmed inside _lagByPublication.
        start = pd.Timestamp("1999-12-08")
        end   = pd.Timestamp("2026-03-31")

        calendar = pd.DataFrame({
            "Date": pd.date_range(start=start, end=end, freq="W-WED")
        })

        calendar["Year"]         = calendar["Date"].dt.isocalendar().year
        calendar["Week"]         = calendar["Date"].dt.isocalendar().week
        calendar["Month"]        = calendar["Date"].dt.month
        calendar["CalendarYear"] = calendar["Date"].dt.year

        ## Base dataset
        data = calendar.merge(
            _data.drop(columns=["Year","Week","Month"], errors="ignore"),
            on="Date",
            how="left"
        )

        ## Weekly merges (aligned by Date)
        for w in [
            _salmonsb, _escapes,
            _broiler, _pig,
            _eurnok, _usdnok,
            _brent, _wheat, _soybean, _rapseed, _carbon,
            _mowi, _salmar,
            _lice, _ila, _shrimp, _fishmeal, _nibor, _chile, _forward
        ]:

            w = w.copy()

            w["Date"] = pd.to_datetime(
                w["Year"].astype(str)
                + "-W"
                + w["Week"].astype(str).str.zfill(2)
                + "-3",
                format="%G-W%V-%u"
            )

            w = w.drop(columns=["Year","Week","Month"], errors="ignore")

            data = data.merge(
                w,
                on="Date",
                how="left"
            )

        ## Monthly merges (use calendar year, not ISO year)
        for m in [_cpi, _cpimeat, _biomass, _biomassCoh, _exports]:

            m = m.groupby(["Year","Month"], as_index=False).first()
            m = m.rename(columns={"Year": "CalendarYear"})

            data = data.merge(
                m,
                on=["CalendarYear","Month"],
                how="left",
                validate="many_to_one"
            )

        ## Sort dataset and drop helper column
        data = data.drop(columns=["CalendarYear"])
        data = data.sort_values("Date").reset_index(drop=True)

        ## Forwards fill only weekly price variables that can have genuine gaps
        ## (e.g. equity holidays). Commodity contracts are excluded — daily resample
        ## already handles holiday weeks via .last(), so ffill only creates spurious zeros.
        _fillCols = data.columns[
            data.columns.str.contains(
                "Equity|EURNOK|USDNOK|Protein_Broiler|Protein_Pig"
            )
        ]

        data[_fillCols] = data[_fillCols].ffill(limit=2)

        ## Time index
        data = data.reset_index(drop=True)
        data.insert(0, "t", range(len(data)))

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

        ## Key uniqueness (Date is the true key)
        print("\n--- KEY UNIQUENESS ---")
        _dupD = data["Date"].duplicated().sum()
        print("Duplicate Date :", _dupD)
        assert _dupD == 0

        ## Missing values
        print("\n--- MISSING VALUES (%) ---")
        _missing = (data.isna().mean() * 100).sort_values(ascending=False)
        print(_missing[_missing > 0].head(20))

        ## Monthly merge consistency
        print("\n--- MONTHLY MERGE CONSISTENCY ---")
        _monthlyCols = data.filter(like="_Monthly").columns

        if len(_monthlyCols) > 0:
            _calYear = data["Date"].dt.year
            _check = data.groupby([_calYear, "Month"])[_monthlyCols].nunique()
            _max = _check.max().max()
            print("Max unique values per month:", _max)
            assert _max <= 1
            print("Monthly merge consistency: OK")

        ## Numeric summary
        print("\n--- NUMERIC SUMMARY ---")
        _numeric = data.select_dtypes(include="number")
        print(_numeric.describe().T.head(10))

        ## Forward-fill staleness check
        print("\n--- FORWARD-FILL STALENESS ---")
        _fillCols = data.columns[
            data.columns.str.contains("Commodity|Equity|EURNOK|USDNOK|Protein_Broiler|Protein_Pig")
        ]
        for col in _fillCols:
            _maxRun = (data[col] == data[col].shift()).astype(int)
            _maxRun = _maxRun.groupby((_maxRun != _maxRun.shift()).cumsum()).sum().max()
            if _maxRun > 10:
                print(f"  WARNING: {col} has {_maxRun} consecutive identical values (possible stale fill)")

        ## Time continuity
        print("\n--- WEEK CONTINUITY ---")
        _weekDiff = data["Date"].diff().dropna()
        assert (_weekDiff == pd.Timedelta(days=7)).all()
        print("Week continuity: OK")

        ## Column duplication
        print("\n--- COLUMN DUPLICATION ---")
        _dupCols = data.columns[data.columns.duplicated()]
        print("Duplicate columns:", list(_dupCols))
        assert len(_dupCols) == 0

        print("\nDATA VALIDATION PASSED")