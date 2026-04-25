#  This module constructs the DataLoader class

##
# This module defines the DataLoader class, which is responsible for loading, cleaning, and transforming the various datasets used in the thesis
# Each method corresponds to a specific dataset and returns a clean DataFrame ready for analysis
# For the final DataFrame, each dataset is merged on common time dimensions, using weekly frequency aligned to Wednesdays (mid-week pricing)
# Reason for mid-week pricing is based on advice from industry experts, as wednsday's are peak trading days for salmon
# All other variables are transformed to match this frequency, aligned to closest (following) Wednsday, to avoid look-ahead bias in the final dataset
# Further adjustments are made in feature_engineering.py, to account for publication lags and other timing issues in the raw data
##

import pandas as pd
import numpy as np


class DataLoader:

    # File paths
    _salmon       = "Data/Salmon"
    _salmonEquity = "/Equity/"
    _salmonMarket = "/Market/"
    _protein      = "Data/Protein/"
    _currency     = "Data/Currency/"
    _commodity    = "Data/Commodity/"

    # Salmon datasets
    SALMON_PRICE_FISHPOOL  = _salmon + _salmonMarket + "Price_FishPool.xls"
    SALMON_PRICE_SSB       = _salmon + _salmonMarket + "Price_SSB.xlsx"
    SALMON_EXPORTS         = _salmon + _salmonMarket + "Exports.xlsx"
    SALMON_BIOMASS         = _salmon + _salmonMarket + "Biomass.xlsx"
    SALMON_LICE            = _salmon + _salmonMarket + "sealice_norway_weekly.xlsx"
    SALMON_ILA             = _salmon + _salmonMarket + "ila_pd.csv"
    CPI_NORWAY             = _salmon + _salmonMarket + "CPI_YOY.xlsx"
    PROTEIN_SHRIMP_PRICE   = _protein + "GlobalShrimpPrice.xlsx"
    PROTEIN_CPI_MEAT       = _protein + "CPI_Meat.xlsx"
    PROTEIN_PRICE_BROILER  = _protein + "Price_Broiler.xlsx"
    PROTEIN_PRICE_PIG      = _protein + "Price_Pig.xlsx"
    CURRENCY_EURNOK        = _currency + "EURNOK.xlsx"
    CURRENCY_USDNOK        = _currency + "USDNOK.xlsx"
    COMMODITY_BRENT        = _commodity + "Price_Brent.xlsx"
    COMMODITY_SOYBEAN      = _commodity + "Price_Soybean.xlsx"
    COMMODITY_FISHMEAL     = _commodity + "Price_Fishmeal.xlsx"
    SALMON_FORWARD_OLD     = _salmon + _salmonMarket + "Forwardprices_20062024.csv"
    SALMON_FORWARD_NEW     = _salmon + _salmonMarket + "Forwardprices_20252026.csv"
    SALMON_NIBOR           = _salmon + _salmonMarket + "NIBOR.xlsx"
    SALMON_CHILE_EXPORTS   = _salmon + _salmonMarket + "Exports_Chile.xlsx"

    # Salmon dataset (not currently included)
    EQUITY_PRICE_MOWI      = _salmon + _salmonEquity + "Price_MOWI.xlsx"
    EQUITY_PRICE_SALMAR    = _salmon + _salmonEquity + "Price_SALMAR.xlsx"

    # The class is for uploading, cleaning and transformations of the different datasets, which returns a dictionary of clean DataFrames
    def __init__(self):
        pass

    # Converts data to weekly frequency aligned to Wednesday (Mid-week pricing)
    def _loadWeekly(self, fileName, columnName, freq="daily"):

        # Read sheets
        _data = pd.read_excel(fileName, sheet_name=None, header=0)

        dataSheets = []

        for i, sheetData in enumerate(_data.values()):

            # Clean
            dataClean = sheetData.copy()
            dataClean["Date"] = pd.to_datetime(dataClean["Date"], format="%Y-%m-%d")
            dataClean = dataClean.sort_values("Date", ascending=True)

            # Column naming
            if isinstance(columnName, list):
                assert len(columnName) == len(_data), "Column names do not match number of sheets"
                columnCurrent = columnName[i]
            else:
                columnCurrent = columnName

            dataClean = dataClean.rename(columns={"Last Price": columnCurrent})

            # Transform: align to weekly Wednesday
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

        # Merge sheets
        dataFinal = dataSheets[0]

        for df in dataSheets[1:]:
            dataFinal = pd.merge(
                dataFinal,
                df,
                on=["Year", "Week", "Month"],
                how="outer"
            )

        return dataFinal
    
    # Returns weekly salmon price per kg, in NOK and EUR, starting from January 2006
    def SalmonPriceFishPool(self):

        # Load file and format (Clean)
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
        
        # Validate datatypes and frequency match (Transform)
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
    
    #  Returns weekly exported salmon tons and price per kilogram in NOK, starting from January 2000
    def SalmonPriceSSB(self):

        # Clean  
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

        # Transform
        dataTransform = dataClean.copy()
        dataTransform = dataTransform.astype({
                         "Year"                           : "int64",
                         "Week"                           : "int64",
                         "Month"                          : "int64",
                         "Salmon_Exported_Tons_SSB_Weekly": "float64",
                         "Salmon_NOK_kg_SSB_Weekly"       : "float64"
                         })
        
        return dataTransform

    # Returns panel of monthly production-area-level aquaculture data on stock, biomass, feed, harvest, and losses, starting from January 2000
    def SalmonBiomass(self):

        # Clean
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

        # Transform
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

    # Returns weekly lice data, starting from January 2012
    def SalmonLice(self):

        # Clean
        _fileName   = self.SALMON_LICE
        _data       = pd.read_excel(_fileName)
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

        # Derive Month from ISO week system
        dataClean["Month"] = pd.to_datetime(
            dataClean["Year"].astype(str)
            + "-W"
            + dataClean["Week"].astype(str).str.zfill(2)
            + "-1",
            format="%G-W%V-%u"
        ).dt.month

        # Transform
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

    # Returns weekly count of unique ILA localities, starting from January 2012
    def SalmonILA(self):

        _data = pd.read_csv(self.SALMON_ILA, sep=",", encoding="utf-8-sig", low_memory=False)

        # Filter ILA only
        _ila = _data[_data["Sykdom"] == "ILA"].copy()

        # Parse dates and handle errors by coercing to NaT
        _ila["Fra dato"] = pd.to_datetime(_ila["Fra dato"], errors="coerce")
        _ila["Til dato"] = pd.to_datetime(_ila["Til dato"], errors="coerce")

        # Build a weekly calendar from the earliest event to today
        _minDate = _ila["Fra dato"].min()
        _weeks   = pd.date_range(
            start = _minDate - pd.Timedelta(days=_minDate.weekday()),  # back to Monday
            end   = pd.Timestamp.today(),
            freq  = "W-WED"
        )

        # Count unique localities active per week
        _records = []
        for _wed in _weeks:
            _mon = _wed - pd.Timedelta(days=2)  
            _sun = _wed + pd.Timedelta(days=4)   

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

    # Returns Monthly YOY CPI in percentage, starting from January 1932
    def CPINorway(self):

        # Clean
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

        # Transform
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

    # Returns Monthly YOY CPI in percentage, starting from January 2012
    def ProteinCPIMeat(self):
        
        # Clean
        _fileName         = self.PROTEIN_CPI_MEAT
        _data             = pd.read_excel(_fileName, header = 0)
        dataClean         = _data.copy()
        dataClean["Date"] = pd.to_datetime(dataClean["Date"], format = "%Y-%m-%d")
        dataClean         = dataClean.sort_values("Date", ascending=True)
        dataClean         = dataClean.rename(columns = {"Last Price" : "Protein_Meat_Inflation_YoY_Monthly"})
        dataClean         = dataClean.reset_index(drop = True)

        # Transform
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
    
    # Returns weekly broiler price per 100 kg, converted to NOK, starting from January 2012
    def ProteinBroilerPrice(self):

        # Clean
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

        # Transform
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

    # Returns weekly pig price per 100 kg, converted to NOK, starting from January 2014
    def ProteinPigPrice(self):

        # Clean
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

        # Transform
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

    # Returns weekly (Every wednesday) EURNOK exchange rate, starting from January 2000
    def EURNOK(self):

        return self._loadWeekly(
                            self.CURRENCY_EURNOK,
                            "EURNOK_Weekly", freq = "weekly"
                            )

    # Returns weekly (Every wednesday) USDNOK exchange rate, starting from January 2000
    def USDNOK(self):

        return self._loadWeekly(
                                self.CURRENCY_USDNOK,
                                "USDNOK_Weekly", freq = "weekly"
                               )
    
    # Returns weekly Brent forward prices per barrel in NOK, starting from January 2000
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

    # Returns weekly soybean forward prices per short ton in NOK, starting from January 2000
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
    
    # Returns weekly share price for MOWI in NOK, starting from January 2000
    def EquityMOWIPrice(self):

        return self._loadWeekly(
            self.EQUITY_PRICE_MOWI,
            "Equity_MOWI_NOK_Weekly", freq = "daily"
        )

    # Returns weekly share price for SALMAR in NOK, starting from January 2000
    def EquitySALMARPrice(self):

        return self._loadWeekly(
            self.EQUITY_PRICE_SALMAR,
            "Equity_SALMAR_NOK_Weekly", freq = "daily"
        )

    # Return weekly (Every Wednsday) NIBOR 3m rate in percentage, starting from January 2000
    def NIBOR3m(self):

        # Clean
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

        # Transform
        dataTransform["Year"]  = dataTransform["Date"].dt.isocalendar().year
        dataTransform["Week"]  = dataTransform["Date"].dt.isocalendar().week
        dataTransform["Month"] = dataTransform["Date"].dt.month
        dataTransform = dataTransform[["Year", "Week", "Month", "NIBOR_3m_Weekly"]]
        dataTransform = dataTransform.astype({
            "Year": "int64", "Week": "int64", "Month": "int64",
            "NIBOR_3m_Weekly": "float64"
        })

        return dataTransform

    # Return weekly Chilean salmon export value, converted to NOK millions, starting from January 2000
    def SalmonChileExports(self):

        # Clean
        _data = pd.read_excel(self.SALMON_CHILE_EXPORTS, header=0)
        _data["Date"] = pd.to_datetime(_data["Date"])
        _data = _data.sort_values("Date").reset_index(drop=True)
        _data = _data.rename(columns={"Last Price": "Chile_USD"})

        # Load USDNOK and convert at the actual observation date
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

        # Transform
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

    # Return weekly fishmeal price, converted to NOK per metric ton, starting from January 2000
    def CommodityFishmeelPrice(self):

        # Clean
        _fileName = self.COMMODITY_FISHMEAL
        _data     = pd.read_excel(_fileName, header=0)
        dataClean = _data.copy()
        dataClean["Date"] = pd.to_datetime(dataClean["Date"])
        dataClean = dataClean.sort_values("Date").reset_index(drop=True)
        dataClean = dataClean.rename(columns={"Last Price": "Fishmeal_USD"})

        # Load USDNOK
        _usdnok = pd.read_excel(self.CURRENCY_USDNOK, header=0)
        _usdnok["Date"] = pd.to_datetime(_usdnok["Date"])
        _usdnok = _usdnok.sort_values("Date").reset_index(drop=True)
        _usdnok.columns = ["Date", "USDNOK"]

        # Convert to NOK
        dataClean = pd.merge_asof(
            dataClean.sort_values("Date"),
            _usdnok.sort_values("Date"),
            on="Date",
            direction="nearest"
        )
        dataClean["Commodity_Fishmeal_USD_mt_Weekly"] = dataClean["Fishmeal_USD"] * dataClean["USDNOK"]

        # Transform: forward-fill monthly NOK price to weekly Wednesday (Since price is only published monthly)
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

    # Return weekly shrimp price, converted to NOK per metric ton, starting from January 1992
    def ProteinGlobalShrimpPrice(self):

        # Clean
        _fileName = self.PROTEIN_SHRIMP_PRICE
        _data     = pd.read_excel(_fileName, sheet_name="Monthly", header=0)
        dataClean = _data.copy()
        dataClean["observation_date"] = pd.to_datetime(dataClean["observation_date"])
        dataClean = dataClean.sort_values("observation_date").reset_index(drop=True)
        dataClean = dataClean.rename(columns={
            "observation_date": "Date",
            "PSHRIUSDM"       : "Shrimp_USD"
        })

        # Load USDNOK
        _usdnok = pd.read_excel(self.CURRENCY_USDNOK, header=0)
        _usdnok["Date"] = pd.to_datetime(_usdnok["Date"])
        _usdnok = _usdnok.sort_values("Date").reset_index(drop=True)
        _usdnok.columns = ["Date", "USDNOK"]

        # Convert to NOK
        dataClean = pd.merge_asof(
            dataClean.sort_values("Date"),
            _usdnok.sort_values("Date"),
            on="Date",
            direction="nearest"
        )
        dataClean["Protein_Shrimp_USD_mt_Weekly"] = dataClean["Shrimp_USD"] * dataClean["USDNOK"]

        # Transform: forward-fill monthly NOK price to weekly Wednesday (since price is only published monthly)
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

    # Return weekly salmon forward price, for different horizons, per kg in NOK, starting from January 2006
    def SalmonForwardPrice(self):

        # Extracting different maturity horizons from the raw data
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

        # Load daily EURNOK rates for conversion
        _eurnok = pd.read_excel(self.CURRENCY_EURNOK, sheet_name="Sheet1", header=0)
        _eurnok["Date"] = pd.to_datetime(_eurnok["Date"])
        _eurnok = _eurnok.sort_values("Date").rename(columns={"Last Price": "EURNOK"})

        # Old file (2006–Sep 2024), with values already in NOK/kg
        _dfOld = pd.read_csv(self.SALMON_FORWARD_OLD,
                             sep=";", skiprows=1, decimal=",", index_col=False)
        _dfOld = _dfOld.dropna(axis=1, how="all")
        _oldResult = _extractHorizons(_dfOld, "NOK Value")

        # New file (Sep 2024–Mar 2026), with values in EUR/tonne, converted to NOK/kg
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

        # Combine the two datasets on ClosingDate, keeping all dates from both (Older file takes priority where both overlap)
        _fwdCols = [f"Salmon_Forward_{l}_Weekly" for l in ["M1", "M3", "M6", "M12"]]

        dataDaily = pd.merge(_oldResult, _newResult,
                             on="ClosingDate", how="outer",
                             suffixes=("_old", "_new"))
        dataDaily = dataDaily.sort_values("ClosingDate")

        for col in _fwdCols:
            dataDaily[col] = dataDaily[f"{col}_old"].fillna(dataDaily[f"{col}_new"])

        dataDaily = dataDaily[["ClosingDate"] + _fwdCols]

        # Resample to weekly Wednesday values, forward-filling where necessary (if there is dates without traded contracts)
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

    # Return a single merged dataset with all features, aligned on a weekly Wednesday calendar, starting from January 2000. Monthly features are forward-filled to each Wednesday until the next monthly observation.
    def Data(self):

        ## Load datasets
        _data        = self.SalmonPriceFishPool()
        _salmonsb    = self.SalmonPriceSSB()
        _eurnok      = self.EURNOK()
        _usdnok      = self.USDNOK()

        _brent       = self.CommodityBrentPrice()
        _soybean     = self.CommoditySoybeanPrice()

        _mowi        = self.EquityMOWIPrice()
        _salmar      = self.EquitySALMARPrice()

        _cpi         = self.CPINorway()
        _broiler     = self.ProteinBroilerPrice()
        _pig         = self.ProteinPigPrice()
        _cpimeat     = self.ProteinCPIMeat()
        _biomass     = self.SalmonBiomass()
        _lice        = self.SalmonLice()
        _ila         = self.SalmonILA()
        _shrimp      = self.ProteinGlobalShrimpPrice()
        _fishmeal    = self.CommodityFishmeelPrice()
        _nibor       = self.NIBOR3m()
        _chile       = self.SalmonChileExports()
        _forward     = self.SalmonForwardPrice()

        # Create Date from FishPool (Wednesday of ISO week structure)
        _data["Date"] = pd.to_datetime(
            _data["Year"].astype(str)
            + "-W"
            + _data["Week"].astype(str).str.zfill(2)
            + "-3",
            format="%G-W%V-%u"
        )

        # Determine earliest Biomass observation
        _biomassStart = pd.to_datetime(
            _biomass["Year"].astype(str)
            + "-"
            + _biomass["Month"].astype(str).str.zfill(2)
            + "-01"
        ).min()

        # Create continuous weekly calendar, starting 4 weeks before 2000-01-05 to account for publication lags (Warm up rows are trimmed inside _lagByPublication)
        start = pd.Timestamp("1999-12-08")
        end   = pd.Timestamp("2026-04-01")

        calendar = pd.DataFrame({
            "Date": pd.date_range(start=start, end=end, freq="W-WED")
        })

        calendar["Year"]         = calendar["Date"].dt.isocalendar().year
        calendar["Week"]         = calendar["Date"].dt.isocalendar().week
        calendar["Month"]        = calendar["Date"].dt.month
        calendar["CalendarYear"] = calendar["Date"].dt.year

        # Base dataset
        data = calendar.merge(
            _data.drop(columns=["Year","Week","Month"], errors="ignore"),
            on="Date",
            how="left"
        )

        # Weekly merges (aligned by Date)
        for w in [
            _salmonsb,
            _broiler, _pig,
            _eurnok, _usdnok,
            _brent, _soybean,
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

        # Monthly merges (use calendar year, not ISO year structure)
        for m in [_cpi, _cpimeat, _biomass]:

            m = m.groupby(["Year","Month"], as_index=False).first()
            m = m.rename(columns={"Year": "CalendarYear"})

            data = data.merge(
                m,
                on=["CalendarYear","Month"],
                how="left",
                validate="many_to_one"
            )

        # Sort dataset and drop helper column
        data = data.drop(columns=["CalendarYear"])
        data = data.sort_values("Date").reset_index(drop=True)

        # Forwards fill only weekly price variables that can have genuine gaps (e.g. days/weeks without trading due to holidays etc.)
        _fillCols = data.columns[
            data.columns.str.contains(
                "Equity|EURNOK|USDNOK|Protein_Broiler|Protein_Pig"
            )
        ]

        data[_fillCols] = data[_fillCols].ffill(limit=2)

        # Time index
        data = data.reset_index(drop=True)
        data.insert(0, "t", range(len(data)))

        return data
    
    # Validates data and return diagnostic output and assertions
    def ValidateData(self, data):

        print("\n--- DATASET STRUCTURE ---")
        print("Rows   :", len(data))
        print("Cols   :", len(data.columns))

        assert "Year" in data.columns
        assert "Week" in data.columns
        assert "Month" in data.columns
        assert "Date" in data.columns

        print("\n--- TIME ORDER CHECK ---")
        assert data["Date"].is_monotonic_increasing
        print("Date ordering: OK")

        print("\n--- KEY UNIQUENESS ---")
        _dupD = data["Date"].duplicated().sum()
        print("Duplicate Date :", _dupD)
        assert _dupD == 0

        print("\n--- MISSING VALUES (%) ---")
        _missing = (data.isna().mean() * 100).sort_values(ascending=False)
        print(_missing[_missing > 0].head(20))

        print("\n--- MONTHLY MERGE CONSISTENCY ---")
        _monthlyCols = data.filter(like="_Monthly").columns

        if len(_monthlyCols) > 0:
            _calYear = data["Date"].dt.year
            _check = data.groupby([_calYear, "Month"])[_monthlyCols].nunique()
            _max = _check.max().max()
            print("Max unique values per month:", _max)
            assert _max <= 1
            print("Monthly merge consistency: OK")

        print("\n--- NUMERIC SUMMARY ---")
        _numeric = data.select_dtypes(include="number")
        print(_numeric.describe().T.head(10))

        print("\n--- FORWARD-FILL STALENESS ---")
        _fillCols = data.columns[
            data.columns.str.contains("Commodity|Equity|EURNOK|USDNOK|Protein_Broiler|Protein_Pig")
        ]
        for col in _fillCols:
            _maxRun = (data[col] == data[col].shift()).astype(int)
            _maxRun = _maxRun.groupby((_maxRun != _maxRun.shift()).cumsum()).sum().max()
            if _maxRun > 10:
                print(f"  WARNING: {col} has {_maxRun} consecutive identical values (possible stale fill)")

        print("\n--- WEEK CONTINUITY ---")
        _weekDiff = data["Date"].diff().dropna()
        assert (_weekDiff == pd.Timedelta(days=7)).all()
        print("Week continuity: OK")

        print("\n--- COLUMN DUPLICATION ---")
        _dupCols = data.columns[data.columns.duplicated()]
        print("Duplicate columns:", list(_dupCols))
        assert len(_dupCols) == 0

        print("\nDATA VALIDATION PASSED")