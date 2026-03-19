##
#  This module engineers the features constructed on selected variables
#

##
#  Import necessary libraries
#
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats

##
#  This class composes the feature engineering, 
#  constructs variables used by the models
#

class featureEngineer:

        ## Defines variables to lag
    _zeroWeek = {
        "EURNOK_Weekly":                              0,   # Last Friday's closing rate, available by Wednesday
        "USDNOK_Weekly":                              0,   # Last Friday's closing rate, available by Wednesday
        "Protein_Pig_EUR_100_kg_Weekly":              0,   # Last Friday's price, available by Wednesday
        "Equity_MOWI_NOK_Weekly":                     0,
        "Equity_SALMAR_NOK_Weekly":                   0,
        "Commodity_Brent_COA_NOK_bbl_Weekly":         0,   # Data available every wednesday
        "Commodity_Brent_CO1_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO2_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO3_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO4_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO5_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO6_NOK_bbl_Weekly":         0,
        "Commodity_Wheat_CAA_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA1_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA2_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA3_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA4_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA5_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA6_NOK_mt_Weekly":          0,
        "Commodity_Soybean_SMA_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM1_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM2_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM3_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM4_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM5_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM6_NOK_st_Weekly":        0,
        "Commodity_Rapeseed_IJA_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ1_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ2_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ3_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ4_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ5_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ6_NOK_mt_Weekly":       0,
        "Commodity_Carbon_MOA_NOK_mt_Weekly":         0,
        "Commodity_Carbon_MO1_NOK_mt_Weekly":         0,
        "Commodity_Carbon_MO2_NOK_mt_Weekly":         0,
        "Commodity_Carbon_MO3_NOK_mt_Weekly":         0,
        "Commodity_Carbon_MO4_NOK_mt_Weekly":         0,
        "Commodity_Carbon_MO5_NOK_mt_Weekly":         0,
        "Commodity_Carbon_MO6_NOK_mt_Weekly":         0}

    _oneWeek  = {
        "Salmon_NOK_kg_SSB_Weekly":                   1,   # SSB publishes following week
        "Salmon_Exported_Tons_SSB_Weekly":            1,
        "Protein_Broiler_EUR_100_kg_Weekly":          1,   # This week's Friday price → confirmed next Wednesday
        "Salmon_Escapes_Rep_Escaped_Weekly":          1,   # 24h statutory reporting window
        "Salmon_Escapes_Recapture_Weekly":            1,
        "Salmon_Escapes_Avg_Wt_Grams_Weekly":         1}

    _fourWeek = {
        "CPI_Norway_Monthly":                         4,   # SSB releases ~10th of following month; median 25 days
        "Protein_Meat_Inflation_YoY_Monthly":         4}   # Eurostat HICP; same schedule as CPI; median 25 days

    _fiveWeek = {
        "Salmon_Biomass_Fish_Stock_Monthly":          5,   # Fiskeridirektoratet biomass panel; median 35 days
        "Salmon_Biomass_Kg_Monthly":                  5,
        "Salmon_Biomass_Smolt_Releases_Monthly":      5,
        "Salmon_Biomass_Feed_Kg_Monthly":             5,
        "Salmon_Biomass_Harvest_Kg_Monthly":          5,
        "Salmon_Biomass_Harvest_N_Monthly":           5,
        "Salmon_Biomass_Mortality_N_Monthly":         5,
        "Salmon_Biomass_Discard_N_Monthly":           5,
        "Salmon_Biomass_Escape_N_Monthly":            5,
        "Salmon_Biomass_Other_Loss_N_Monthly":        5,
        "Salmon_Biomass_Biomass_Kg_Age0_Monthly":     5,
        "Salmon_Biomass_Biomass_Kg_Age1_Monthly":     5,
        "Salmon_Biomass_Biomass_Kg_Age2Plus_Monthly": 5,
        "Salmon_Biomass_Fish_Stock_Age0_Monthly":     5,
        "Salmon_Biomass_Fish_Stock_Age1_Monthly":     5,
        "Salmon_Biomass_Fish_Stock_Age2Plus_Monthly": 5}

    _eightWeek = {
        "Salmon_Export_Net_Weight_Kg_Monthly":        8,   # UN Comtrade; median 55 days from reference Wednesday
        "Salmon_Export_Value_USD_Monthly":            8,
        "Salmon_Export_Avg_Price_USD_Kg_Monthly":     8}

    PUBLISH_LAG_WEEKS = _zeroWeek | _oneWeek | _fourWeek | _fiveWeek | _eightWeek

    def __init__(self):
        pass

    ##
    #   Apply publication lags to time series features
    #   @data dataset containing multiple features with different publication timings
    #   @return DataFrame where each feature is shifted according to its publication delay (in weeks),
    #           ensuring that only information available at time t is used (no look-ahead bias)
    #
    def _lagByPublication(self, data: pd.DataFrame) -> pd.DataFrame:

        _publishLag = self.PUBLISH_LAG_WEEKS

        for col, lag in _publishLag.items():
            if col in data.columns and lag > 0:
                data[col] = data[col].shift(lag)

        return data
