#  This module engineers the features constructed on selected variables

##
# This module defines the FeatureEngineer class, which is responsible for making the variables and final dataset to be used in modelling
# Data sources had a huge variation in setup, publication days, frequencies etc., making it crucial to carefully align all variables to avoid look-ahead bias
# We first account for publication lags, and validated that these are correct
# Then we construct the variables to make the final dataset for modelling
# This is mostly based on what data was available to us, but also guided by some general assumptions we had about the price formation process and what information would be relevant for forecasting salmon prices
# Among other things, this included lagging several variables. As a biological commodity, our assumption is that Salmon prices are affected by short- and long-term supply and demand shocks
# As well as the fish following natural growth and life-cycle patterns, which makes variables relevant at different lags and forecast horizons
##

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from pandas.tseries.offsets import MonthEnd


# Shifts variables based on publication lag of the different datasets
# By default, data dated last day of the week (i.e. friday), is already pushed to following Wednesday
# The naming is a bit inconsistent, as it stems from a early draft. We have decided to keep it as is, to avoid changing something and messing up the code
# Naming is simplified later in the code
class FeatureEngineer:

    _zeroWeek = {
        "Protein_Pig_EUR_100_kg_Weekly":             -1, # Bloomberg gives data as weekly, meaning Friday close, but price is actually released on Wednesday, by the original source
        "EURNOK_Weekly":                              0,
        "USDNOK_Weekly":                              0,
        "NIBOR_3m_Weekly":                            0,
        "Protein_Broiler_EUR_100_kg_Weekly":          0,   
        "Equity_MOWI_NOK_Weekly":                     0,
        "Equity_SALMAR_NOK_Weekly":                   0,
        "Commodity_Brent_CO1_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO6_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO12_NOK_bbl_Weekly":        0,
        "Commodity_Soybean_SM1_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM6_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM12_NOK_st_Weekly":       0,
        "Salmon_Forward_M1_Weekly":                   0,
        "Salmon_Forward_M3_Weekly":                   0,
        "Salmon_Forward_M6_Weekly":                   0,
        "Salmon_Forward_M12_Weekly":                  0}

    _oneWeek  = {
        "Salmon_NOK_kg_SSB_Weekly":                   1,   #SSB: Published following Wednesday
        "Salmon_Exported_Tons_SSB_Weekly":            1,  
        "Salmon_Lice_AvgFemale_Weekly":               1,    #BarentsWatch: Published no later than following Wednsday, most times earlier
        "Salmon_SeaTemp_3m_Weekly":                   1,
        "Salmon_Lice_ICA_Count_Weekly":               1,   
        "Salmon_ILA_ActiveLocalities_Weekly":         1,   #Mattilsynet: Published as soon as information is available, but reporting usually takes 2-3 days
        "Salmon_Chile_Export_Volume_Weekly":          1}   #Chilean customs: Published no later than following Wednesday
    
    _fourWeek = {
        "Protein_Shrimp_USD_mt_Weekly":               4,    #IMF/FRED: End-of-Month data is published within 4 weeks
        "Commodity_Fishmeal_USD_mt_Weekly":           4,}   #Bloomberg: End-of-Month data is published within 4 weeks

    PUBLISH_LAG_WEEKS = _zeroWeek | _oneWeek | _fourWeek


    # Function is used to lag variables where the data is released on a given day of the month (Based on Excel's EOMONTH function)
    # Variables are then snapped to the first Wednesday following that day
    # Note: There may be occations where the release date is a weekend, in which case data is released following Monday.
    # As we snap to the first Wednesday anyways, this doesn't cause any issues
    PUBLISH_LAG_EOMONTH = {
        # SSB releases ~10th of following month
        "CPI_Norway_Monthly":                         10, #SSB: CPI is released on the 10th of the following month
        "Protein_Meat_Inflation_YoY_Monthly":         10, #Eurostat HICP: Follows same pattern as SSB

        # Fiskeridirektoratet: Biomass panel released on the 20th of following month
        "Salmon_Biomass_Fish_Stock_Monthly":          20,
        "Salmon_Biomass_Kg_Monthly":                  20,
        "Salmon_Biomass_Smolt_Releases_Monthly":      20,
        "Salmon_Biomass_Feed_Kg_Monthly":             20,
        "Salmon_Biomass_Harvest_Kg_Monthly":          20,
        "Salmon_Biomass_Harvest_N_Monthly":           20,
        "Salmon_Biomass_Mortality_N_Monthly":         20,
        "Salmon_Biomass_Discard_N_Monthly":           20,
        "Salmon_Biomass_Escape_N_Monthly":            20,
        "Salmon_Biomass_Other_Loss_N_Monthly":        20,

    }

    def __init__(self):
        pass

    # Shifts all variables by their given publication lag, giving us a DataFrame without look-ahead bias
    def _lagByPublication(self, data: pd.DataFrame) -> pd.DataFrame:

        # Find dates
        if "Date" in data.columns:
            dates = pd.to_datetime(data["Date"]).reset_index(drop=True)
        elif isinstance(data.index, pd.DatetimeIndex):
            dates = data.index.to_series().reset_index(drop=True)
        else:
            raise ValueError("DataFrame must have a 'Date' column or a DatetimeIndex.")

        # Weekly lags (shift by fixed number of rows, one per weekly lag)
        for col, lag in self.PUBLISH_LAG_WEEKS.items():
            if col in data.columns and lag != 0:
                data[col] = data[col].shift(lag)

        # Monthly lags (Move to last days of the month, adds N days according to publication schedule, and snaps to following Wednesday)
        for col, days_after in self.PUBLISH_LAG_EOMONTH.items():
            if col not in data.columns:
                continue

            schedule = (
                pd.DataFrame({
                    "month_end" : dates + MonthEnd(0),
                    "value"     : data[col].values,
                })
                .dropna(subset=["value"])
                .drop_duplicates(subset=["month_end"])
                .sort_values("month_end")
                .reset_index(drop=True)
            )

            raw_pub          = schedule["month_end"] + pd.Timedelta(days=days_after)
            days_to_wed      = (2 - raw_pub.dt.dayofweek) % 7          # 0 if already Wednesday
            schedule["publish_wed"] = raw_pub + pd.to_timedelta(days_to_wed, unit="D")

            row_frame = (
                pd.DataFrame({"row_date": dates, "orig_order": range(len(dates))})
                .sort_values("row_date")
            )

            merged = pd.merge_asof(
                row_frame,
                schedule[["publish_wed", "value"]].rename(columns={"publish_wed": "row_date"}),
                on="row_date",
                direction="backward",
            )

            # Restore original row order and assign back dates
            merged  = merged.sort_values("orig_order")
            data[col] = merged["value"].values

        # Trim the 4-week warm-up buffer (to avoid missing rows) added by Data() and reset the time index
        data = data[data["Date"] >= pd.Timestamp("2000-01-05")].reset_index(drop=True)
        data["t"] = range(len(data))

        return data

    # Validates that publication lags where applied correctly 
    # For weekly variables, checks that the number of leading NaN rows are correct
    # For monthly variables, finds value transition points in the lagged series, back-calculates the reference month from each transition date,
    # and verifies the transition falls on the correct publish Wednesday
    def validatePublicationLags(self, data: pd.DataFrame) -> None:

        # Find dates
        if "Date" in data.columns:
            dates = pd.to_datetime(data["Date"]).reset_index(drop=True)
        elif isinstance(data.index, pd.DatetimeIndex):
            dates = data.index.to_series().reset_index(drop=True)
        else:
            raise ValueError("DataFrame must have a 'Date' column or a DatetimeIndex.")

        SEP = "═" * 72

        print(f"\n{SEP}")
        print(f"  PUBLICATION LAG VALIDATION REPORT")
        print(f"{SEP}")

        # Weekly lags
        print(f"\n── WEEKLY LAGS (fixed row shift) {'─'*39}")
        print(f"\n  {'Column':<47} {'Lag':>3}  {'Leading NaNs':>13}  {'First value on':<16} Status")
        print(f"  {'─'*47} {'───':>3}  {'─────────────':>13}  {'─'*16} ──────")

        for col, lag in self.PUBLISH_LAG_WEEKS.items():
            if col not in data.columns or lag == 0:
                continue

            series = data[col].reset_index(drop=True)

            leading_nans = int(series.isna().cumprod().sum())
            first_val_date = (dates.iloc[leading_nans].date()
                              if leading_nans < len(dates) else "N/A")

            if leading_nans >= lag:
                status = "✓ PASS"
            else:
                status = "✗ TOO FEW NaNs (look-ahead?)"

            col_disp = (col[:45] + "..") if len(col) > 47 else col
            print(f"  {col_disp:<47} {lag:>3}  {leading_nans:>6} (exp≥{lag:>2})  "
                  f"{str(first_val_date):<16} {status}")

        # Monthly lags
        print(f"\n── MONTHLY LAGS (EOMONTH + N days, snapped to Wednesday) {'─'*16}")
        print(f"   Back-calculates reference month from each transition date.\n")
        print(f"  {'Column':<47} {'Offset':>7}  {'Ref Month':>9}  "
              f"{'Exp Pub Wed':>11}  {'Actual Trans':>12}  Status")
        print(f"  {'─'*47} {'───────':>7}  {'─'*9}  {'─'*11}  {'─'*12}  ──────")

        for col, days_after in self.PUBLISH_LAG_EOMONTH.items():
            if col not in data.columns:
                continue

            series   = data[col].reset_index(drop=True)
            col_disp = (col[:45] + "..") if len(col) > 47 else col
            offset_str = f"+{days_after}d"

            # Collect value transitions
            transitions = []
            prev_val = np.nan
            for i in range(len(series)):
                v = series.iloc[i]
                if pd.isna(v):
                    prev_val = np.nan
                    continue
                if pd.isna(prev_val) or not np.isclose(v, prev_val):
                    if not pd.isna(prev_val):          # skip the very first appearance
                        transitions.append((dates.iloc[i], v))
                prev_val = v

            if not transitions:
                print(f"  {col_disp:<47} {offset_str:>7}  ── no transitions found ──")
                continue

            # Sample one transition from the middle of the series
            trans_date, _ = transitions[len(transitions) // 2]

            # Back-calculate reference month end
            upper             = trans_date - pd.Timedelta(days=days_after)
            month_end_upper   = upper + MonthEnd(0)   # end of upper's own month (≥ upper)
            if month_end_upper <= upper:
                ref_month_end = month_end_upper
            else:
                ref_month_end = pd.Timestamp(upper.year, upper.month, 1) - pd.Timedelta(days=1)

            # Derive expected publish Wednesday
            raw_pub     = ref_month_end + pd.Timedelta(days=days_after)
            days_to_wed = (2 - raw_pub.dayofweek) % 7
            exp_pub_wed = raw_pub + pd.Timedelta(days=days_to_wed)

            ref_month_str = ref_month_end.strftime("%Y-%m")
            delta         = (trans_date - exp_pub_wed).days

            if delta == 0:
                status = "✓ PASS"
            elif delta < 0:
                status = f"✗ EARLY by {-delta}d"
            else:
                status = f"✗ LATE by {delta}d"

            print(f"  {col_disp:<47} {offset_str:>7}  {ref_month_str:>9}  "
                  f"{str(exp_pub_wed.date()):>11}  {str(trans_date.date()):>12}  {status}")

        print(f"\n{SEP}\n")
 

    # Building the feature matrix used for the forecasting models
    def buildFeatureMatrix(self,
                           data: pd.DataFrame) -> pd.DataFrame:

        data = self._lagByPublication(data)
        self.validatePublicationLags(data)

        df   = data.copy()
        spot = df["Salmon_NOK_kg_FP_Weekly"]

        # Log level — returns NaN where input is zero, negative, or missing
        # As CATBoost natively handles NaNs, we would rather have NaN than a zero value (which is not actually zero, but just missing/invalid data)
        def _ln(s):
            s      = pd.to_numeric(s, errors="coerce")
            result = np.full(len(s), np.nan)
            mask   = s.values > 0
            result[mask] = np.log(s.values[mask])
            return pd.Series(result, index=s.index)

        # Week-over-week log return
        def _dln(s):
            return _ln(s).diff()

        out = pd.DataFrame({"Date": df["Date"]})

        # Target construction: Cumulative log returns over each horzon
        # Y 0w is included as a "nowcast" (Inclusion is based on today's price is not published until the following week)
        # For the other we use cumulative log returns, which have the added benefit of being easily transferred back to the raw price levels
        # Given we are in week t, our most recent price is from t-1, and therefore price change from t-1 to t (dln[t]) is unknown at time of prediction and can be included as part of the target.
        _dln_spot = _dln(spot)
        out["Y 0w ∆ Salmon (NOK/KG)"]  = _dln_spot
        out["Y 1w ∆ Salmon (NOK/KG)"]  = _dln_spot.rolling(2).sum().shift(-1)
        out["Y 2w ∆ Salmon (NOK/KG)"]  = _dln_spot.rolling(3).sum().shift(-2)
        out["Y 1m ∆ Salmon (NOK/KG)"]  = _dln_spot.rolling(5).sum().shift(-4)
        out["Y 3m ∆ Salmon (NOK/KG)"]  = _dln_spot.rolling(14).sum().shift(-13)
        out["Y 6m ∆ Salmon (NOK/KG)"]  = _dln_spot.rolling(27).sum().shift(-26)
        out["Y 12m ∆ Salmon (NOK/KG)"] = _dln_spot.rolling(53).sum().shift(-52)

        # HAR-style Δln spot lags: avg over 1w, 2w, 1m, 3m, 6m, 12m
        # Uses rolling means, consistent with Corsi (2009) realised volatility paper
        # Shifted by 1 week to avoid look-ahead bias
        out["∆ Salmon (NOK/KG) 1w"]  = _dln_spot.rolling(1).mean().shift(1)
        out["∆ Salmon (NOK/KG) 2w"]  = _dln_spot.rolling(2).mean().shift(1)
        out["∆ Salmon (NOK/KG) 1m"]  = _dln_spot.rolling(4).mean().shift(1)
        out["∆ Salmon (NOK/KG) 3m"]  = _dln_spot.rolling(13).mean().shift(1)
        out["∆ Salmon (NOK/KG) 6m"]  = _dln_spot.rolling(26).mean().shift(1)
        out["∆ Salmon (NOK/KG) 12m"] = _dln_spot.rolling(52).mean().shift(1)

        # Realized (rolling) volatility
        out["RVol 4w"]  = _dln_spot.rolling(4).std().shift(1) # Fast signal for onset of volatility
        out["RVol 13w"] = _dln_spot.rolling(13).std().shift(1) # Medium signal for sustained high-vol window (captures typical seasonal volatility peaks)
        out["RVol 52w"] = _dln_spot.rolling(52).std().shift(1) # Slow signal for overall vol regime

        # FP–SSB spread and log-change: For consistency SSB lag is removed to compute the spread, and instead the spread is lagged 1-week
        if "Salmon_NOK_kg_SSB_Weekly" in df.columns:
            _ssb_contemp = df["Salmon_NOK_kg_SSB_Weekly"].shift(-1)
            out["Spread (FP - SSB)"]   = (spot - _ssb_contemp).shift(1)
            out["∆ Spread (FP - SSB)"] = (spot - _ssb_contemp).diff().shift(1)

        # Forward bases: ln(F_t / S_{t-1})
        # Apply spot.shift(1), again to account for publication lag of spot price (no lag for forwards)
        for label, col in [("FWD 1m",  "Salmon_Forward_M1_Weekly"),
                            ("FWD 3m",  "Salmon_Forward_M3_Weekly"),
                            ("FWD 6m",  "Salmon_Forward_M6_Weekly"),
                            ("FWD 12m", "Salmon_Forward_M12_Weekly")]:
            if col in df.columns:
                _basis            = _ln(df[col] / spot.shift(1))
                out[label]        = _basis
                out[f"∆ {label}"] = _basis.diff()

        # Forward curve slope & curvature
        if all(c in df.columns for c in ["Salmon_Forward_M1_Weekly",
                                          "Salmon_Forward_M6_Weekly",
                                          "Salmon_Forward_M12_Weekly"]):
            lnM1  = _ln(df["Salmon_Forward_M1_Weekly"])
            lnM6  = _ln(df["Salmon_Forward_M6_Weekly"])
            lnM12 = _ln(df["Salmon_Forward_M12_Weekly"])
            out["FWD Slope"]     = lnM12 - lnM1
            out["FWD Curvature"] = lnM1 - 2 * lnM6 + lnM12

        # Export volume: Similar HAR-style lags 1w, 2w, 1m
        # Short memory as we assume Norwegian supply shocks price within a month
        if "Salmon_Exported_Tons_SSB_Weekly" in df.columns:
            _exp_dln = _dln(df["Salmon_Exported_Tons_SSB_Weekly"])
            out["∆ Export Volume 1w"] = _exp_dln.rolling(1).mean().shift(1)
            out["∆ Export Volume 2w"] = _exp_dln.rolling(2).mean().shift(1)
            out["∆ Export Volume 1m"] = _exp_dln.rolling(4).mean().shift(1)

        # Chilean Exports: Similar HAR-style lags 1w, 2w, 1m, 3m
        # Longer memory to account for transit lag (5-6 weeks by ship) and market penetration of Chilean salmon in Europe
        if "Salmon_Chile_Export_Volume_Weekly" in df.columns:
            _chile = df["Salmon_Chile_Export_Volume_Weekly"]
            _chile_changed = _chile != _chile.shift(1)
            _chile_dln = _dln(_chile)
            _chile_dln[~_chile_changed] = np.nan
            _chile_dln = _chile_dln.ffill()
            out["∆ Chilean Exports 1w"] = _chile_dln.rolling(1).mean().shift(1)
            out["∆ Chilean Exports 2w"] = _chile_dln.rolling(2).mean().shift(1)
            out["∆ Chilean Exports 1m"] = _chile_dln.rolling(4).mean().shift(1)
            out["∆ Chilean Exports 3m"] = _chile_dln.rolling(13).mean().shift(1)

        # Total biomass MoM Δln and YOY cummulative log returns
        # Compute at transitions, ffill within month, to avoid zeros as data is published monthly (Done for all monthly variables)
        if "Salmon_Biomass_Kg_Monthly" in df.columns:
            _tbio = df["Salmon_Biomass_Kg_Monthly"]
            _tbio_changed = _tbio != _tbio.shift(1)
            _tbio_dln = _dln(_tbio)
            _tbio_dln[~_tbio_changed] = np.nan
            out["∆ Total Biomass Monthly"] = _tbio_dln.ffill()

            _ln_tbio = _ln(_tbio)
            out["∆YOY Total Biomass Monthly"] = _ln_tbio - _ln_tbio.shift(52)

        # Average weight: kg/fish and YOY cummulative log returns
        if all(c in df.columns for c in ["Salmon_Biomass_Kg_Monthly",
                                          "Salmon_Biomass_Fish_Stock_Monthly"]):
            _avgw = df["Salmon_Biomass_Kg_Monthly"] / df["Salmon_Biomass_Fish_Stock_Monthly"]
            out["Avg Weight (KG) Monthly"] = _avgw
            out["YOY Avg Weight (KG) Monthly"] = _ln(_avgw) - _ln(_avgw).shift(52)

        # Harvest intensity and loss rate: HAR-style lags 1m, 3m for harvest intensity, 1m, 3m, 6m for loss rate
        # Intuitively, harvest intensity has a more immediate effect on prices
        bkg   = "Salmon_Biomass_Kg_Monthly"
        stock = "Salmon_Biomass_Fish_Stock_Monthly"

        if all(c in df.columns for c in ["Salmon_Biomass_Harvest_Kg_Monthly", bkg]):
            _hi = df["Salmon_Biomass_Harvest_Kg_Monthly"] / df[bkg]
            out["Harvest Intensity 1m Monthly"] = _hi
            out["Harvest Intensity 3m Monthly"] = _hi.rolling(13).mean()

        loss_cols = ["Salmon_Biomass_Mortality_N_Monthly",
                     "Salmon_Biomass_Discard_N_Monthly",
                     "Salmon_Biomass_Escape_N_Monthly",
                     "Salmon_Biomass_Other_Loss_N_Monthly"]
        if all(c in df.columns for c in loss_cols + [stock]):
            total_loss = sum(df[c] for c in loss_cols)
            _loss_rate = total_loss / df[stock]
            out["Loss Rate 1m Monthly"] = _loss_rate
            out["Loss Rate 3m Monthly"] = _loss_rate.rolling(13).mean()
            out["Loss Rate 6m Monthly"] = _loss_rate.rolling(26).mean()

        # Smolt: ln-level, individual monthly lags 3m–19m (publication lag already covers one month)
        # Lags are directed at different forecast horizons from Y 0w to Y 12m with a 14 - 18 month growth cycle from smolt release to harvest
        if "Salmon_Biomass_Smolt_Releases_Monthly" in df.columns:
            _ln_smolt = _ln(df["Salmon_Biomass_Smolt_Releases_Monthly"])
            _dates = pd.to_datetime(df["Date"])
            _ln_smolt_monthly = (
                _ln_smolt
                .groupby(_dates.dt.to_period("M"))
                .first()
            )
            _ln_smolt_monthly.index = _ln_smolt_monthly.index.to_timestamp(how="start")
            for _m in range(2, 19):
                _shifted = (
                    _ln_smolt_monthly
                    .shift(_m)
                    .reindex(_dates, method="ffill")
                    .values
                )
                out[f"Smolt Release {_m}m Monthly"] = _shifted

        # ISA outbreak: Similar HAR-style with current + 1m and 3m rolling means
        if "Salmon_ILA_ActiveLocalities_Weekly" in df.columns:
            _isa = df["Salmon_ILA_ActiveLocalities_Weekly"]
            out["ISA Outbreak"]    = _isa
            out["ISA Outbreak 1m"] = _isa.rolling(4).mean().shift(1)
            out["ISA Outbreak 3m"] = _isa.rolling(13).mean().shift(1)

        # Lice outbreak: Similar HAR-style with current + 1m and 3m rolling means
        if "Salmon_Lice_AvgFemale_Weekly" in df.columns:
            _lice = df["Salmon_Lice_AvgFemale_Weekly"]
            out["Lice Outbreak"]    = _lice
            out["Lice Outbreak 1m"] = _lice.rolling(4).mean().shift(1)
            out["Lice Outbreak 3m"] = _lice.rolling(13).mean().shift(1)

        # Sea temp: Both current and 12m rolling average (to account for immediate and long-term effects)
        if "Salmon_SeaTemp_3m_Weekly" in df.columns:
            out["Sea Temp"]         = df["Salmon_SeaTemp_3m_Weekly"]
            out["Sea Temp 12m Avg"] = df["Salmon_SeaTemp_3m_Weekly"].rolling(52).mean()

        # EURNOK Δln with HAR-style lags 1w, 2w, 1m, 3m, 6m, 12m
        if "EURNOK_Weekly" in df.columns:
            _dln_eurnok = _dln(df["EURNOK_Weekly"])
            out["∆ EURNOK"]     = _dln_eurnok
            out["∆ EURNOK 1w"]  = _dln_eurnok.rolling(1).mean().shift(1)
            out["∆ EURNOK 2w"]  = _dln_eurnok.rolling(2).mean().shift(1)
            out["∆ EURNOK 1m"]  = _dln_eurnok.rolling(4).mean().shift(1)
            out["∆ EURNOK 3m"]  = _dln_eurnok.rolling(13).mean().shift(1)
            out["∆ EURNOK 6m"]  = _dln_eurnok.rolling(26).mean().shift(1)
            out["∆ EURNOK 12m"] = _dln_eurnok.rolling(52).mean().shift(1)

        # CPI Norway (level, already YoY %)
        if "CPI_Norway_Monthly" in df.columns:
            out["CPI NO Monthly"] = df["CPI_Norway_Monthly"]

        # NIBOR 3m (raw level)
        if "NIBOR_3m_Weekly" in df.columns:
            out["NIBOR 3m"] = df["NIBOR_3m_Weekly"]

        # Shrimp Δln: HAR structure with 1m, 3m, 6m and 12m rolling mean
        if "Protein_Shrimp_USD_mt_Weekly" in df.columns:
            _shrimp = df["Protein_Shrimp_USD_mt_Weekly"]
            _shrimp_changed = _shrimp != _shrimp.shift(1)
            _shrimp_dln = _dln(_shrimp)
            _shrimp_dln[~_shrimp_changed] = np.nan
            _shrimp_dln = _shrimp_dln.ffill()
            out["∆ Shrimp Price (Global) 1m Monthly"] = _shrimp_dln

            # Rolling averages
            _dates = pd.to_datetime(df["Date"])
            _shrimp_dln_monthly = (
                _shrimp_dln
                .groupby(_dates.dt.to_period("M"))
                .first()
            )
            _shrimp_dln_monthly.index = _shrimp_dln_monthly.index.to_timestamp(how="start")
            for _months, _label in [(3, "3m"), (6, "6m"), (12, "12m")]:
                _rolled = (
                    _shrimp_dln_monthly
                    .rolling(_months).mean()
                    .reindex(_dates, method="ffill")
                    .values
                )
                out[f"∆ Shrimp Price (Global) {_label} Monthly"] = _rolled

        # Competing proteins Δln with HAR structure
        # For pig: Published in current week based on Mon-Tue negotiations, so we can include the current week
        if "Protein_Broiler_EUR_100_kg_Weekly" in df.columns:
            _dln_b = _dln(df["Protein_Broiler_EUR_100_kg_Weekly"])
            out["∆ Broiler Price (EU) 1w"]  = _dln_b
            out["∆ Broiler Price (EU) 2w"]  = _dln_b.rolling(2).mean()
            out["∆ Broiler Price (EU) 1m"]  = _dln_b.rolling(4).mean()
            out["∆ Broiler Price (EU) 3m"]  = _dln_b.rolling(13).mean()
            out["∆ Broiler Price (EU) 6m"]  = _dln_b.rolling(26).mean()
            out["∆ Broiler Price (EU) 12m"] = _dln_b.rolling(52).mean()

        if "Protein_Pig_EUR_100_kg_Weekly" in df.columns:
            _dln_pig = _dln(df["Protein_Pig_EUR_100_kg_Weekly"])
            out["∆ Pig Price (EU)"]     = _dln_pig
            out["∆ Pig Price (EU) 1w"]  = _dln_pig.rolling(2).mean()
            out["∆ Pig Price (EU) 2w"]  = _dln_pig.rolling(3).mean()
            out["∆ Pig Price (EU) 1m"]  = _dln_pig.rolling(5).mean()
            out["∆ Pig Price (EU) 3m"]  = _dln_pig.rolling(14).mean()
            out["∆ Pig Price (EU) 6m"]  = _dln_pig.rolling(27).mean()
            out["∆ Pig Price (EU) 12m"] = _dln_pig.rolling(53).mean()

        # CPI Meat EU (level, already YoY %)
        if "Protein_Meat_Inflation_YoY_Monthly" in df.columns:
            out["Meat CPI (EU) Monthly"] = df["Protein_Meat_Inflation_YoY_Monthly"]

        # Fishmeal Δln: HAR structure with 1m, 3m, 6m and 12m rolling mean
        if "Commodity_Fishmeal_USD_mt_Weekly" in df.columns:
            _fish = df["Commodity_Fishmeal_USD_mt_Weekly"]
            _fish_changed = _fish != _fish.shift(1)
            _fish_dln = _dln(_fish)
            _fish_dln[~_fish_changed] = np.nan
            _fish_dln = _fish_dln.ffill()
            out["∆ Fishmeal 1m Monthly"] = _fish_dln

            # Rolling averages
            _dates = pd.to_datetime(df["Date"])
            _fish_dln_monthly = (
                _fish_dln
                .groupby(_dates.dt.to_period("M"))
                .first()
            )
            _fish_dln_monthly.index = _fish_dln_monthly.index.to_timestamp(how="start")
            for _months, _label in [(3, "3m"), (6, "6m"), (12, "12m")]:
                _rolled = (
                    _fish_dln_monthly
                    .rolling(_months).mean()
                    .reindex(_dates, method="ffill")
                    .values
                )
                out[f"∆ Fishmeal {_label} Monthly"] = _rolled

        # Commodities: slope ln(C12/C01), curvature ln(C01)−2·ln(C06)+ln(C12)
        # Brent and Soybean only (Previously we had included Wheat and Rapeseed, but data was unreliable)
        _commodities = [
            ("Brent",   "Commodity_Brent_CO1_NOK_bbl_Weekly",
                        "Commodity_Brent_CO6_NOK_bbl_Weekly",
                        "Commodity_Brent_CO12_NOK_bbl_Weekly"),
            ("Soybean", "Commodity_Soybean_SM1_NOK_st_Weekly",
                        "Commodity_Soybean_SM6_NOK_st_Weekly",
                        "Commodity_Soybean_SM12_NOK_st_Weekly"),
        ]
        for label, c1, c6, c12 in _commodities:
            if c1 in df.columns and c12 in df.columns:
                out[f"{label} FWD Slope"] = _ln(df[c12] / df[c1])
            if all(c in df.columns for c in [c1, c6, c12]):
                out[f"{label} FWD Curvature"] = _ln(df[c1]) - 2 * _ln(df[c6]) + _ln(df[c12])

        out = out.sort_values("Date").reset_index(drop=True)

        # Build freq_map from column naming convention
        # Columns ending in "Monthly" are monthly features, all others are weekly features
        # Done for EDA purposes to perform statistical tests consistent with the true frequency of the data
        freq_map = {
            col: ("monthly" if col.endswith("Monthly") else "weekly")
            for col in out.columns
        }

        return out, freq_map

    # Validate the feature matrix produced by buildFeatureMatrix()
    # Checks the following:
    #  - Column presence & dtypes
    #  - Infinite values
    #  - NaN counts and first/last valid date
    #  - Range plausibility — flags outliers beyond expected bounds
    #  - Δln / log-ratio columns: flags if any |value| > 0.5 (50% weekly move)
    #  - Proportion columns: flags if any value outside [0, 1]
    #  - Summary: rows fully populated (no NaN in any feature column)
    #  - Top-10 absolute correlations with target y
    def validateFeatureMatrix(self, matrix: pd.DataFrame) -> None:

        SEP  = "═" * 80
        SEP2 = "─" * 80

        feat_cols = [c for c in matrix.columns if c not in ("Date", "y")]
        n         = len(matrix)

        print(f"\n{SEP}")
        print(f"  FEATURE MATRIX VALIDATION REPORT")
        print(f"  Rows: {n}   Columns: {len(feat_cols)} features + Date + y")
        if "Date" in matrix.columns:
            print(f"  Date range: {matrix['Date'].min().date()} → {matrix['Date'].max().date()}")
        print(SEP)

        # Per-column checks
        print(f"\n── COLUMN CHECKS {'─'*63}")
        hdr = f"  {'Column':<35} {'NaNs':>6}  {'First valid':>11}  {'Min':>10}  {'Max':>10}  {'Infs':>5}  Status"
        print(hdr)
        print(f"  {'─'*35} {'─'*6}  {'─'*11}  {'─'*10}  {'─'*10}  {'─'*5}  ──────")

        # Expected bounds for plausibility check
        _dln_cols  = [c for c in feat_cols if c.endswith("_dln") or c == "spot_dln_lag1" or c == "y"]
        _prop_cols = [c for c in feat_cols if any(x in c for x in
                      ["share", "pct_treated", "harvest_intensity", "loss_rate",
                       "mortality_rate", "fcr_proxy", "biomass_share"])]
        _basis_cols = [c for c in feat_cols if "basis" in c or "slope" in c or "curv" in c or "spread" in c]

        issues = []
        for col in feat_cols:
            s           = matrix[col]
            nan_count   = int(s.isna().sum())
            inf_count   = int(np.isinf(s.replace([np.nan], [0])).sum())
            valid       = s.dropna()
            col_min     = f"{valid.min():.4f}" if len(valid) else "—"
            col_max     = f"{valid.max():.4f}" if len(valid) else "—"
            first_valid = (matrix.loc[s.first_valid_index(), "Date"].date()
                           if s.first_valid_index() is not None and "Date" in matrix.columns
                           else "—")

            flags = []
            if inf_count:
                flags.append(f"⚠ {inf_count} inf")
            if nan_count == n:
                flags.append("✗ ALL NaN")
            if col in _dln_cols and len(valid) and valid.abs().max() > 0.5:
                flags.append(f"⚠ |max|>{valid.abs().max():.2f} (>50%wk)")
            if col in _prop_cols and len(valid) and (valid.min() < 0 or valid.max() > 1.5):
                flags.append(f"⚠ out of [0,1.5]")
            if col in _basis_cols and len(valid) and valid.abs().max() > 1.0:
                flags.append(f"⚠ |basis|>{valid.abs().max():.2f}")

            status = "  ".join(flags) if flags else "✓"
            if flags:
                issues.append(col)

            col_disp = (col[:33] + "..") if len(col) > 35 else col
            print(f"  {col_disp:<35} {nan_count:>6}  {str(first_valid):>11}  "
                  f"{col_min:>10}  {col_max:>10}  {inf_count:>5}  {status}")

        # Proportion check for columns that are expected to be in [0, 1]
        print(f"\n── PROPORTION COLUMNS (should be in [0, ~1]) {'─'*35}")
        for col in _prop_cols:
            s = matrix[col].dropna()
            if len(s) == 0:
                continue
            ok = "✓" if s.min() >= 0 and s.max() <= 1.5 else f"⚠  min={s.min():.4f}  max={s.max():.4f}"
            print(f"  {col:<40}  {ok}")

        # Row completeness
        print(f"\n── ROW COMPLETENESS {'─'*60}")
        core_cols   = [c for c in feat_cols if c not in
                       ("fp_ssb_spread", "mortality_rate", "meat_inflation_yoy")]
        complete    = matrix[core_cols].dropna()
        pct         = 100 * len(complete) / n if n else 0
        first_full  = (matrix.loc[matrix[core_cols].dropna().index[0], "Date"].date()
                       if len(complete) and "Date" in matrix.columns else "—")
        print(f"  Rows with no NaN in core features : {len(complete):>5} / {n}  ({pct:.1f}%)")
        print(f"  First fully-populated row         : {first_full}")
        print(f"  (Excluded from core: fp_ssb_spread, mortality_rate, meat_inflation_yoy)")

        # Correlation with target y
        if "y" in matrix.columns:
            print(f"\n── TOP-10 ABSOLUTE CORRELATIONS WITH TARGET y {'─'*34}")
            corr = (matrix[feat_cols + ["y"]]
                    .dropna()
                    .corr()["y"]
                    .drop("y", errors="ignore")
                    .abs()
                    .sort_values(ascending=False)
                    .head(10))
            for col, val in corr.items():
                bar = "█" * int(val * 20)
                print(f"  {col:<40}  {val:>6.4f}  {bar}")

        # Summary
        print(f"\n{SEP2}")
        if issues:
            print(f"  ⚠  {len(issues)} column(s) flagged: {', '.join(issues)}")
        else:
            print(f"  ✓  No issues found.")
        print(f"{SEP}\n")