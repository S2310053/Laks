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
from pandas.tseries.offsets import MonthEnd

##
#  This class composes the feature engineering,
#  constructs variables used by the models
#

class featureEngineer:

    ## Defines weekly variables with fixed row-shift publication lags.
    #  Lag = number of Wednesday steps to shift forward.
    #  Only variables with lag > 0 are shifted; lag = 0 entries document
    #  that real-time availability was confirmed (no shift needed).
    #
    _zeroWeek = {
        "EURNOK_Weekly":                              0,   
        "USDNOK_Weekly":                              0,   
        "Protein_Pig_EUR_100_kg_Weekly":              0,   
        "Equity_MOWI_NOK_Weekly":                     0,
        "Equity_SALMAR_NOK_Weekly":                   0,
        "Commodity_Brent_COA_NOK_bbl_Weekly":         0,   
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
        "Commodity_Carbon_MO6_NOK_mt_Weekly":         0,
        "Salmon_Forward_M1_Weekly":                   0,   # FishPool end-of-day closing price, NOK/kg
        "Salmon_Forward_M3_Weekly":                   0,
        "Salmon_Forward_M6_Weekly":                   0,
        "Salmon_Forward_M12_Weekly":                  0}

    _oneWeek  = {
        "Salmon_NOK_kg_SSB_Weekly":                   1,   # SSB publishes following week
        "Salmon_Exported_Tons_SSB_Weekly":            1,
        "Protein_Broiler_EUR_100_kg_Weekly":          1,   # EU Commission publishes following Wednesday
        "Salmon_Escapes_Rep_Escaped_Weekly":          1,   # 24h statutory reporting window
        "Salmon_Escapes_Recapture_Weekly":            1,
        "Salmon_Escapes_Avg_Wt_Grams_Weekly":         1,
        "Salmon_Lice_LocalitiesReporting_Weekly":     1,   # BarentsWatch: published following week
        "Salmon_Lice_AvgFemale_Weekly":               1,
        "Salmon_SeaTemp_3m_Weekly":                   1,
        "Salmon_Lice_PctAboveLimit_Weekly":           1,
        "Salmon_Lice_PctTreated_Weekly":              1,
        "Salmon_Lice_ICA_Count_Weekly":               1,   # ISA (ICA) locality count from BarentsWatch API
        "Salmon_ILA_ActiveLocalities_Weekly":         1,   # Active ILA outbreaks from Mattilsynet ila_pd.csv
        "Protein_Shrimp_USD_mt_Weekly":               4}   # IMF/FRED monthly, ~4-week publication lag

    PUBLISH_LAG_WEEKS = _zeroWeek | _oneWeek

    ## Defines monthly variables with calendar-exact publication lags.
    #
    #  Each value is the number of calendar days after the END OF THE
    #  REFERENCE MONTH when the data becomes publicly available.
    #
    #  Implementation: _lagByPublication uses merge_asof to assign each row
    #  the most recently published monthly value, defined as the latest month M
    #  whose publish Wednesday (= EOMONTH(M) + N, snapped to next Wednesday)
    #  falls on or before the row's date.
    #
    PUBLISH_LAG_EOMONTH = {

        # SSB releases ~10th of following month
        "CPI_Norway_Monthly":                         10,
        "Protein_Meat_Inflation_YoY_Monthly":         10,  # Eurostat HICP; same schedule

        # Fiskeridirektoratet biomass panel, releases ~20th of following month
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
        "Salmon_Biomass_Biomass_Kg_Age0_Monthly":     20,
        "Salmon_Biomass_Biomass_Kg_Age1_Monthly":     20,
        "Salmon_Biomass_Biomass_Kg_Age2Plus_Monthly": 20,
        "Salmon_Biomass_Fish_Stock_Age0_Monthly":     20,
        "Salmon_Biomass_Fish_Stock_Age1_Monthly":     20,
        "Salmon_Biomass_Fish_Stock_Age2Plus_Monthly": 20,

        # UN Comtrade multi-step pipeline (SSB → customs → Comtrade), releases ~10th of 2nd next month
        "Salmon_Export_Net_Weight_Kg_Monthly":        40,
        "Salmon_Export_Value_USD_Monthly":            40,
        "Salmon_Export_Avg_Price_USD_Kg_Monthly":     40,

    }

    def __init__(self):
        pass

    ##
    #   Apply publication lags to time series features.
    #
    #   Weekly variables  → shifted by a fixed number of rows (weeks).
    #   Monthly variables → each row receives the most recently published
    #                       monthly value, where "published" means the first
    #                       Wednesday on or after EOMONTH(reference_month) + N days.
    #                       Implemented via merge_asof for exact calendar alignment.
    #
    #   @data   DataFrame with a 'Date' column or DatetimeIndex (Wednesday spine)
    #   @return DataFrame with look-ahead bias removed
    #
    def _lagByPublication(self, data: pd.DataFrame) -> pd.DataFrame:

        # ── Resolve dates ─────────────────────────────────────────────────────
        if "Date" in data.columns:
            dates = pd.to_datetime(data["Date"]).reset_index(drop=True)
        elif isinstance(data.index, pd.DatetimeIndex):
            dates = data.index.to_series().reset_index(drop=True)
        else:
            raise ValueError("DataFrame must have a 'Date' column or a DatetimeIndex.")

        # ── Weekly lags (fixed row shift) ─────────────────────────────────────
        for col, lag in self.PUBLISH_LAG_WEEKS.items():
            if col in data.columns and lag > 0:
                data[col] = data[col].shift(lag)

        # ── Monthly lags (EOMONTH-exact, via merge_asof) ──────────────────────
        for col, days_after in self.PUBLISH_LAG_EOMONTH.items():
            if col not in data.columns:
                continue

            # Build one entry per reference month:
            #   month_end      = last calendar day of the reference month
            #   value          = the monthly value broadcast to all rows in that month
            #   publish_wed    = first Wednesday on or after EOMONTH(month) + N days
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

            # For each row date: look up the most recently published value
            # merge_asof(direction="backward") gives the last publish_wed ≤ row_date
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

            # Restore original row order and assign back
            merged  = merged.sort_values("orig_order")
            data[col] = merged["value"].values

        ## Trim the 4-week warm-up buffer added by Data() and reset the time index
        data = data[data["Date"] >= pd.Timestamp("2000-01-05")].reset_index(drop=True)
        data["t"] = range(len(data))

        return data

    ##
    #   Validate that publication lags were applied correctly on an
    #   already-lagged DataFrame (output of _lagByPublication).
    #
    #   Weekly variables  — checks that the number of leading NaN rows
    #                       equals the expected lag (shift introduces NaN
    #                       at the top of the series).
    #   Monthly variables — finds value transition points in the lagged
    #                       series, back-calculates the reference month from
    #                       each transition date, and verifies the transition
    #                       falls on the correct publish Wednesday
    #                       (= first Wednesday on or after EOMONTH(M) + N days).
    #
    #   Prints a formatted report. No data is modified.
    #
    #   @data   DataFrame output of _lagByPublication()
    #
    def validatePublicationLags(self, data: pd.DataFrame) -> None:

        # Resolve dates
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

        # ── 1. Weekly lags ────────────────────────────────────────────────────
        # After shift(n), the first n rows are NaN. Check that leading NaN
        # count is >= lag (it may be larger if the raw source itself starts late).
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

        # ── 2. Monthly lags ───────────────────────────────────────────────────
        # Finds value transitions in the lagged series, then back-calculates
        # the reference month end from each transition date:
        #   upper         = trans_date − days_after
        #   ref_month_end = largest month-end ≤ upper
        # Then recomputes the expected publish Wednesday and compares.
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

            # Collect value-to-different-value transitions
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

            # Back-calculate reference month end:
            #   EOMONTH(M) is the largest month-end ≤ (trans_date − days_after)
            upper             = trans_date - pd.Timedelta(days=days_after)
            month_end_upper   = upper + MonthEnd(0)   # end of upper's own month (≥ upper)
            if month_end_upper <= upper:
                ref_month_end = month_end_upper
            else:
                ref_month_end = pd.Timestamp(upper.year, upper.month, 1) - pd.Timedelta(days=1)

            # Re-derive expected publish Wednesday
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

    ##
    #   Build the feature matrix used for forecasting models.
    #
    #   Variables in the order specified:
    #     Target : Δln(Salmon_NOK_kg_FP_Weekly)
    #     1.  Δln spot lag-1 (AR term; additional lags added after ACF/PACF)
    #     2.  ln(FP / SSB) spread              [deferred — Granger test first]
    #     3.  ln(F_M1  / Spot)
    #     4.  ln(F_M3  / Spot)
    #     5.  ln(F_M6  / Spot)
    #     6.  ln(F_M12 / Spot)
    #     7.  Forward slope     ln(F_M12 / F_M1)
    #     8.  Forward curvature ln(F_M1) − 2·ln(F_M6) + ln(F_M12)
    #     9.  Export volume Δln
    #     10. Biomass Age0  ln-level
    #     11. Biomass Age1  ln-level
    #     12. Biomass Age2+ ln-level
    #     13. Biomass share Age2+ (raw proportion)
    #     14. Average weight kg/fish (raw)
    #     15. FCR proxy Feed/Biomass (raw)
    #     16. Harvest Intensity Harvest/Biomass (raw)
    #     17. Loss Rate (raw)
    #     18. Mortality Rate (raw)              [deferred — collinearity check]
    #     19. Smolt ln-level, lagged smolt_lag weeks
    #     20. ILA active localities ln(1+x)
    #     21. Sea lice avg female ln(1+x)
    #     22. Sea lice treatment % (raw)
    #     23. Sea temperature (raw)
    #     24. Δln EURNOK
    #     25. CPI Norway level (already YoY %)
    #     26. Δln Shrimp
    #     27. Δln Broiler
    #     28. Δln Pig
    #     29. Protein Meat Inflation YoY level  [deferred — may drop]
    #     30. Δln Soybean
    #     31. Δln Wheat
    #     32. Δln Rapeseed
    #     33. Δln Brent
    #     34–37. Commodity curve slope  × 4  ln(C6/C1)
    #     38–41. Commodity curve curv   × 4  ln(C1)−2·ln(C3)+ln(C6)
    #
    #   @data       DataFrame output of _lagByPublication()
    #   @smolt_lag  weeks to lag smolt releases (default 65)
    #   @return     DataFrame with Date, y, and all feature columns
    #
    def buildFeatureMatrix(self,
                           data: pd.DataFrame,
                           raw: pd.DataFrame = None,
                           smolt_lag: int = 65) -> pd.DataFrame:

        df   = data.copy()
        spot = df["Salmon_NOK_kg_FP_Weekly"]

        def _ln(s):
            """Log level — returns NaN where input is zero, negative, or missing."""
            s      = pd.to_numeric(s, errors="coerce")
            result = np.full(len(s), np.nan)
            mask   = s.values > 0
            result[mask] = np.log(s.values[mask])
            return pd.Series(result, index=s.index)

        def _dln(s):
            """Week-over-week log return — returns 0 where input is zero or negative."""
            return _ln(s).diff()

        out = pd.DataFrame({"Date": df["Date"]})

        # ── Target ────────────────────────────────────────────────────────────
        out["Salmon (NOK/KG)"] = _dln(spot)

        # ── 1. AR term: Δln spot lag-1 ────────────────────────────────────────
        out["Salmon (NOK/KG) 1w"] = _dln(spot).shift(1)

        # ── 2. FP–SSB spread (deferred) — uses unlagged SSB for contemporaneous basis
        _ssb_source = raw if raw is not None else df
        if "Salmon_NOK_kg_SSB_Weekly" in _ssb_source.columns:
            _ssb_unlagged = _ssb_source["Salmon_NOK_kg_SSB_Weekly"].values
            out["Spread (FP - SSB)"] = _ln(spot / pd.Series(_ssb_unlagged, index=df.index))

        # ── 3–6. Forward bases: ln(F_Mn / Spot) ──────────────────────────────
        for label, col in [("FWD 1m",  "Salmon_Forward_M1_Weekly"),
                            ("FWD 3m",  "Salmon_Forward_M3_Weekly"),
                            ("FWD 6m",  "Salmon_Forward_M6_Weekly"),
                            ("FWD 12m", "Salmon_Forward_M12_Weekly")]:
            if col in df.columns:
                out[label] = _ln(df[col] / spot)

        # ── 7–8. Forward curve slope & curvature ─────────────────────────────
        if all(c in df.columns for c in ["Salmon_Forward_M1_Weekly",
                                          "Salmon_Forward_M6_Weekly",
                                          "Salmon_Forward_M12_Weekly"]):
            lnM1  = _ln(df["Salmon_Forward_M1_Weekly"])
            lnM6  = _ln(df["Salmon_Forward_M6_Weekly"])
            lnM12 = _ln(df["Salmon_Forward_M12_Weekly"])
            out["FWD Slope"]     = lnM12 - lnM1
            out["FWD Curvature"] = lnM1 - 2 * lnM6 + lnM12

        # ── 9. Export volume Δln ──────────────────────────────────────────────
        if "Salmon_Exported_Tons_SSB_Weekly" in df.columns:
            out["Export Volume"] = _dln(df["Salmon_Exported_Tons_SSB_Weekly"])

        # ── 10–12. Biomass by age: ln-level ───────────────────────────────────
        for label, col in [("Biomass (0 - 1)",  "Salmon_Biomass_Biomass_Kg_Age0_Monthly"),
                            ("Biomass (1 - 2)",  "Salmon_Biomass_Biomass_Kg_Age1_Monthly"),
                            ("Biomass (2+)",     "Salmon_Biomass_Biomass_Kg_Age2Plus_Monthly")]:
            if col in df.columns:
                out[label] = _ln(df[col])

        # ── 13. Biomass share Age2+ ───────────────────────────────────────────
        c0, c1, c2 = ("Salmon_Biomass_Biomass_Kg_Age0_Monthly",
                       "Salmon_Biomass_Biomass_Kg_Age1_Monthly",
                       "Salmon_Biomass_Biomass_Kg_Age2Plus_Monthly")
        if all(c in df.columns for c in [c0, c1, c2]):
            out["Biomass Share 2+"] = df[c2] / (df[c0] + df[c1] + df[c2])

        # ── 14. Average weight kg/fish ────────────────────────────────────────
        if all(c in df.columns for c in ["Salmon_Biomass_Kg_Monthly",
                                          "Salmon_Biomass_Fish_Stock_Monthly"]):
            out["Avg Weight (KG)"] = (df["Salmon_Biomass_Kg_Monthly"]
                                      / df["Salmon_Biomass_Fish_Stock_Monthly"])

        # ── 15–18. Derived ratios ─────────────────────────────────────────────
        bkg   = "Salmon_Biomass_Kg_Monthly"
        stock = "Salmon_Biomass_Fish_Stock_Monthly"

        if all(c in df.columns for c in ["Salmon_Biomass_Feed_Kg_Monthly", bkg]):
            out["FCR"] = df["Salmon_Biomass_Feed_Kg_Monthly"] / df[bkg]

        if all(c in df.columns for c in ["Salmon_Biomass_Harvest_Kg_Monthly", bkg]):
            out["Harvest Intensity"] = df["Salmon_Biomass_Harvest_Kg_Monthly"] / df[bkg]

        loss_cols = ["Salmon_Biomass_Mortality_N_Monthly",
                     "Salmon_Biomass_Discard_N_Monthly",
                     "Salmon_Biomass_Escape_N_Monthly",
                     "Salmon_Biomass_Other_Loss_N_Monthly"]
        if all(c in df.columns for c in loss_cols + [stock]):
            total_loss = sum(df[c] for c in loss_cols)
            out["Loss Rate"]     = total_loss / df[stock]
            out["Mortality Rate"] = df["Salmon_Biomass_Mortality_N_Monthly"] / df[stock]

        # ── 19. Smolt: ln-level, lagged smolt_lag weeks ───────────────────────
        if "Salmon_Biomass_Smolt_Releases_Monthly" in df.columns:
            out["Smolt Release 65w"] = _ln(df["Salmon_Biomass_Smolt_Releases_Monthly"]).shift(smolt_lag)

        # ── 20. ILA: ln(1 + x) ────────────────────────────────────────────────
        if "Salmon_ILA_ActiveLocalities_Weekly" in df.columns:
            out["ISA Outbreak"] = np.log1p(df["Salmon_ILA_ActiveLocalities_Weekly"])

        # ── 21. Sea lice: ln(1 + x) ───────────────────────────────────────────
        if "Salmon_Lice_AvgFemale_Weekly" in df.columns:
            out["Lice Outbreak"] = np.log1p(df["Salmon_Lice_AvgFemale_Weekly"])

        # ── 22–23. Lice treatment & sea temperature ───────────────────────────
        if "Salmon_Lice_PctTreated_Weekly" in df.columns:
            out["Lice Treatment (%)"] = df["Salmon_Lice_PctTreated_Weekly"] / 100
        if "Salmon_SeaTemp_3m_Weekly" in df.columns:
            out["Sea Temp"] = df["Salmon_SeaTemp_3m_Weekly"]

        # ── 24. EURNOK Δln ────────────────────────────────────────────────────
        if "EURNOK_Weekly" in df.columns:
            out["EURNOK"] = _dln(df["EURNOK_Weekly"])

        # ── 25. CPI Norway (level, already YoY %) ────────────────────────────
        if "CPI_Norway_Monthly" in df.columns:
            out["CPI NO"] = df["CPI_Norway_Monthly"]

        # ── 26. Shrimp Δln ────────────────────────────────────────────────────
        if "Protein_Shrimp_USD_mt_Weekly" in df.columns:
            out["Shrimp Price (Global)"] = _dln(df["Protein_Shrimp_USD_mt_Weekly"])

        # ── 27–28. Competing proteins Δln ────────────────────────────────────
        for label, col in [("Broiler Price (EU)", "Protein_Broiler_EUR_100_kg_Weekly"),
                            ("Pig Price (EU)",     "Protein_Pig_EUR_100_kg_Weekly")]:
            if col in df.columns:
                out[label] = _dln(df[col])

        # ── 29. Meat Inflation YoY (deferred) ────────────────────────────────
        if "Protein_Meat_Inflation_YoY_Monthly" in df.columns:
            out["Meat CPI (EU)"] = df["Protein_Meat_Inflation_YoY_Monthly"]

        # ── 30–41. Commodities: Δln C01/C03/C06, slope ln(C06/C01),
        #          curvature ln(C01) − 2·ln(C03) + ln(C06) ─────────────────────
        _commodities = [
            ("Brent",    "Commodity_Brent_CO1_NOK_bbl_Weekly",
                         "Commodity_Brent_CO3_NOK_bbl_Weekly",
                         "Commodity_Brent_CO6_NOK_bbl_Weekly"),
            ("Wheat",    "Commodity_Wheat_CA1_NOK_mt_Weekly",
                         "Commodity_Wheat_CA3_NOK_mt_Weekly",
                         "Commodity_Wheat_CA6_NOK_mt_Weekly"),
            ("Rapeseed", "Commodity_Rapeseed_IJ1_NOK_mt_Weekly",
                         "Commodity_Rapeseed_IJ3_NOK_mt_Weekly",
                         "Commodity_Rapeseed_IJ6_NOK_mt_Weekly"),
            ("Soybean",  "Commodity_Soybean_SM1_NOK_st_Weekly",
                         "Commodity_Soybean_SM3_NOK_st_Weekly",
                         "Commodity_Soybean_SM6_NOK_st_Weekly"),
        ]
        for label, c1, c3, c6 in _commodities:
            if c1 in df.columns:
                out[f"{label} FWD 1m"] = _dln(df[c1])
            if c3 in df.columns:
                out[f"{label} FWD 3m"] = _dln(df[c3])
            if c6 in df.columns:
                out[f"{label} FWD 6m"] = _dln(df[c6])
            if c1 in df.columns and c6 in df.columns:
                out[f"{label} FWD Slope"] = _ln(df[c6] / df[c1])
            if all(c in df.columns for c in [c1, c3, c6]):
                out[f"{label} FWD Curvature"] = _ln(df[c1]) - 2 * _ln(df[c3]) + _ln(df[c6])

        return out.reset_index(drop=True)

    ##
    #   Validate the feature matrix produced by buildFeatureMatrix().
    #
    #   Checks (per column):
    #     1. Column presence & dtypes
    #     2. Infinite values
    #     3. NaN counts and first/last valid date
    #     4. Range plausibility — flags outliers beyond expected bounds
    #     5. Δln / log-ratio columns: flags if any |value| > 0.5 (50% weekly move)
    #     6. Proportion columns: flags if any value outside [0, 1]
    #     7. Summary: rows fully populated (no NaN in any feature column)
    #     8. Top-10 absolute correlations with target y
    #
    #   @matrix   DataFrame output of buildFeatureMatrix()
    #
    def validateFeatureMatrix(self, matrix: pd.DataFrame) -> None:

        from scipy import stats as _stats

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

        # ── 1–5. Per-column checks ─────────────────────────────────────────────
        print(f"\n── COLUMN CHECKS {'─'*63}")
        hdr = f"  {'Column':<35} {'NaNs':>6}  {'First valid':>11}  {'Min':>10}  {'Max':>10}  {'Infs':>5}  Status"
        print(hdr)
        print(f"  {'─'*35} {'─'*6}  {'─'*11}  {'─'*10}  {'─'*10}  {'─'*5}  ──────")

        # Expected bounds for quick plausibility check
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

        # ── 6. Proportion sanity ───────────────────────────────────────────────
        print(f"\n── PROPORTION COLUMNS (should be in [0, ~1]) {'─'*35}")
        for col in _prop_cols:
            s = matrix[col].dropna()
            if len(s) == 0:
                continue
            ok = "✓" if s.min() >= 0 and s.max() <= 1.5 else f"⚠  min={s.min():.4f}  max={s.max():.4f}"
            print(f"  {col:<40}  {ok}")

        # ── 7. Fully populated rows ────────────────────────────────────────────
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

        # ── 8. Correlation with target y ──────────────────────────────────────
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

        # ── Summary ───────────────────────────────────────────────────────────
        print(f"\n{SEP2}")
        if issues:
            print(f"  ⚠  {len(issues)} column(s) flagged: {', '.join(issues)}")
        else:
            print(f"  ✓  No issues found.")
        print(f"{SEP}\n")
