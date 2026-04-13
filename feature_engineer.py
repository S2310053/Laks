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
        "NIBOR_3m_Weekly":                            0,   # Bloomberg daily, Wednesday close, no lag
        "Protein_Pig_EUR_100_kg_Weekly":             -1,   # Bloomberg dates Friday, actual release Wed — shift back 1 week to correct misdating
        "Protein_Broiler_EUR_100_kg_Weekly":          0,   # W-WED resample already places Friday publication in following bin — no extra lag needed
        "Equity_MOWI_NOK_Weekly":                     0,
        "Equity_SALMAR_NOK_Weekly":                   0,
        "Commodity_Brent_COA_NOK_bbl_Weekly":         0,   
        "Commodity_Brent_CO1_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO2_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO3_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO4_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO5_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO6_NOK_bbl_Weekly":         0,
        "Commodity_Brent_CO12_NOK_bbl_Weekly":        0,
        "Commodity_Wheat_CAA_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA1_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA2_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA3_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA4_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA5_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA6_NOK_mt_Weekly":          0,
        "Commodity_Wheat_CA12_NOK_mt_Weekly":         0,
        "Commodity_Soybean_SMA_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM1_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM2_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM3_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM4_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM5_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM6_NOK_st_Weekly":        0,
        "Commodity_Soybean_SM12_NOK_st_Weekly":       0,
        "Commodity_Rapeseed_IJA_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ1_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ2_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ3_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ4_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ5_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ6_NOK_mt_Weekly":       0,
        "Commodity_Rapeseed_IJ12_NOK_mt_Weekly":      0,
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
        "Protein_Shrimp_USD_mt_Weekly":               4,    # IMF/FRED monthly, ~4-week publication lag
        "Commodity_Fishmeal_USD_mt_Weekly":           4,    # Bloomberg monthly end-of-month, ~4-week lag
        "Salmon_Chile_Export_Volume_Weekly":          1}    # Chilean customs, ~1-week publication lag

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
            if col in data.columns and lag != 0:
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

        # ── Target: Y 0w (nowcast) + multi-horizon leads Y 1w…12m ───────────────
        #    Spot price has a 1-week publication lag: at row t, the most recently
        #    published price is for week t-1. So dln[t+1] is the first unknown return.
        #
        #    Y 0w  = dln[t+1]                       — nowcast (1 period)
        #    Y 1w  = dln[t+2]                       — 1 week after nowcast
        #    Y h   = dln[t+1] + … + dln[t+h]       — cumulative log return (Corsi HAR)
        #          = ln(spot[t+h] / spot[t])
        #
        #    These use future data — valid as targets only, never as predictors.
        _dln_spot = _dln(spot)
        out["Y 0w ∆ Salmon (NOK/KG)"]  = _dln_spot
        out["Y 1w ∆ Salmon (NOK/KG)"]  = _dln_spot.rolling(2).sum().shift(-1)
        out["Y 2w ∆ Salmon (NOK/KG)"]  = _dln_spot.rolling(3).sum().shift(-2)
        out["Y 1m ∆ Salmon (NOK/KG)"]  = _dln_spot.rolling(5).sum().shift(-4)
        out["Y 3m ∆ Salmon (NOK/KG)"]  = _dln_spot.rolling(14).sum().shift(-13)
        out["Y 6m ∆ Salmon (NOK/KG)"]  = _dln_spot.rolling(27).sum().shift(-26)
        out["Y 12m ∆ Salmon (NOK/KG)"] = _dln_spot.rolling(53).sum().shift(-52)

        # ── 1. HAR-style Δln spot lags: avg over 1w, 2w, 1m, 3m, 6m, 12m ────
        #    Each is the rolling average of Δln(spot) ending last week (shift(1)
        #    ensures no look-ahead). Averages keep all horizons on the same scale.
        out["∆ Salmon (NOK/KG) 1w"]  = _dln_spot.rolling(1).mean().shift(1)
        out["∆ Salmon (NOK/KG) 2w"]  = _dln_spot.rolling(2).mean().shift(1)
        out["∆ Salmon (NOK/KG) 1m"]  = _dln_spot.rolling(4).mean().shift(1)
        out["∆ Salmon (NOK/KG) 3m"]  = _dln_spot.rolling(13).mean().shift(1)
        out["∆ Salmon (NOK/KG) 6m"]  = _dln_spot.rolling(26).mean().shift(1)
        out["∆ Salmon (NOK/KG) 12m"] = _dln_spot.rolling(52).mean().shift(1)

        # ── 2. FP–SSB spread — reconstruct contemporaneous SSB by undoing the
        #       1-week publication lag (shift(-1)), compute ln(FP_t / SSB_t),
        #       then shift(1) so row t shows last week's spread (first available)
        if "Salmon_NOK_kg_SSB_Weekly" in df.columns:
            _ssb_contemp = df["Salmon_NOK_kg_SSB_Weekly"].shift(-1)
            out["Spread (FP - SSB)"]   = (spot - _ssb_contemp).shift(1)
            out["∆ Spread (FP - SSB)"] = (spot - _ssb_contemp).diff().shift(1)

        # ── 3–6. Forward bases: ln(F_t / S_{t-1}) — current forward vs last known
        #        spot. F_t is observable same-day (FishPool end-of-day). S_{t-1}
        #        is the spot published this week (1-week publication lag).
        #        spot.shift(1) applies only here — not a general lag change.
        for label, col in [("FWD 1m",  "Salmon_Forward_M1_Weekly"),
                            ("FWD 3m",  "Salmon_Forward_M3_Weekly"),
                            ("FWD 6m",  "Salmon_Forward_M6_Weekly"),
                            ("FWD 12m", "Salmon_Forward_M12_Weekly")]:
            if col in df.columns:
                _basis            = _ln(df[col] / spot.shift(1))
                out[label]        = _basis
                out[f"∆ {label}"] = _basis.diff()

        # ── 7–8. Forward curve slope & curvature ─────────────────────────────
        if all(c in df.columns for c in ["Salmon_Forward_M1_Weekly",
                                          "Salmon_Forward_M6_Weekly",
                                          "Salmon_Forward_M12_Weekly"]):
            lnM1  = _ln(df["Salmon_Forward_M1_Weekly"])
            lnM6  = _ln(df["Salmon_Forward_M6_Weekly"])
            lnM12 = _ln(df["Salmon_Forward_M12_Weekly"])
            out["FWD Slope"]     = lnM12 - lnM1
            out["FWD Curvature"] = lnM1 - 2 * lnM6 + lnM12

        # ── 9. Export volume: HAR-style lags 1w, 2w, 1m ─────────────────────
        #    Short memory — Norwegian supply shocks price within 1-2 weeks.
        if "Salmon_Exported_Tons_SSB_Weekly" in df.columns:
            _exp_dln = _dln(df["Salmon_Exported_Tons_SSB_Weekly"])
            out["∆ Export Volume 1w"] = _exp_dln.rolling(1).mean().shift(1)
            out["∆ Export Volume 2w"] = _exp_dln.rolling(2).mean().shift(1)
            out["∆ Export Volume 1m"] = _exp_dln.rolling(4).mean().shift(1)

        # ── 9b. Chilean Exports: HAR-style lags 1w, 2w, 1m, 3m ──────────────
        #    Transit lag (~5-6 weeks by ship) + market penetration justifies
        #    the 3m window on top of the standard short-memory horizons.
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

        # ── 10–11. Biomass (1-2) and (2+) — removed (not usable)

        # ── 13b. Total biomass MoM Δln (compute at transitions, ffill within month)
        #        + YoY: ln(x_t) − ln(x_{t-52}) — cumulative log return vs same week last year
        if "Salmon_Biomass_Kg_Monthly" in df.columns:
            _tbio = df["Salmon_Biomass_Kg_Monthly"]
            _tbio_changed = _tbio != _tbio.shift(1)
            _tbio_dln = _dln(_tbio)
            _tbio_dln[~_tbio_changed] = np.nan
            out["∆ Total Biomass Monthly"] = _tbio_dln.ffill()

            _ln_tbio = _ln(_tbio)
            out["∆YOY Total Biomass Monthly"] = _ln_tbio - _ln_tbio.shift(52)

        # ── 14. Average weight kg/fish
        #        + YoY: ln(x_t) − ln(x_{t-52}) — level vs same week last year
        if all(c in df.columns for c in ["Salmon_Biomass_Kg_Monthly",
                                          "Salmon_Biomass_Fish_Stock_Monthly"]):
            _avgw = df["Salmon_Biomass_Kg_Monthly"] / df["Salmon_Biomass_Fish_Stock_Monthly"]
            out["Avg Weight (KG) Monthly"] = _avgw
            out["YOY Avg Weight (KG) Monthly"] = _ln(_avgw) - _ln(_avgw).shift(52)

        # ── 15–18. Derived ratios ─────────────────────────────────────────────
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

        # ── 19. Smolt: ln-level, individual monthly lags 2m–18m ─────────────────
        #    Publication lag already covers ~1m. Lags 2m–18m cover the full
        #    14–18 month grow-out cycle across all forecast horizons (Y 0w–Y 12m).
        #    Resampled to monthly before shifting to avoid week-count drift.
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

        # ── 20. ISA outbreak: current + 1m and 3m rolling means (Corsi-consistent)
        if "Salmon_ILA_ActiveLocalities_Weekly" in df.columns:
            _isa = df["Salmon_ILA_ActiveLocalities_Weekly"]
            out["ISA Outbreak"]    = _isa
            out["ISA Outbreak 1m"] = _isa.rolling(4).mean().shift(1)
            out["ISA Outbreak 3m"] = _isa.rolling(13).mean().shift(1)

        # ── 21. Lice outbreak: current + 1m and 3m rolling means (Corsi-consistent)
        if "Salmon_Lice_AvgFemale_Weekly" in df.columns:
            _lice = df["Salmon_Lice_AvgFemale_Weekly"]
            out["Lice Outbreak"]    = _lice
            out["Lice Outbreak 1m"] = _lice.rolling(4).mean().shift(1)
            out["Lice Outbreak 3m"] = _lice.rolling(13).mean().shift(1)
        if "Salmon_SeaTemp_3m_Weekly" in df.columns:
            out["Sea Temp"]         = df["Salmon_SeaTemp_3m_Weekly"]
            out["Sea Temp 12m Avg"] = df["Salmon_SeaTemp_3m_Weekly"].rolling(52).mean()

        # ── 24. EURNOK Δln — contemporaneous (no publication lag) + HAR lags ──────
        if "EURNOK_Weekly" in df.columns:
            _dln_eurnok = _dln(df["EURNOK_Weekly"])
            out["∆ EURNOK"]     = _dln_eurnok
            out["∆ EURNOK 1w"]  = _dln_eurnok.rolling(1).mean().shift(1)
            out["∆ EURNOK 2w"]  = _dln_eurnok.rolling(2).mean().shift(1)
            out["∆ EURNOK 1m"]  = _dln_eurnok.rolling(4).mean().shift(1)
            out["∆ EURNOK 3m"]  = _dln_eurnok.rolling(13).mean().shift(1)
            out["∆ EURNOK 6m"]  = _dln_eurnok.rolling(26).mean().shift(1)
            out["∆ EURNOK 12m"] = _dln_eurnok.rolling(52).mean().shift(1)

        # ── 25. CPI Norway (level, already YoY %) ────────────────────────────
        if "CPI_Norway_Monthly" in df.columns:
            out["CPI NO Monthly"] = df["CPI_Norway_Monthly"]

        # ── 25b. NIBOR 3m (raw level, already stationary) ────────────────────
        if "NIBOR_3m_Weekly" in df.columns:
            out["NIBOR 3m"] = df["NIBOR_3m_Weekly"]

        # ── 26. Shrimp Δln — HAR structure (4-week publication lag, no contemporaneous)
        if "Protein_Shrimp_USD_mt_Weekly" in df.columns:
            _shrimp = df["Protein_Shrimp_USD_mt_Weekly"]
            _shrimp_changed = _shrimp != _shrimp.shift(1)
            _shrimp_dln = _dln(_shrimp)
            _shrimp_dln[~_shrimp_changed] = np.nan
            _shrimp_dln = _shrimp_dln.ffill()
            out["∆ Shrimp Price (Global) 1m Monthly"] = _shrimp_dln

            # Rolling averages: resample to monthly first to avoid over-weighting
            # forward-filled weekly repetitions, then reindex back to weekly
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

        # ── 27–28. Competing proteins Δln (HAR structure) ────────────────────
        # Broiler: W-WED resample places Friday publication in following bin,
        #          so _dln_p is already a 1w lag — label accordingly, no extra shift(1).
        # Pig: published Wed (current week), so _dln_p is contemporaneous.
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

        # ── 29. Meat Inflation YoY (deferred) ────────────────────────────────
        if "Protein_Meat_Inflation_YoY_Monthly" in df.columns:
            out["Meat CPI (EU) Monthly"] = df["Protein_Meat_Inflation_YoY_Monthly"]

        # ── 30. Fishmeal Δln — HAR structure (4-week publication lag, no contemporaneous)
        if "Commodity_Fishmeal_USD_mt_Weekly" in df.columns:
            _fish = df["Commodity_Fishmeal_USD_mt_Weekly"]
            _fish_changed = _fish != _fish.shift(1)
            _fish_dln = _dln(_fish)
            _fish_dln[~_fish_changed] = np.nan
            _fish_dln = _fish_dln.ffill()
            out["∆ Fishmeal 1m Monthly"] = _fish_dln

            # Rolling averages: resample to monthly first to avoid over-weighting
            # forward-filled weekly repetitions, then reindex back to weekly
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

        # ── 31–34. Commodities: slope ln(C12/C01), curvature ln(C01)−2·ln(C06)+ln(C12)
        #          Brent and Soybean only (Wheat starts 2016, Rapeseed data unreliable)
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

        out = out.reset_index(drop=True)

        ## Build freq_map from column naming convention.
        ## Columns ending in "Monthly" are broadcast monthly features — EDA must
        ## downsample them to true monthly frequency before running statistical tests.
        ## All other columns are treated as weekly.
        freq_map = {
            col: ("monthly" if col.endswith("Monthly") else "weekly")
            for col in out.columns
        }

        return out, freq_map

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
