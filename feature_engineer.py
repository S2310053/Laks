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
        "EURNOK_Weekly":                              0,   # Last Friday's closing rate, available by Wednesday
        "USDNOK_Weekly":                              0,   # Last Friday's closing rate, available by Wednesday
        "Protein_Pig_EUR_100_kg_Weekly":              0,   # Last Friday's price, available by Wednesday
        "Equity_MOWI_NOK_Weekly":                     0,
        "Equity_SALMAR_NOK_Weekly":                   0,
        "Commodity_Brent_COA_NOK_bbl_Weekly":         0,   # Wednesday Bloomberg close
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
        "Protein_Broiler_EUR_100_kg_Weekly":          1,   # EU Commission publishes following Wednesday
        "Salmon_Escapes_Rep_Escaped_Weekly":          1,   # 24h statutory reporting window
        "Salmon_Escapes_Recapture_Weekly":            1,
        "Salmon_Escapes_Avg_Wt_Grams_Weekly":         1}

    PUBLISH_LAG_WEEKS = _zeroWeek | _oneWeek

    ## Defines monthly variables with calendar-exact publication lags.
    #
    #  Each value is the number of calendar days after the END OF THE
    #  REFERENCE MONTH when the data becomes publicly available.
    #  This is the EOMONTH offset — it mirrors Excel's =EOMONTH([@Date],0)+N.
    #
    #  IMPORTANT: these are NOT "days from the reference Wednesday".
    #  The pipeline doc reports median days from Wednesday (25, 35, 55).
    #  Conversion: EOMONTH_days = Wednesday_days − 16.5
    #  where 16.5 = average days remaining in month at a mid-month Wednesday.
    #
    #  Example: biomass = 35 days from Wednesday → 35 − 16.5 = 18.5 → 20 days from EOMONTH.
    #
    #  Implementation: _lagByPublication uses merge_asof to assign each row
    #  the most recently published monthly value, defined as the latest month M
    #  whose publish Wednesday (= EOMONTH(M) + N, snapped to next Wednesday)
    #  falls on or before the row's date.
    #
    PUBLISH_LAG_EOMONTH = {

        # SSB releases ~10th of following month
        # Wednesday_days ≈ 25  →  EOMONTH_days = 25 − 16.5 ≈ 10  (exact: SSB publishes on the 10th)
        "CPI_Norway_Monthly":                         10,
        "Protein_Meat_Inflation_YoY_Monthly":         10,  # Eurostat HICP; same schedule

        # Fiskeridirektoratet biomass panel, releases ~20th of following month
        # Wednesday_days ≈ 35  →  EOMONTH_days = 35 − 16.5 ≈ 20
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

        # UN Comtrade multi-step pipeline (SSB → customs → Comtrade)
        # Wednesday_days ≈ 55  →  EOMONTH_days = 55 − 16.5 ≈ 39
        "Salmon_Export_Net_Weight_Kg_Monthly":        39,
        "Salmon_Export_Value_USD_Monthly":            39,
        "Salmon_Export_Avg_Price_USD_Kg_Monthly":     39,
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
