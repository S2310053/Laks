##
#  EDA — Summary statistics and correlation matrix for the Factors feature matrix.
#
#  Input:  Factors DataFrame from FeatureEngineer.buildFeatureMatrix()
#  Output: summary statistics table (console) + correlation heatmap (PDF)
##

import pandas as pd
import numpy  as np
import matplotlib.pyplot as plt
import seaborn as sns


class EDA:

    _META   = ["Date"]
    _TARGET = "Y 0w ∆ Salmon (NOK/KG)"
    SEP     = "═" * 72

    ##
    #  @param factors   DataFrame — output of FeatureEngineer.buildFeatureMatrix()
    #  @param freq_map  dict {col: "monthly" | "weekly"} — from FeatureEngineer.
    #                   Monthly columns are broadcast to weekly; EDA downsamples
    #                   them to true monthly frequency for summary statistics.
    #
    def __init__(self, factors: pd.DataFrame, freq_map: dict = None):

        self._factors  = factors.copy()
        self._freq_map = freq_map or {}

        if self._TARGET not in self._factors.columns:
            raise ValueError(f"Target column '{self._TARGET}' not found in Factors.")

        self.y      = self._factors[self._TARGET].copy()
        self.y.name = "y"

        _drop  = [c for c in self._META + [self._TARGET] if c in self._factors.columns]
        self.X = self._factors.drop(columns=_drop)

        self._fp_mask = self.y.notna()

        self._valid_cols = [
            c for c in self.X.columns
            if self.X.loc[self._fp_mask, c].notna().sum() >= 52
        ]

        self._monthly_cols = [c for c in self._valid_cols
                              if self._freq_map.get(c) == "monthly"]

    def _toMonthly(self, col: str) -> pd.Series:
        df = pd.DataFrame({
            "Date": self._factors.loc[self._fp_mask, "Date"],
            "val" : self.X.loc[self._fp_mask, col],
        }).dropna().set_index("Date")
        return df["val"].resample("ME").last().dropna()

    ##
    #  Summary statistics for all valid features.
    #  Monthly columns use true monthly values (avoids inflating N ~4x).
    #  @return  DataFrame (mean, std, skew, kurt, min, max, %NaN, %zero)
    #
    def summaryStatistics(self) -> pd.DataFrame:

        print(f"\n{self.SEP}")
        print(f"  SUMMARY STATISTICS")
        print(f"{self.SEP}\n")

        rows = []
        for col in self._valid_cols:
            s = self._toMonthly(col).values if col in self._monthly_cols \
                else self.X.loc[self._fp_mask, col].dropna().values
            s = pd.Series(s)
            rows.append({
                "Column"   : col,
                "Mean"     : round(s.mean(),     4),
                "Std"      : round(s.std(),      4),
                "Skew"     : round(s.skew(),     4),
                "Kurt"     : round(s.kurtosis(), 4),
                "Min"      : round(s.min(),      4),
                "Max"      : round(s.max(),      4),
                "pct_NaN"  : round(self.X.loc[self._fp_mask, col].isna().mean()  * 100, 1),
                "pct_Zero" : round((self.X.loc[self._fp_mask, col] == 0).mean()  * 100, 1),
            })

        summary = pd.DataFrame(rows).set_index("Column")
        print(summary.to_string())
        print(f"\n{self.SEP}\n")
        return summary

    ##
    #  Correlation matrix heatmap (features + y_t as last row/column).
    #  @param save_path  output PDF path
    #  @return  full correlation DataFrame
    #
    def correlationMatrix(self, save_path: str = "eda_correlation_matrix.pdf") -> pd.DataFrame:

        print(f"\n{self.SEP}")
        print(f"  CORRELATION MATRIX")
        print(f"{self.SEP}\n")

        X_fp = self.X.loc[self._fp_mask, self._valid_cols].copy()
        X_fp = X_fp.dropna(axis=1, thresh=int(self._fp_mask.sum() * 0.5)).dropna()

        if X_fp.empty:
            print(f"  ⚠ No complete rows after listwise deletion — skipped.")
            return pd.DataFrame()

        _Y_LABEL  = "── y_t (target)"
        y_aligned = self.y.loc[X_fp.index].rename(_Y_LABEL)
        corr      = pd.concat([X_fp, y_aligned], axis=1).corr()

        fig, ax = plt.subplots(figsize=(16, 14))
        sns.heatmap(corr, cmap="RdBu_r", center=0, vmin=-1, vmax=1,
                    ax=ax, annot=False, linewidths=0.3)
        ax.set_title("Correlation Matrix — Features + y_t (target in last row/column)")
        plt.tight_layout()
        plt.savefig(save_path)
        plt.close()
        print(f"  Saved: {save_path}")
        print(f"\n{self.SEP}\n")
        return corr

    ##
    #  Runs summaryStatistics() then correlationMatrix().
    #
    def report(self) -> None:
        print(f"\n{'█'*72}")
        print(f"  EDA — Salmon Price Forecasting")
        print(f"  FishPool period: {int(self._fp_mask.sum())} obs  |  "
              f"Valid features: {len(self._valid_cols)}")
        print(f"{'█'*72}")

        self.summaryStatistics()
        self.correlationMatrix()
