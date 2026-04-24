##
#  EDA — Exploratory Data Analysis for the Factors feature matrix.
#
#  Outputs saved to Results/EDA/:
#    1. Summary statistics  → summary_statistics.csv + .pdf
#    2. Correlation matrix  → correlation_matrix.pdf
#    3. Missing obs.        → missing_observations.pdf + .csv
#    4. Stationarity (ADF)  → stationarity.csv + stationarity_failures.pdf
#    5. Target horizons     → target_horizons.pdf  (TS+outliers, dist, ACF, PACF per Y)
#    6. Feature plots       → feature_plots.pdf    (TS+outliers, dist, ACF, PACF per feature)
#    7. Autocorrelation     → autocorrelation.csv
#    8. Seasonal decomp.    → seasonal_decomposition.pdf
#
#  Usage:  python eda.py                      (standalone)
#          EDA(df, freq_map).report()         (imported)
##

import os
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.gridspec             import GridSpec
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
import numpy  as np
import seaborn as sns
from scipy import stats
from statsmodels.tsa.stattools    import adfuller, acf, pacf
from statsmodels.stats.diagnostic import acorr_ljungbox, het_arch
from statsmodels.tsa.seasonal     import STL

# ── global style ──────────────────────────────────────────────────────────────
plt.rcParams["font.family"]         = "Times New Roman"
plt.rcParams["mathtext.fontset"]    = "custom"
plt.rcParams["mathtext.rm"]         = "Times New Roman"
plt.rcParams["mathtext.it"]         = "Times New Roman:italic"
plt.rcParams["mathtext.bf"]         = "Times New Roman:bold"

# ── palette ───────────────────────────────────────────────────────────────────
_BLUE   = "#1A6B8A"
_RED    = "#C4654A"
_TEAL   = "#2A9D8F"
_DARK   = "#0D3B5E"
_LIGHT  = "#A8C8D8"
_GREY   = "dimgrey"


class EDA:

    _META       = ["Date"]
    _TARGET     = "Y 0w ∆ Salmon (NOK/KG)"
    _Y_PREFIX   = "Y "
    SEP         = "═" * 72
    RESULTS_DIR = os.path.join("Results", "EDA")

    _HDR_COLOR  = _DARK
    _ROW_COLOR  = "#f0f4f8"

    def __init__(self, factors: pd.DataFrame, freq_map: dict = None):

        self._factors  = factors.copy()
        self._freq_map = freq_map or {}
        os.makedirs(self.RESULTS_DIR, exist_ok=True)

        if self._TARGET not in self._factors.columns:
            raise ValueError(f"Target column '{self._TARGET}' not found in Factors.")

        self.y      = self._factors[self._TARGET].copy()
        self.y.name = "y"

        self._y_cols = [c for c in self._factors.columns
                        if c.startswith(self._Y_PREFIX)]

        _drop  = [c for c in self._META + self._y_cols if c in self._factors.columns]
        self.X = self._factors.drop(columns=_drop)

        self._fp_mask = self.y.notna()

        self._valid_cols = [
            c for c in self.X.columns
            if self.X.loc[self._fp_mask, c].notna().sum() >= 52
        ]

        self._monthly_cols = [c for c in self._valid_cols
                              if self._freq_map.get(c) == "monthly"]

    # ── helpers ───────────────────────────────────────────────────────────────

    def _path(self, filename: str) -> str:
        return os.path.join(self.RESULTS_DIR, filename)

    def _toMonthly(self, col: str) -> pd.Series:
        df = pd.DataFrame({
            "Date": self._factors.loc[self._fp_mask, "Date"],
            "val" : self.X.loc[self._fp_mask, col],
        }).dropna().set_index("Date")
        return df["val"].resample("ME").last().dropna()

    def _dates(self, index) -> np.ndarray:
        if "Date" in self._factors.columns:
            return self._factors.loc[index, "Date"].values
        return index.values

    def _style(self, ax, xlabel="", ylabel="", grid_alpha=0.1):
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(True, linestyle="--", alpha=grid_alpha)
        if xlabel:
            ax.set_xlabel(xlabel, fontweight="bold")
        if ylabel:
            ax.set_ylabel(ylabel, fontweight="bold")

    def _ann(self, ax, text, loc="upper left"):
        x  = 0.03 if "left" in loc else 0.97
        ha = "left" if "left" in loc else "right"
        y  = 0.97 if "upper" in loc else 0.03
        va = "top" if "upper" in loc else "bottom"
        ax.text(x, y, text, transform=ax.transAxes, fontsize=8.5,
                ha=ha, va=va,
                bbox=dict(boxstyle="round,pad=0.4", facecolor="white",
                          alpha=0.85, edgecolor="none"))

    def _footer(self, fig, left="", right=""):
        if left:
            fig.text(0.01, 0.01, left, fontsize=7.5, ha="left", va="bottom",
                     style="italic", color=_GREY)
        if right:
            fig.text(0.99, 0.01, right, fontsize=7.5, ha="right", va="bottom",
                     style="italic", color=_GREY)

    def _save_table_pdf(self, df: pd.DataFrame, title: str, path: str) -> None:
        nrows, ncols = df.shape
        fig, ax = plt.subplots(figsize=(min(28, max(10, ncols * 1.8)),
                                        max(2.5, 0.32 * nrows + 1.2)))
        ax.axis("off")
        tbl = ax.table(cellText=df.values, colLabels=df.columns,
                       cellLoc="center", loc="center")
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(8)
        tbl.auto_set_column_width(col=list(range(ncols)))
        for (r, c), cell in tbl.get_celld().items():
            if r == 0:
                cell.set_facecolor(self._HDR_COLOR)
                cell.set_text_props(color="white", fontweight="bold")
            elif r % 2 == 0:
                cell.set_facecolor(self._ROW_COLOR)
        ax.set_title(title, fontsize=11, fontweight="bold", pad=12)
        plt.tight_layout()
        plt.savefig(path, format="pdf", bbox_inches="tight")
        plt.close()
        print(f"  Saved: {path}")

    # ── 1. Summary statistics ─────────────────────────────────────────────────

    def summaryStatistics(self) -> pd.DataFrame:

        print(f"\n{self.SEP}\n  SUMMARY STATISTICS\n{self.SEP}\n")

        rows = []

        # Y target horizons first
        for col in self._y_cols:
            if col not in self._factors.columns:
                continue
            s = self._factors[col].dropna()
            rows.append({
                "Column"   : col,
                "Type"     : "Target",
                "Mean"     : round(s.mean(),     4),
                "Std"      : round(s.std(),      4),
                "Skew"     : round(s.skew(),     4),
                "Kurt"     : round(s.kurtosis(), 4),
                "Min"      : round(s.min(),      4),
                "Max"      : round(s.max(),      4),
                "pct_NaN"  : round(s.isna().mean() * 100, 1),
                "pct_Zero" : round((s == 0).mean() * 100, 1),
            })

        # Features
        for col in self._valid_cols:
            s = pd.Series(
                self._toMonthly(col).values if col in self._monthly_cols
                else self.X.loc[self._fp_mask, col].dropna().values
            )
            rows.append({
                "Column"   : col,
                "Type"     : "Feature",
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

        summary.to_csv(self._path("summary_statistics.csv"))
        print(f"  Saved: {self._path('summary_statistics.csv')}")

        disp = summary.reset_index().apply(
            lambda col: col.map(lambda x: f"{x:.4f}" if isinstance(x, float) else x))
        self._save_table_pdf(disp, "Summary Statistics — Target Horizons + Features",
                             self._path("summary_statistics.pdf"))
        return summary

    # ── 2. Correlation matrix ─────────────────────────────────────────────────

    def correlationMatrix(self) -> pd.DataFrame:

        print(f"\n{self.SEP}\n  CORRELATION MATRIX\n{self.SEP}\n")

        X_fp = self.X.loc[self._fp_mask, self._valid_cols].copy()
        X_fp = X_fp.dropna(axis=1, thresh=int(self._fp_mask.sum() * 0.5)).dropna()

        if X_fp.empty:
            print("  ⚠ No complete rows after listwise deletion — skipped.")
            return pd.DataFrame()

        y_label   = self._TARGET
        y_aligned = self.y.loc[X_fp.index].rename(y_label)
        corr      = pd.concat([X_fp, y_aligned], axis=1).corr()
        labels    = corr.columns.tolist()

        n = len(corr)
        fig, ax = plt.subplots(figsize=(max(18, n * 0.22), max(16, n * 0.2)))
        sns.heatmap(corr, cmap="RdBu_r", center=0, vmin=-1, vmax=1,
                    ax=ax, annot=False, linewidths=0.2,
                    xticklabels=labels, yticklabels=labels)
        ax.set_title(f"Correlation Matrix — Features + {y_label}",
                     fontweight="bold", fontsize=12)
        ax.tick_params(axis="both", labelsize=5)
        plt.setp(ax.get_xticklabels(), rotation=90, ha="right")
        plt.setp(ax.get_yticklabels(), rotation=0)
        self._footer(fig, right=f"→ Target: {y_label}  (last row/column)")
        plt.tight_layout()
        path = self._path("correlation_matrix.pdf")
        plt.savefig(path, format="pdf", bbox_inches="tight")
        plt.close()
        print(f"  Saved: {path}\n{self.SEP}\n")
        return corr

    # ── 3. Missing observations ───────────────────────────────────────────────

    def missingObservations(self) -> pd.DataFrame:

        print(f"\n{self.SEP}\n  MISSING OBSERVATIONS\n{self.SEP}\n")

        pct_nan = (self.X.loc[self._fp_mask].isna().mean() * 100).sort_values(ascending=False)
        colors  = [_RED if v > 50 else _RED if v > 20 else _TEAL for v in pct_nan]
        colors  = [_RED if v > 50 else "#E8925A" if v > 20 else _TEAL for v in pct_nan]

        fig, ax = plt.subplots(figsize=(14, max(8, len(pct_nan) * 0.22)))
        ax.barh(pct_nan.index[::-1], pct_nan.values[::-1],
                color=colors[::-1], alpha=0.85)
        ax.axvline(50, color=_RED,    lw=1.2, ls="--", label=">50%  high concern")
        ax.axvline(20, color="#E8925A", lw=1.2, ls="--", label=">20%  moderate concern")
        ax.set_title(f"Missing Observations by Feature  (n={int(self._fp_mask.sum())} weeks)",
                     fontweight="bold", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        self._style(ax, xlabel="% Missing")
        self._footer(fig, right="→ Features above 50% are candidates for exclusion")
        plt.tight_layout()
        path = self._path("missing_observations.pdf")
        plt.savefig(path, format="pdf", bbox_inches="tight")
        plt.close()
        print(f"  Saved: {path}")

        result = pct_nan.reset_index()
        result.columns = ["Feature", "pct_NaN"]
        result.to_csv(self._path("missing_observations.csv"), index=False)
        print(f"  Saved: {self._path('missing_observations.csv')}\n{self.SEP}\n")
        return result

    # ── 4. Stationarity (ADF) ─────────────────────────────────────────────────

    def stationarityTests(self) -> pd.DataFrame:

        print(f"\n{self.SEP}\n  STATIONARITY TESTS (ADF)\n{self.SEP}\n")

        rows = []
        for col in self._valid_cols:
            s = self._toMonthly(col) if col in self._monthly_cols \
                else self.X.loc[self._fp_mask, col].dropna()
            if len(s) < 20:
                continue
            try:
                adf_stat, p_val, _, _, crit, _ = adfuller(s, autolag="AIC")
                rows.append({
                    "Feature" : col,
                    "ADF"     : round(adf_stat,    3),
                    "p-value" : round(p_val,        4),
                    "Crit 1%" : round(crit["1%"],   3),
                    "Crit 5%" : round(crit["5%"],   3),
                    "Result"  : "I(0) ok" if p_val < 0.05 else "! Non-stationary",
                })
            except Exception:
                continue

        df_adf  = pd.DataFrame(rows)
        n_pass  = (df_adf["Result"] == "I(0) ✓").sum()
        n_total = len(df_adf)
        print(f"  {n_pass} / {n_total} features reject unit root at 5%")

        df_adf.to_csv(self._path("stationarity.csv"), index=False)
        print(f"  Full results: {self._path('stationarity.csv')}")

        failures = df_adf[df_adf["Result"] != "I(0) ok"].copy()
        if failures.empty:
            failures = pd.DataFrame([{"Feature": "All features stationary at 5%",
                                       "ADF": "—", "p-value": "—",
                                       "Crit 1%": "—", "Crit 5%": "—", "Result": "ok"}])
        self._save_table_pdf(
            failures,
            f"ADF Stationarity — Non-Stationary Features\n"
            f"{n_pass}/{n_total} reject H₀ at 5%  |  Full table: stationarity.csv",
            self._path("stationarity_failures.pdf"),
        )
        print(f"\n{self.SEP}\n")
        return df_adf

    # ── 5. Target horizon plots ───────────────────────────────────────────────
    #
    #  One page per Y horizon:
    #    Row 1 (full width) : time series
    #    Row 2, col 1       : distribution + fitted normal + KDE
    #    Row 2, col 2       : ACF
    #    Row 2, col 3       : PACF
    #
    def targetHorizonPlots(self) -> None:

        print(f"\n{self.SEP}\n  TARGET HORIZON PLOTS\n{self.SEP}\n")

        y_cols = [c for c in self._y_cols if c in self._factors.columns]
        path   = self._path("target_horizons.pdf")

        with PdfPages(path) as pdf:
            for col in y_cols:
                s = self._factors[col].dropna()
                if len(s) < 20:
                    continue

                dates = self._dates(s.index)
                label = col.replace("∆ Salmon (NOK/KG)", "").strip()
                n_obs = len(s)

                _, jb_p = stats.jarque_bera(s)
                try:
                    _, lm_p, _, _ = het_arch(s, nlags=4)
                    arch_str = f"ARCH p={'<0.001' if lm_p < 0.001 else f'{lm_p:.3f}'}"
                except Exception:
                    arch_str = "ARCH: n/a"

                fig = plt.figure(figsize=(16, 10), facecolor="white")
                fig.suptitle(col, fontsize=13, fontweight="bold", y=0.99)
                gs  = GridSpec(2, 3, figure=fig, hspace=0.48, wspace=0.35)

                # ── time series ───────────────────────────────────────────────
                ax_ts = fig.add_subplot(gs[0, :])
                ax_ts.plot(dates, s.values, color=_BLUE, lw=1.2, alpha=0.85)
                ax_ts.axhline(0, color=_GREY, lw=0.8, ls="--")
                z_s = (s - s.mean()) / s.std()
                out_mask = z_s.abs() > 3
                if out_mask.any():
                    ax_ts.scatter(dates[out_mask.values], s.values[out_mask.values],
                                  color=_RED, zorder=5, s=35,
                                  label=f"Outliers |z|>3 (n={int(out_mask.sum())})")
                    ax_ts.legend(frameon=False, fontsize=8)
                ax_ts.set_title("Time Series", fontweight="bold", fontsize=10)
                ax_ts.xaxis.set_major_locator(mdates.YearLocator(2))
                ax_ts.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
                self._style(ax_ts, ylabel="∆ Price (NOK/kg)")

                # ── distribution + normal + KDE ───────────────────────────────
                ax_dist = fig.add_subplot(gs[1, 0])
                ax_dist.hist(s, bins=40, density=True,
                             color=_BLUE, alpha=0.20, edgecolor="white")
                kde_x = np.linspace(s.min(), s.max(), 200)
                ax_dist.plot(kde_x, stats.norm.pdf(kde_x, s.mean(), s.std()),
                             color=_RED, lw=1.8, ls="--", label="Normal")
                jb_str = "< 0.001" if jb_p < 0.001 else f"{jb_p:.3f}"
                self._ann(ax_dist,
                          f"JB p = {jb_str}\n{arch_str}\n"
                          f"Skew = {float(s.skew()):.3f}\n"
                          f"Kurt = {float(s.kurtosis()):.3f}")
                ax_dist.set_title("Distribution", fontweight="bold", fontsize=10)
                ax_dist.legend(frameon=False, fontsize=8)
                self._style(ax_dist, xlabel="Return", ylabel="Density")

                # ── ACF ───────────────────────────────────────────────────────
                ax_acf  = fig.add_subplot(gs[1, 1])
                max_lag = min(52, n_obs // 2 - 1)
                _plot_correlogram(ax_acf, acf(s, nlags=max_lag), n_obs,
                                  f"ACF  ({label})")

                # ── PACF ──────────────────────────────────────────────────────
                ax_pacf = fig.add_subplot(gs[1, 2])
                _plot_correlogram(ax_pacf, pacf(s, nlags=min(max_lag, n_obs // 4)),
                                  n_obs, f"PACF  ({label})")

                self._footer(fig,
                             right="→ JB: Jarque-Bera  |  ARCH: volatility clustering  |  CI: ±1.96/√n")
                pdf.savefig(fig, bbox_inches="tight")
                plt.close(fig)
                print(f"    {label}")

        print(f"  Saved: {path}\n{self.SEP}\n")

    # ── 5b. Feature plots — one page per feature ─────────────────────────────
    #
    #  Same layout as targetHorizonPlots:
    #    Row 1 (full width) : time series + outliers (|z|>3)
    #    Row 2, col 1       : distribution + KDE + normal
    #    Row 2, col 2       : ACF
    #    Row 2, col 3       : PACF
    #
    def featurePlots(self) -> None:

        print(f"\n{self.SEP}\n  FEATURE PLOTS (all features)\n{self.SEP}\n")

        path = self._path("feature_plots.pdf")

        with PdfPages(path) as pdf:
            for col in self._valid_cols:
                s = self.X.loc[self._fp_mask, col].dropna()
                if len(s) < 20:
                    continue

                dates_all = self._factors.loc[self._fp_mask, "Date"] if "Date" in self._factors.columns \
                            else self._factors.index[self._fp_mask]
                s_aligned = s.reindex(self._factors.loc[self._fp_mask].index)
                dates_np  = dates_all.values if hasattr(dates_all, "values") else np.array(dates_all)
                n_obs     = len(s)

                _, jb_p = stats.jarque_bera(s)
                try:
                    _, lm_p, _, _ = het_arch(s.values, nlags=4)
                    arch_str = f"ARCH p={'<0.001' if lm_p < 0.001 else f'{lm_p:.3f}'}"
                except Exception:
                    arch_str = "ARCH: n/a"

                fig = plt.figure(figsize=(16, 10), facecolor="white")
                fig.suptitle(col, fontsize=11, fontweight="bold", y=0.99)
                gs  = GridSpec(2, 3, figure=fig, hspace=0.48, wspace=0.35)

                # ── time series ───────────────────────────────────────────────
                ax_ts = fig.add_subplot(gs[0, :])
                ax_ts.plot(dates_np, s_aligned.values, color=_BLUE, lw=1.2, alpha=0.85)
                ax_ts.axhline(0, color=_GREY, lw=0.8, ls="--")
                z_s = (s - s.mean()) / s.std()
                out_idx  = z_s[z_s.abs() > 3].index
                out_mask_full = s_aligned.index.isin(out_idx)
                if out_mask_full.any():
                    ax_ts.scatter(dates_np[out_mask_full],
                                  s_aligned.values[out_mask_full],
                                  color=_RED, zorder=5, s=35,
                                  label=f"Outliers |z|>3 (n={int(out_mask_full.sum())})")
                    ax_ts.legend(frameon=False, fontsize=8)
                ax_ts.set_title("Time Series", fontweight="bold", fontsize=10)
                ax_ts.xaxis.set_major_locator(mdates.YearLocator(2))
                ax_ts.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
                self._style(ax_ts)

                # ── distribution ─────────────────────────────────────────────
                ax_dist = fig.add_subplot(gs[1, 0])
                ax_dist.hist(s, bins=40, density=True,
                             color=_BLUE, alpha=0.20, edgecolor="white")
                kde_x = np.linspace(s.min(), s.max(), 200)
                ax_dist.plot(kde_x, stats.norm.pdf(kde_x, s.mean(), s.std()),
                             color=_RED, lw=1.8, ls="--", label="Normal")
                jb_str = "< 0.001" if jb_p < 0.001 else f"{jb_p:.3f}"
                self._ann(ax_dist,
                          f"JB p = {jb_str}\n{arch_str}\n"
                          f"Skew = {float(s.skew()):.3f}\n"
                          f"Kurt = {float(s.kurtosis()):.3f}")
                ax_dist.set_title("Distribution", fontweight="bold", fontsize=10)
                ax_dist.legend(frameon=False, fontsize=8)
                self._style(ax_dist, xlabel="Value", ylabel="Density")

                # ── ACF ───────────────────────────────────────────────────────
                ax_acf  = fig.add_subplot(gs[1, 1])
                max_lag = min(52, n_obs // 2 - 1)
                _plot_correlogram(ax_acf, acf(s, nlags=max_lag), n_obs,
                                  f"ACF  ({col[:40]})")

                # ── PACF ──────────────────────────────────────────────────────
                ax_pacf = fig.add_subplot(gs[1, 2])
                _plot_correlogram(ax_pacf, pacf(s, nlags=min(max_lag, n_obs // 4)),
                                  n_obs, f"PACF  ({col[:40]})")

                self._footer(fig,
                             right="→ JB: Jarque-Bera  |  ARCH: volatility clustering  |  CI: ±1.96/√n")
                pdf.savefig(fig, bbox_inches="tight")
                plt.close(fig)
                print(f"    {col}")

        print(f"  Saved: {path}\n{self.SEP}\n")

    # ── 6. Autocorrelation — all features ─────────────────────────────────────

    def autocorrelationTests(self) -> pd.DataFrame:

        print(f"\n{self.SEP}\n  AUTOCORRELATION TESTS (LJUNG-BOX)\n{self.SEP}\n")

        TEST_LAGS = [4, 13, 26, 52]
        rows = []

        for col in self._valid_cols:
            s = self._toMonthly(col) if col in self._monthly_cols \
                else self.X.loc[self._fp_mask, col].dropna()
            if len(s) < 60:
                continue
            try:
                lb  = acorr_ljungbox(s, lags=TEST_LAGS, return_df=True)
                row = {"Feature": col}
                for lag in TEST_LAGS:
                    p = lb.loc[lag, "lb_pvalue"] if lag in lb.index else np.nan
                    row[f"p ({lag}w)"] = round(float(p), 4) if not np.isnan(p) else np.nan
                rows.append(row)
            except Exception:
                continue

        df_lb = pd.DataFrame(rows).set_index("Feature")
        df_lb.to_csv(self._path("autocorrelation.csv"))
        print(f"  Saved: {self._path('autocorrelation.csv')}\n{self.SEP}\n")
        return df_lb

    # ── 7. Seasonal decomposition (STL) ──────────────────────────────────────

    def seasonalDecomposition(self) -> None:

        print(f"\n{self.SEP}\n  SEASONAL DECOMPOSITION (STL, period=52)\n{self.SEP}\n")

        y_clean = self.y.loc[self._fp_mask].dropna()
        if len(y_clean) < 104:
            print("  ⚠ Fewer than 2 full years — STL skipped.")
            return

        result = STL(y_clean, period=52, robust=True).fit()
        dates  = self._dates(y_clean.index)

        colors = [_DARK, _BLUE, _TEAL, _RED]
        titles = ["Observed", "Trend", "Seasonal  (period = 52 weeks)", "Residual"]

        fig, axes = plt.subplots(4, 1, figsize=(14, 14), sharex=True, facecolor="white")
        fig.suptitle("STL Seasonal Decomposition — Y 0w ∆ Salmon (NOK/KG)",
                     fontweight="bold", fontsize=12, linespacing=1.6)

        for ax, data, title, color in zip(
            axes,
            [y_clean, result.trend, result.seasonal, result.resid],
            titles, colors,
        ):
            ax.plot(dates, data.values, color=color, lw=1.2)
            ax.axhline(0, color=_GREY, lw=0.7, ls="--")
            ax.set_title(title, fontweight="bold", fontsize=10)
            ax.xaxis.set_major_locator(mdates.YearLocator(2))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
            self._style(ax, ylabel="∆ Price")

        self._footer(fig, right="→ Seasonal component justifies SARIMA(p,d,q)(P,D,Q)[52]")
        plt.tight_layout()
        path = self._path("seasonal_decomposition.pdf")
        plt.savefig(path, format="pdf", bbox_inches="tight")
        plt.close()
        print(f"  Saved: {path}\n{self.SEP}\n")

    # ── report ────────────────────────────────────────────────────────────────

    def report(self) -> None:
        print(f"\n{'█'*72}")
        print(f"  EDA — Salmon Price Forecasting")
        print(f"  FishPool period : {int(self._fp_mask.sum())} obs")
        print(f"  Valid features  : {len(self._valid_cols)}")
        print(f"  Y horizons      : {len(self._y_cols)}")
        print(f"  Output          : {self.RESULTS_DIR}/")
        print(f"{'█'*72}")

        self.summaryStatistics()
        self.correlationMatrix()
        self.missingObservations()
        self.stationarityTests()
        self.targetHorizonPlots()
        self.featurePlots()
        self.autocorrelationTests()
        self.seasonalDecomposition()


# ── ACF/PACF bar plot — significant bars dark, others light, CI lines in red ─

def _plot_correlogram(ax, vals: np.ndarray, n_obs: int, title: str) -> None:
    ci   = 1.96 / np.sqrt(n_obs)
    lags = np.arange(len(vals))
    bar_colors = [_BLUE if abs(v) > ci else _LIGHT for v in vals]
    ax.bar(lags, vals, color=bar_colors, width=0.6, alpha=0.85)
    ax.axhline( ci, color=_RED, lw=1.2, ls="--", alpha=0.8, label="95% CI")
    ax.axhline(-ci, color=_RED, lw=1.2, ls="--", alpha=0.8)
    ax.axhline(0,   color="black", lw=0.6)
    ax.set_title(title, fontweight="bold", fontsize=9)
    ax.set_xlabel("Lag (weeks)", fontweight="bold", fontsize=8)
    ax.set_ylabel("Autocorrelation", fontweight="bold", fontsize=8)
    ax.legend(frameon=False, fontsize=7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, linestyle="--", alpha=0.1)


if __name__ == "__main__":
    df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
    freq_map = {col: ("monthly" if col.endswith("Monthly") else "weekly")
                for col in df.columns}
    EDA(df, freq_map=freq_map).report()
