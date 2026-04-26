# This module constructs the EDA class

##
# This module defines the Exploratory Data Analysis class, where we aim to identify the characteristics of
# our time series, which are the Y targets ranging from 0w, 1w, 2w, 1m, 3m, 6m to 12m, and each feature
# selected in the feature engineering stage. We generate a summary of the statistics of these variables, identify
# the percentage of missing values and outliers, and create a correlation matrix among them. We then validate that our
# current selection passes both ADF and KPSS stationarity tests, test for normality with Jarque-Bera, run the Ljung-Box
# test for autocorrelation, identify ARCH effects of volatility clustering across features, and finally,
# assuming that salmon prices are seasonal, we perform seasonal decomposition to the annual cycle produced over 52 weeks
# All results are saved in files inside the Results/EDA directory
##

import os
import warnings
from statsmodels.tools.sm_exceptions import InterpolationWarning
warnings.filterwarnings("ignore", category=InterpolationWarning)
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
from statsmodels.tsa.stattools    import adfuller, kpss, acf, pacf
from statsmodels.stats.diagnostic import acorr_ljungbox, het_arch
from statsmodels.tsa.seasonal     import STL

# Plot font
plt.rcParams["font.family"]      = "Times New Roman"
plt.rcParams["mathtext.fontset"] = "custom"
plt.rcParams["mathtext.rm"]      = "Times New Roman"
plt.rcParams["mathtext.it"]      = "Times New Roman:italic"
plt.rcParams["mathtext.bf"]      = "Times New Roman:bold"

# Plot Colour
_BLUE  = "#1A6B8A"
_RED   = "#C4654A"
_TEAL  = "#2A9D8F"
_DARK  = "#0D3B5E"
_LIGHT = "#A8C8D8"
_GREY  = "dimgrey"


# Runs all EDA diagnostics on the Factors feature matrix
class EDA:

    # Identify  non feature columns (such as date), and target variables, plus printing results formating and saving directories
    META_COLS     = ["Date"]
    TARGET_COL    = "Y 0w ∆ Salmon (NOK/KG)"
    TARGET_PREFIX = "Y "
    SEP           = "═" * 72
    RESULTS_DIR   = os.path.join("Results", "EDA")
    HDR_COLOR     = _DARK
    ROW_COLOR     = "#f0f4f8"

    # Built at the constructor so every method can use it without re-deriving it
    # Identifies the features that where mapped in weekly and monthly frequencies from the
    # feature engineering stage
    def __init__(self, factors: pd.DataFrame, freqMap: dict = None):

        # Define the matrix of factors and frequency identifier masks
        self._factorMatrix = factors.copy()
        self._freqMap      = freqMap or {}
        os.makedirs(self.RESULTS_DIR, exist_ok=True)

        # Validates that the current target (Y 0w ∆ Salmon (NOK/KG)) is in the data set
        if self.TARGET_COL not in self._factorMatrix.columns:
            raise ValueError(f"Target column '{self.TARGET_COL}' not found in Factors.")

        # Place an identifier to the target as y for clarity
        self._y      = self._factorMatrix[self.TARGET_COL].copy()
        self._y.name = "y"

        # Identifies the horizon y targets by horizon (0w, 1w, 2w, 1m, 3m, 6m, 12m)
        self._targetCols = [c for c in self._factorMatrix.columns if c.startswith(self.TARGET_PREFIX)]

        # Feature matrix as X, without the dates
        colsToDrop  = [c for c in self.META_COLS + self._targetCols if c in self._factorMatrix.columns]
        self._X     = self._factorMatrix.drop(columns=colsToDrop)

        # Use the observation were the target is present
        self._targetMask = self._y.notna()

        # Only keep features with observations greater or equal than a year
        self._validCols = [
            c for c in self._X.columns
            if self._X.loc[self._targetMask, c].notna().sum() >= 52
        ]

        # Downsamples monthly features that repeated themselves across weeks to their original set up in months
        self._monthlyCols = [c for c in self._validCols if self._freqMap.get(c) == "monthly"]

    # Returns the path for a given filename
    def _path(self, filename: str) -> str:
        return os.path.join(self.RESULTS_DIR, filename)

    # Downsamples a monthly-broadcast that was in weeks back to months
    def _toMonthly(self, column: str) -> pd.Series:
        monthlyData = pd.DataFrame({
            "Date": self._factorMatrix.loc[self._targetMask, "Date"],
            "val" : self._X.loc[self._targetMask, column],
        }).dropna().set_index("Date")
        return monthlyData["val"].resample("ME").last().dropna()

    # Returns dates as a numpy array
    def _dates(self, index) -> np.ndarray:
        if "Date" in self._factorMatrix.columns:
            return self._factorMatrix.loc[index, "Date"].values
        return index.values

    # Takes out top and right axis frame lines in plots, adds a grid and bold font
    def _style(self, ax, xlabel="", ylabel="", grid_alpha=0.1):
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(True, linestyle="--", alpha=grid_alpha)
        if xlabel:
            ax.set_xlabel(xlabel, fontweight="bold")
        if ylabel:
            ax.set_ylabel(ylabel, fontweight="bold")

    # Adds a floating text box with statistics inside a plot
    def _ann(self, ax, text, loc="upper left"):
        xPos  = 0.03 if "left" in loc else 0.97
        hAlign = "left" if "left" in loc else "right"
        yPos  = 0.97 if "upper" in loc else 0.03
        vAlign = "top"  if "upper" in loc else "bottom"
        ax.text(xPos, yPos, text, transform=ax.transAxes, fontsize=8.5, ha=hAlign, va=vAlign,
                bbox=dict(boxstyle="round,pad=0.4", facecolor="white", alpha=0.85, edgecolor="none"))

    # Render a PDF for the DataFrames output results
    def _save_table_pdf(self, df: pd.DataFrame, title: str, path: str) -> None:
        nRows, nCols = df.shape
        fig, ax = plt.subplots(figsize=(min(28, max(10, nCols * 1.8)), max(2.5, 0.32 * nRows + 1.2)))
        ax.axis("off")
        table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc="center", loc="center")
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.auto_set_column_width(col=list(range(nCols)))
        for (rowIdx, colIdx), cell in table.get_celld().items():
            if rowIdx == 0:
                cell.set_facecolor(self.HDR_COLOR)
                cell.set_text_props(color="white", fontweight="bold")
            elif rowIdx % 2 == 0:
                cell.set_facecolor(self.ROW_COLOR)
        ax.set_title(title, fontsize=11, fontweight="bold", pad=12)
        plt.tight_layout()
        plt.savefig(path, format="pdf", bbox_inches="tight")
        plt.close()
        print(f"  Saved: {path}")

    # Builds a summary statistics dictionary
    def _statsRow(self, column: str, series: pd.Series, rawSeries: pd.Series, rowType: str) -> dict:
        return {
            "Column"   : column,
            "Type"     : rowType,
            "Mean"     : round(series.mean(),                    4),
            "Std"      : round(series.std(),                     4),
            "Skew"     : round(series.skew(),                    4),
            "Kurt"     : round(series.kurtosis(),                4),
            "Min"      : round(series.min(),                     4),
            "Max"      : round(series.max(),                     4),
            "pct_NaN"  : round(rawSeries.isna().mean()  * 100,  1),
            "pct_Zero" : round((rawSeries == 0).mean()  * 100,  1),
        }

    # Returns the summary statistics for targets and features, in addition to a csv output results file and pdf
    def summaryStatistics(self) -> pd.DataFrame:

        print(f"\n{self.SEP}\n  SUMMARY STATISTICS\n{self.SEP}\n")

        rows = []

        # Targets
        for column in self._targetCols:
            if column not in self._factorMatrix.columns:
                continue
            rawSeries = self._factorMatrix[column]
            rows.append(self._statsRow(column, rawSeries.dropna(), rawSeries, "Target"))

        # Features
        for column in self._validCols:
            rawSeries = self._X.loc[self._targetMask, column]
            series    = self._toMonthly(column) if column in self._monthlyCols else rawSeries.dropna()
            rows.append(self._statsRow(column, series, rawSeries, "Feature"))

        summary = pd.DataFrame(rows).set_index("Column")

        summary.to_csv(self._path("summary_statistics.csv"))
        print(f"  Saved: {self._path('summary_statistics.csv')}")

        summaryDisplay = summary.reset_index().apply(
            lambda column: column.map(lambda x: f"{x:.4f}" if isinstance(x, float) else x))
        self._save_table_pdf(summaryDisplay, "Summary Statistics — Target Horizons + Features",
                             self._path("summary_statistics.pdf"))
        return summary

    # Returns a pairwise correlation heatmap of all features and all Y horizon targets
    def correlationMatrix(self) -> pd.DataFrame:

        print(f"\n{self.SEP}\n  CORRELATION MATRIX\n{self.SEP}\n")

        featureMatrix = self._X.loc[self._targetMask, self._validCols].copy()
        featureMatrix = featureMatrix.dropna(axis=1, thresh=int(self._targetMask.sum() * 0.5)).dropna()

        if featureMatrix.empty:
            print("  No complete rows after listwise deletion — skipped.")
            return pd.DataFrame()

        # Append all Y horizon targets as the last columns
        horizonTargets    = self._factorMatrix.loc[featureMatrix.index, self._targetCols].dropna(axis=1, how="all")
        correlationMatrix = pd.concat([featureMatrix, horizonTargets], axis=1).corr()
        labels            = correlationMatrix.columns.tolist()

        nVariables = len(correlationMatrix)
        fig, ax = plt.subplots(figsize=(max(18, nVariables * 0.22), max(16, nVariables * 0.2)))
        sns.heatmap(correlationMatrix, cmap="RdBu_r", center=0, vmin=-1, vmax=1,
                    ax=ax, annot=False, linewidths=0.2,
                    xticklabels=labels, yticklabels=labels)
        ax.set_title("Correlation Matrix — Features + All Y Horizons", fontweight="bold", fontsize=12)
        ax.tick_params(axis="both", labelsize=5)
        plt.setp(ax.get_xticklabels(), rotation=90, ha="right")
        plt.setp(ax.get_yticklabels(), rotation=0)
        plt.tight_layout()
        path = self._path("correlation_matrix.pdf")
        plt.savefig(path, format="pdf", bbox_inches="tight")
        plt.close()
        print(f"  Saved: {path}\n{self.SEP}\n")
        return correlationMatrix

    # Returns the missing observations percentage per feature and its output results in a plod pdf file and csv dataset file
    def missingObservations(self) -> pd.DataFrame:

        print(f"\n{self.SEP}\n  MISSING OBSERVATIONS\n{self.SEP}\n")

        missingPct = (self._X.loc[self._targetMask].isna().mean() * 100).sort_values(ascending=False)
        barColours = [_RED if v > 50 else "#E8925A" if v > 20 else _TEAL for v in missingPct]

        fig, ax = plt.subplots(figsize=(14, max(8, len(missingPct) * 0.22)))
        ax.barh(missingPct.index[::-1], missingPct.values[::-1], color=barColours[::-1], alpha=0.85)
        ax.axvline(50, color=_RED,      lw=1.2, ls="--", label=">50%  high concern")
        ax.axvline(20, color="#E8925A", lw=1.2, ls="--", label=">20%  moderate concern")
        ax.set_title(f"Missing Observations by Feature  (n={int(self._targetMask.sum())} weeks)",
                     fontweight="bold", fontsize=11)
        ax.legend(frameon=False, fontsize=9)
        self._style(ax, xlabel="% Missing")
        plt.tight_layout()
        path = self._path("missing_observations.pdf")
        plt.savefig(path, format="pdf", bbox_inches="tight")
        plt.close()
        print(f"  Saved: {path}")

        result = missingPct.reset_index()
        result.columns = ["Feature", "pct_NaN"]
        result.to_csv(self._path("missing_observations.csv"), index=False)
        print(f"  Saved: {self._path('missing_observations.csv')}\n{self.SEP}\n")
        return result

    # Returns the ADF and KPSS stationarity tests for each feature, saves full results in an csv file
    # The pdf file saved includes the variables that didnt pass both test jointly
    def stationarityTests(self) -> pd.DataFrame:

        print(f"\n{self.SEP}\n  STATIONARITY TESTS (ADF + KPSS)\n{self.SEP}\n")

        rows = []
        for column in self._validCols:
            series = self._toMonthly(column) if column in self._monthlyCols \
                else self._X.loc[self._targetMask, column].dropna()
            if len(series) < 20:
                continue
            try:
                adfStatistic, adfPvalue, _, _, adfCritValues, _ = adfuller(series, autolag="AIC")
                kpssStatistic, kpssPvalue, _, kpssCritValues    = kpss(series, regression="c", nlags="auto")

                adfRejectsUnitRoot       = adfPvalue  < 0.05
                kpssRejectsStationarity  = kpssPvalue < 0.05

                if adfRejectsUnitRoot and not kpssRejectsStationarity:
                    verdict = "I(0)"
                elif not adfRejectsUnitRoot and kpssRejectsStationarity:
                    verdict = "I(1)"
                else:
                    verdict = "Ambiguous"

                rows.append({
                    "Feature"    : column,
                    "ADF stat"   : round(adfStatistic,          3),
                    "ADF p"      : round(adfPvalue,             4),
                    "ADF 5% cv"  : round(adfCritValues["5%"],   3),
                    "KPSS stat"  : round(kpssStatistic,         3),
                    "KPSS p"     : round(kpssPvalue,            4),
                    "KPSS 5% cv" : round(kpssCritValues["5%"],  3),
                    "Verdict"    : verdict,
                    "KPSS p note": "p-value capped at boundary (true p outside [0.01, 0.10])" if kpssPvalue in (0.01, 0.10) else "",
                })
            except Exception:
                continue

        stationarityResults = pd.DataFrame(rows)
        nStationary         = (stationarityResults["Verdict"] == "I(0)").sum()
        nUnitRoot           = (stationarityResults["Verdict"] == "I(1)").sum()
        nAmbiguous          = (stationarityResults["Verdict"] == "Ambiguous").sum()
        nTotal              = len(stationarityResults)
        print(f"  I(0): {nStationary}  |  I(1): {nUnitRoot}  |  Ambiguous: {nAmbiguous}  |  Total: {nTotal}")

        stationarityResults.to_csv(self._path("stationarity.csv"), index=False)
        print(f"  Full results: {self._path('stationarity.csv')}")

        nonStationaryFeatures = stationarityResults[stationarityResults["Verdict"] != "I(0)"].copy()
        if nonStationaryFeatures.empty:
            nonStationaryFeatures = pd.DataFrame([{"Feature": "All features I(0) at 5%",
                                                    "Verdict": "I(0)"}])
        self._save_table_pdf(
            nonStationaryFeatures,
            f"Stationarity — Non-I(0) Features  (ADF + KPSS at 5%)\n"
            f"I(0): {nStationary}  |  I(1): {nUnitRoot}  |  Ambiguous: {nAmbiguous}  |  Full table: stationarity.csv",
            self._path("stationarity_failures.pdf"),
        )
        print(f"\n{self.SEP}\n")
        return stationarityResults

    # Draws a one page diagonisis where the time series and outliers plots are inlcuded, in addition to
    # the distribution, ACF and PACF graphs, concentrated in an open pdf page
    def _plotPage(self, pdf, series: pd.Series, dates: np.ndarray, title: str,
                  ylabel: str = "", xlabel_dist: str = "Value", label: str = "") -> None:
        nObservations = len(series)
        label         = label or title[:40]

        _, jarqueBeraP = stats.jarque_bera(series)
        try:
            _, archPvalue, _, _ = het_arch(series.values, nlags=4)
            archAnnotation = f"ARCH p={'<0.001' if archPvalue < 0.001 else f'{archPvalue:.3f}'}"
        except Exception:
            archAnnotation = "ARCH: n/a"

        fig        = plt.figure(figsize=(16, 10), facecolor="white")
        fig.suptitle(title, fontsize=11, fontweight="bold", y=0.99)
        gridLayout = GridSpec(2, 3, figure=fig, hspace=0.48, wspace=0.35)

        axTimeSeries = fig.add_subplot(gridLayout[0, :])
        axTimeSeries.plot(dates, series.values, color=_BLUE, lw=1.2, alpha=0.85)
        axTimeSeries.axhline(0, color=_GREY, lw=0.8, ls="--")
        outlierMask = ((series - series.mean()) / series.std()).abs() > 3
        if outlierMask.any():
            axTimeSeries.scatter(dates[outlierMask.values], series.values[outlierMask.values],
                                 color=_RED, zorder=5, s=35,
                                 label=f"Outliers |z|>3 (n={int(outlierMask.sum())})")
            axTimeSeries.legend(frameon=False, fontsize=8)
        axTimeSeries.set_title("Time Series", fontweight="bold", fontsize=10)
        axTimeSeries.xaxis.set_major_locator(mdates.YearLocator(2))
        axTimeSeries.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        self._style(axTimeSeries, ylabel=ylabel)

        axDistribution = fig.add_subplot(gridLayout[1, 0])
        axDistribution.hist(series, bins=40, density=True, color=_BLUE, alpha=0.20, edgecolor="white")
        normalCurveX = np.linspace(series.min(), series.max(), 200)
        axDistribution.plot(normalCurveX, stats.norm.pdf(normalCurveX, series.mean(), series.std()),
                            color=_RED, lw=1.8, ls="--", label="Normal")
        jarqueBeraAnnotation = "< 0.001" if jarqueBeraP < 0.001 else f"{jarqueBeraP:.3f}"
        self._ann(axDistribution, f"JB p = {jarqueBeraAnnotation}\n{archAnnotation}\n"
                                  f"Skew = {float(series.skew()):.3f}\n"
                                  f"Kurt = {float(series.kurtosis()):.3f}")
        axDistribution.set_title("Distribution", fontweight="bold", fontsize=10)
        axDistribution.legend(frameon=False, fontsize=8)
        self._style(axDistribution, xlabel=xlabel_dist, ylabel="Density")

        maximumLag = min(52, nObservations // 2 - 1)
        axACF      = fig.add_subplot(gridLayout[1, 1])
        _plot_correlogram(axACF,  acf(series, nlags=maximumLag),                          nObservations, f"ACF  ({label})")
        axPACF     = fig.add_subplot(gridLayout[1, 2])
        _plot_correlogram(axPACF, pacf(series, nlags=min(maximumLag, nObservations // 4)), nObservations, f"PACF  ({label})")

        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

    # Produces the diagonis plots from the target and its horizons
    def targetHorizonPlots(self) -> None:

        print(f"\n{self.SEP}\n  TARGET HORIZON PLOTS\n{self.SEP}\n")

        path       = self._path("target_horizons.pdf")
        targetCols = [c for c in self._targetCols if c in self._factorMatrix.columns]

        with PdfPages(path) as pdf:
            for column in targetCols:
                series = self._factorMatrix[column].dropna()
                if len(series) < 20:
                    continue
                label = column.replace("∆ Salmon (NOK/KG)", "").strip()
                self._plotPage(pdf, series, self._dates(series.index),
                               title=column, ylabel="∆ Price (NOK/kg)",
                               xlabel_dist="Return", label=label)
                print(f"    {label}")

        print(f"  Saved: {path}\n{self.SEP}\n")

    # Produces the one pager diagnosis plots for each one of the features
    def featurePlots(self) -> None:

        print(f"\n{self.SEP}\n  FEATURE PLOTS\n{self.SEP}\n")

        path = self._path("feature_plots.pdf")

        with PdfPages(path) as pdf:
            for column in self._validCols:
                series = self._X.loc[self._targetMask, column].dropna()
                if len(series) < 20:
                    continue
                featureDates = self._factorMatrix.loc[self._targetMask, "Date"] \
                               if "Date" in self._factorMatrix.columns \
                               else self._factorMatrix.index[self._targetMask]
                datesArray   = featureDates.values if hasattr(featureDates, "values") else np.array(featureDates)
                series       = series.reindex(self._factorMatrix.loc[self._targetMask].index)
                self._plotPage(pdf, series.dropna(), datesArray[series.notna().values],
                               title=column, xlabel_dist="Value", label=column[:40])
                print(f"    {column}")

        print(f"  Saved: {path}\n{self.SEP}\n")

    # Returns Ljung-Box test for autocorrelation and siginficance at lags w, 13w, 26w, 52w
    # saves results in a csv file
    def autocorrelationTests(self) -> pd.DataFrame:

        print(f"\n{self.SEP}\n  AUTOCORRELATION TESTS (LJUNG-BOX)\n{self.SEP}\n")

        testLags = [4, 13, 26, 52]
        rows     = []

        for column in self._validCols:
            series = self._toMonthly(column) if column in self._monthlyCols \
                else self._X.loc[self._targetMask, column].dropna()
            if len(series) < 60:
                continue
            try:
                ljungBoxResults = acorr_ljungbox(series, lags=testLags, return_df=True)
                featureRow      = {"Feature": column}
                for lagWeeks in testLags:
                    pvalue = ljungBoxResults.loc[lagWeeks, "lb_pvalue"] if lagWeeks in ljungBoxResults.index else np.nan
                    featureRow[f"p ({lagWeeks}w)"] = round(float(pvalue), 4) if not np.isnan(pvalue) else np.nan
                rows.append(featureRow)
            except Exception:
                continue

        autocorrResults = pd.DataFrame(rows).set_index("Feature")
        autocorrResults.to_csv(self._path("autocorrelation.csv"))
        print(f"  Saved: {self._path('autocorrelation.csv')}\n{self.SEP}\n")
        return autocorrResults

    # Decomposes Y 0w into trend, seasonal and residual to visualise the annual cycle
    # that SARIMA(m=52) is modelling
    def seasonalDecomposition(self) -> None:

        print(f"\n{self.SEP}\n  SEASONAL DECOMPOSITION (STL, period=52)\n{self.SEP}\n")

        targetSeries = self._y.loc[self._targetMask].dropna()
        if len(targetSeries) < 104:
            print("  Fewer than 2 full years — STL skipped.")
            return

        stlResult = STL(targetSeries, period=52, robust=True).fit()
        dates     = self._dates(targetSeries.index)

        colours = [_DARK, _BLUE, _TEAL, _RED]
        titles  = ["Observed", "Trend", "Seasonal  (period = 52 weeks)", "Residual"]

        fig, axes = plt.subplots(4, 1, figsize=(14, 14), sharex=True, facecolor="white")
        fig.suptitle("STL Seasonal Decomposition — Y 0w ∆ Salmon (NOK/KG)",
                     fontweight="bold", fontsize=12, linespacing=1.6)

        for ax, componentData, title, colour in zip(axes,
                                                    [targetSeries, stlResult.trend, stlResult.seasonal, stlResult.resid],
                                                    titles, colours):
            ax.plot(dates, componentData.values, color=colour, lw=1.2)
            ax.axhline(0, color=_GREY, lw=0.7, ls="--")
            ax.set_title(title, fontweight="bold", fontsize=10)
            ax.xaxis.set_major_locator(mdates.YearLocator(2))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
            self._style(ax, ylabel="∆ Price")

        plt.tight_layout()
        path = self._path("seasonal_decomposition.pdf")
        plt.savefig(path, format="pdf", bbox_inches="tight")
        plt.close()
        print(f"  Saved: {path}\n{self.SEP}\n")

    # Runs all diagnostics in sequence
    def report(self) -> None:
        print(f"\n{'█'*72}")
        print(f"  EDA — Salmon Price Forecasting")
        print(f"  FishPool period : {int(self._targetMask.sum())} obs")
        print(f"  Valid features  : {len(self._validCols)}")
        print(f"  Y horizons      : {len(self._targetCols)}")
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


# Draws an ACF or PACF bar chart along with its confidence interval lines
def _plot_correlogram(ax, correlationValues: np.ndarray, nObservations: int, title: str) -> None:
    confidenceInterval = 1.96 / np.sqrt(nObservations)
    lagValues          = np.arange(len(correlationValues))
    barColours         = [_BLUE if abs(v) > confidenceInterval else _LIGHT for v in correlationValues]
    ax.bar(lagValues, correlationValues, color=barColours, width=0.6, alpha=0.85)
    ax.axhline( confidenceInterval, color=_RED,    lw=1.2, ls="--", alpha=0.8, label="95% CI")
    ax.axhline(-confidenceInterval, color=_RED,    lw=1.2, ls="--", alpha=0.8)
    ax.axhline(0,                   color="black", lw=0.6)
    ax.set_title(title, fontweight="bold", fontsize=9)
    ax.set_xlabel("Lag (weeks)",     fontweight="bold", fontsize=8)
    ax.set_ylabel("Autocorrelation", fontweight="bold", fontsize=8)
    ax.legend(frameon=False, fontsize=7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, linestyle="--", alpha=0.1)


if __name__ == "__main__":
    factorData = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
    freqMap    = {col: ("monthly" if col.endswith("Monthly") else "weekly") for col in factorData.columns}
    EDA(factorData, freqMap=freqMap).report()
