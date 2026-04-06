##
#  This module defines the EDA class.
#  Pipeline Stage 2: EDA & Validation Gates
#
#  Input:  factors  — Factors DataFrame from featureEngineer.buildFeatureMatrix()
#
#  Output: diagnostic reports printed to console + saved PDF plots
#
#  Sections:
#    2.1  Distribution Analysis      (histograms, JB test, QQ-plot)
#    2.2  Stationarity Tests         (ADF + KPSS, decision table)
#    2.3  Temporal Dependence        (ACF, PACF, Ljung-Box — y_t + all features)
#    2.4  Granger Causality          (each feature vs y_t, SBIC-selected lag, maxlag=104wk/24mo=2yr)
#    2.5  Cointegration              (Engle-Granger, Granger-filtered candidates)
#    2.6  Heteroscedasticity/Volatility → Stage 3 (White, BG, ARCH-LM on actual OLS residuals)
#    2.7  Multicollinearity          (correlation matrix, VIF)
#    2.8  plotAllFeatures            (PDF: time series / histogram+normal / ACF / PACF per feature)
#    2.9  report()                   (runs all sections)
#
#  @library pandas, numpy, matplotlib, seaborn, scipy, statsmodels
##


import warnings
import pandas              as pd
import numpy               as np
import matplotlib.pyplot             as plt
import matplotlib.gridspec           as gridspec
import seaborn                       as sns
from matplotlib.backends.backend_pdf import PdfPages

from scipy                                 import stats
from statsmodels.tsa.stattools             import adfuller, kpss, coint
from statsmodels.tsa.vector_ar.var_model  import VAR
from statsmodels.stats.diagnostic          import acorr_ljungbox
from statsmodels.stats.outliers_influence  import variance_inflation_factor
from statsmodels.graphics.tsaplots         import plot_acf, plot_pacf
from statsmodels.tools.sm_exceptions      import InterpolationWarning

## KPSS p-values are bounded at [0.01, 0.10] in the statsmodels lookup table.
## When the test statistic falls outside this range the actual p-value is beyond
## the table boundary — the warning is expected and not a data error.
warnings.filterwarnings("ignore", category=InterpolationWarning)

## plot_acf / plot_pacf / acorr_ljungbox trigger a ValueWarning when the series
## has an integer index instead of DatetimeIndex.  The statistics are unaffected;
## statsmodels only needs the DatetimeIndex for out-of-sample forecasting, which
## we do not use in EDA.
warnings.filterwarnings(
    "ignore",
    message="An unsupported index was provided",
    category=UserWarning,
)

##
#  EDA runs all Stage 2 diagnostics on the Factors feature matrix.
#
#  Usage:
#      eda = EDA(Factors)
#      eda.report()          # runs all sections
#
#      or individually:
#      eda.stationarityTests()
#      eda.grangerCausality()
#
class EDA:

    ## Meta columns in Factors (not features, not target)
    _META   = ["Date"]

    ## Target column in Factors — already log return (computed by FeatureEngineer)
    _TARGET = "Salmon (NOK/KG)"

    ## Lag reference structure — consistent across ALL tests in this class.
    ## Economic horizon: 24 months (104 weeks) — covers the full salmon production
    ## cycle of 14–22 months from smolt release to harvest.
    ##
    ##   Economic period │ Weekly lag │ Monthly lag
    ##   ────────────────┼────────────┼────────────
    ##   1 month         │     4      │      1
    ##   6 months        │    26      │      6
    ##   1 year          │    52      │     12
    ##   2 years         │   104      │     24
    ##
    ## Used by: Ljung-Box (temporalDependence), Granger maxlags, plotAllFeatures ACF/PACF.
    ## White's, BG, and ARCH-LM run in Stage 3 on actual OLS residuals.
    _LAGS_WEEKLY  = [4, 26, 52, 104]   ## 1mo, 6mo, 1yr, 2yr in weeks
    _LAGS_MONTHLY = [1,  6, 12,  24]   ## 1mo, 6mo, 1yr, 2yr in months

    SEP = "═" * 72

    ##
    #  Constructor.
    #  @param factors   DataFrame — output of featureEngineer.buildFeatureMatrix()
    #  @param freq_map  dict {col: "monthly" | "weekly"} — supplied by FeatureEngineer.
    #                   Monthly columns are broadcast (same value ~4 weeks in a row).
    #                   EDA downsamples them to true monthly frequency before running
    #                   stationarity tests, Granger causality, and distribution tests.
    #                   If omitted, all features are treated as weekly.
    #
    def __init__(self, factors: pd.DataFrame, freq_map: dict = None):

        self._factors  = factors.copy()
        self._freq_map = freq_map or {}

        if self._TARGET not in self._factors.columns:
            raise ValueError(f"Target column '{self._TARGET}' not found in Factors.")

        ## Target y: already log return — use directly
        self.y      = self._factors[self._TARGET].copy()
        self.y.name = "y"

        ## Feature matrix: all columns except meta and target
        _drop  = [c for c in self._META + [self._TARGET] if c in self._factors.columns]
        self.X = self._factors.drop(columns=_drop)

        ## FishPool-period mask: rows where y is defined
        self._fp_mask = self.y.notna()

        ## Valid feature columns: at least 52 non-NaN obs in FishPool period
        self._valid_cols = [
            c for c in self.X.columns
            if self.X.loc[self._fp_mask, c].notna().sum() >= 52
        ]

        ## Split valid columns by frequency
        ## Monthly columns are broadcast to weekly in Factors — must be downsampled
        ## before any statistical test that assumes independent observations
        self._monthly_cols = [c for c in self._valid_cols
                              if self._freq_map.get(c) == "monthly"]

        if self._monthly_cols:
            print(f"  [EDA] Monthly columns ({len(self._monthly_cols)}): "
                  f"{self._monthly_cols}")
        if not self._freq_map:
            print(f"  [EDA] No freq_map supplied — all features treated as weekly")

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    ##
    #  Returns a monthly-frequency Series for any column in self.X.
    #  For broadcast monthly columns: recovers the true monthly value (last non-NaN per month).
    #  For weekly columns: takes the last Wednesday observation per month (used in mixed pairs
    #  where one column is monthly and the other weekly — both must share the same frequency).
    #  FishPool period only.
    #
    #  @param col  column name in self.X
    #  @return     pd.Series indexed by month-end dates
    #
    def _toMonthly(self, col: str) -> pd.Series:
        df = pd.DataFrame({
            "Date" : self._factors.loc[self._fp_mask, "Date"],
            "val"  : self.X.loc[self._fp_mask, col],
        }).dropna().set_index("Date")
        return df["val"].resample("ME").last().dropna()

    ##
    #  Returns y_t aggregated to monthly frequency.
    #  Monthly log return = sum of weekly log returns (log returns are additive).
    #  Used as the dependent variable when testing Granger causality against
    #  a monthly feature — both series must share the same frequency.
    #
    #  @return  pd.Series of monthly log returns, indexed by month-end dates
    #
    def _yToMonthly(self) -> pd.Series:
        df = pd.DataFrame({
            "Date" : self._factors.loc[self._fp_mask, "Date"],
            "y"    : self.y.loc[self._fp_mask],
        }).dropna().set_index("Date")
        return df["y"].resample("ME").sum().dropna()

    # ─────────────────────────────────────────────────────────────────────────
    # 2.1  DISTRIBUTION ANALYSIS
    # ─────────────────────────────────────────────────────────────────────────

    ##
    #  Computes summary statistics, Jarque-Bera normality tests,
    #  QQ-plot and histogram for y_t.
    #
    #  @return  summary DataFrame (mean, std, skew, kurt, min, max, %NaN, %zero)
    #
    def distributionAnalysis(self) -> pd.DataFrame:

        print(f"\n{self.SEP}")
        print(f"  2.1  DISTRIBUTION ANALYSIS")
        print(f"{self.SEP}")

        ## Summary statistics table
        ## Monthly columns: use unique monthly values (avoids inflating N ~4x)
        summary_rows = []
        for col in self._valid_cols:
            s = self._toMonthly(col).values if col in self._monthly_cols \
                else self.X.loc[self._fp_mask, col].dropna().values
            s = pd.Series(s)
            summary_rows.append({
                "Column"   : col,
                "Mean"     : round(s.mean(), 4),
                "Std"      : round(s.std(), 4),
                "Skew"     : round(s.skew(), 4),
                "Kurt"     : round(s.kurtosis(), 4),
                "Min"      : round(s.min(), 4),
                "Max"      : round(s.max(), 4),
                "pct_NaN"  : round(self.X.loc[self._fp_mask, col].isna().mean() * 100, 1),
                "pct_Zero" : round((self.X.loc[self._fp_mask, col] == 0).mean() * 100, 1),
            })

        summary = pd.DataFrame(summary_rows).set_index("Column")
        print(f"\n{summary.to_string()}\n")

        ## Jarque-Bera on y_t
        ## H₀: skewness=0 and excess kurtosis=0. Test stat = T×(S²/6 + (K-3)²/24) ~ χ²(2)
        ## [Ref: GRA 6547, Ch 3, chapter_3-3_non-normality.R — JB formula and interpretation]
        ## Rejection expected: financial returns are typically leptokurtic (fat tails).
        ## [Ref: GRA 6547, Ch 8, slide 265 — leptokurtosis as motivation for GARCH]
        y_clean = self.y.loc[self._fp_mask].dropna()

        print(f"── TARGET: {self._TARGET} (log return) {'─'*35}")
        try:
            jb_stat, jb_p = stats.jarque_bera(y_clean)
            print(f"  Jarque-Bera: stat={jb_stat:.4f}   p={jb_p:.4f}  "
                  f"{'Non-normal — leptokurtosis expected for financial returns' if jb_p < 0.05 else 'Cannot reject normality'}")
        except Exception as e:
            print(f"  ⚠ JB on y_t failed — {type(e).__name__}: {e}")
        ## Non-normality does not block the pipeline:
        ## OLS: White's robust SEs (HC3) are consistent regardless of error distribution.
        ## [Ref: GRA 6547, Ch 3, slide 82 — heteroscedasticity-consistent SEs]
        ## CatBoost: nonparametric — no distributional assumption on y_t.
        ## [Ref: GRA 6518, Lesson 10 — trees are nonparametric]
        print(f"  Note: OLS uses White robust SEs — normality not required [GRA 6547, Ch 3, slide 82]")
        print(f"        CatBoost is nonparametric — normality not required [GRA 6518, Lesson 10]")

        ## Jarque-Bera on features
        ## NOTE on JB magnitude: the statistic is JB = T × (S²/6 + (K−3)²/24).
        ## It scales linearly with sample size T.  With T≈1000 weekly obs and price
        ## series in levels (structurally right-skewed, leptokurtic), JB routinely
        ## reaches thousands — this is EXPECTED, not a data error.
        ## Interpretation: rejection here signals that the feature needs log transformation
        ## (confirmed by stationarity tests).  The policy-relevant JB test is on OLS
        ## residuals in Stage 3 — after all transformations have been applied.
        ## [Ref: GRA 6547, Ch 3, chapter_3-3_non-normality.R — JB formula and interpretation]
        print(f"\n── JARQUE-BERA (features) {'─'*46}")
        print(f"  [M]=monthly freq  [W]=weekly freq")
        print(f"  ⚠ JB scales with T — high stats for level series are expected (needs log transform)")
        print(f"  {'Column':<40}  {'JB stat':>10}  {'p-value':>10}  {'Skew':>6}  {'Kurt':>6}  Result")
        print(f"  {'─'*40}  {'─'*10}  {'─'*10}  {'─'*6}  {'─'*6}  ──────")

        for col in self._valid_cols:
            ## Monthly columns: test on unique monthly values, not 4x-repeated weekly rows
            ## Weekly columns: FishPool period only (consistent with all other EDA tests)
            s = self._toMonthly(col) if col in self._monthly_cols \
                else self.X.loc[self._fp_mask, col].dropna()
            if len(s) < 8:
                continue
            s        = pd.Series(s.values)   ## strip DatetimeIndex to avoid statsmodels index warning
            jb, p    = stats.jarque_bera(s)
            freq_tag = "[M]" if col in self._monthly_cols else "[W]"
            skew     = s.skew()
            kurt     = s.kurtosis() + 3      ## scipy kurtosis() returns excess; show raw kurtosis
            result   = "Non-normal" if p < 0.05 else "Normal"
            col_disp = (col[:36] + "..") if len(col) > 38 else col
            print(f"  {col_disp:<38} {freq_tag}  {jb:>10.2f}  {p:>10.4f}  "
                  f"{skew:>6.2f}  {kurt:>6.2f}  {result}")

        ## QQ-plot for y_t
        fig, ax = plt.subplots(figsize=(6, 5))
        try:
            stats.probplot(y_clean, dist="norm", plot=ax)
            ax.set_title("QQ-Plot: Log Return of Salmon Price (y_t)")
            plt.tight_layout()
            plt.savefig("eda_qqplot_y.pdf")
        except Exception as e:
            print(f"  ⚠ QQ-plot failed — {type(e).__name__}: {e}")
        finally:
            plt.close()

        ## Histogram + normal overlay for y_t
        ## Course approach: hist + dnorm overlay (GRA Quant Risk, Assignment1.R lines 171-173)
        ## The red curve is the null hypothesis of JB — visual departure = non-normality
        fig, ax = plt.subplots(figsize=(8, 4))
        try:
            y_clean.hist(bins=50, density=True, alpha=0.5, ax=ax,
                         color="steelblue", label="Empirical")
            x_range = np.linspace(y_clean.min(), y_clean.max(), 300)
            ax.plot(x_range,
                    stats.norm.pdf(x_range, loc=y_clean.mean(), scale=y_clean.std()),
                    color="red", lw=1.8, label="Normal fit (JB H₀)")
            ax.set_title("Distribution of Log Return y_t — Salmon (NOK/KG)")
            ax.legend()
            plt.tight_layout()
            plt.savefig("eda_hist_y.pdf")
        except Exception as e:
            print(f"  ⚠ Histogram failed — {type(e).__name__}: {e}")
        finally:
            plt.close()

        print(f"\n  Saved: eda_qqplot_y.pdf, eda_hist_y.pdf")
        print(f"\n{self.SEP}\n")

        return summary

    # ─────────────────────────────────────────────────────────────────────────
    # 2.2  STATIONARITY TESTS
    # ─────────────────────────────────────────────────────────────────────────

    ##
    #  Runs ADF and KPSS on every feature and on y_t.
    #  Uses decision matrix from pipeline plan §2.2.
    #
    #  @return  DataFrame with ADF p, KPSS p, decision, and recommended action
    #
    def stationarityTests(self) -> pd.DataFrame:

        print(f"\n{self.SEP}")
        print(f"  2.2  STATIONARITY TESTS  (ADF + KPSS)")
        print(f"{self.SEP}")
        print(f"\n  ADF  H₀: unit root exists  (reject → stationary)")
        print(f"  KPSS H₀: series is stationary (reject → non-stationary)\n")
        print(f"  [M] = tested at monthly frequency  [W] = tested at weekly frequency")
        print(f"  {'Column':<42}  {'ADF_p':>7}  {'KPSS_p':>7}  {'Decision':<18}  Action")
        print(f"  {'─'*42}  {'─'*7}  {'─'*7}  {'─'*18}  ─────")

        results = []

        for col in self._valid_cols:

            ## Monthly columns: downsample to true monthly frequency before testing.
            ## Running ADF on broadcast weekly data (same value 4x per month) inflates T
            ## and creates artificial autocorrelation at lags 1-3, distorting the test.
            ##
            ## Weekly columns: restrict to FishPool period (consistent with cointegration
            ## Step 1 and grangerCausality, which also use _fp_mask for weekly features).
            ## Using full history here would give a different ADF verdict for the same
            ## column than what cointegration Step 1 uses internally.
            series = self._toMonthly(col) if col in self._monthly_cols \
                     else self.X.loc[self._fp_mask, col].dropna()
            freq_tag = "[M]" if col in self._monthly_cols else "[W]"

            if len(series) < 12:   ## minimum 12 months (or 52 weeks) for ADF
                continue

            ## ADF (AIC lag selection, regression="c": constant/drift)
            ## Non-stationary series invalidate standard asymptotics for OLS.
            ## [Ref: GRA 6547, Ch 7, slides 218–227 — "standard assumptions for asymptotic
            ##  analysis will not be valid" if non-stationary]
            ##
            ## Regression type: "c" (constant/drift) is the default for price and return series.
            ## "ct" only if a deterministic trend is visible; "n" only for zero-mean series.
            ## [Ref: GRA 6547, Ch 7, slides 219–221 — variant selection based on visual inspection]
            ##
            ## Lag selection: AIC-based automatic augmentation whitens residuals.
            ## Too few lags → size distortion; too many → power loss.
            ## [Ref: GRA 6547, Ch 7, slide 222 — AIC-based lag length selection]
            try:
                _, adf_p, *_ = adfuller(series, autolag="AIC", regression="c")
                adf_reject   = adf_p < 0.05
            except Exception as e:
                print(f"  ⚠ [{col}] ADF failed — {type(e).__name__}: {e}")
                adf_p, adf_reject = None, None

            ## KPSS (Newey-West automatic bandwidth, regression="c")
            ## Used jointly with ADF to handle the power/size tradeoff:
            ## ADF has low power; KPSS has correct size under H₀ of stationarity.
            ## [Ref: GRA 6547, Ch 7 — use ADF and KPSS together as complementary tests]
            try:
                _, kpss_p, *_ = kpss(series, regression="c", nlags="auto")
                kpss_reject   = kpss_p < 0.05
            except Exception as e:
                print(f"  ⚠ [{col}] KPSS failed — {type(e).__name__}: {e}")
                kpss_p, kpss_reject = None, None

            ## Decision matrix (pipeline plan §2.2)
            ## Guard: if either test failed (None), label explicitly rather than
            ## letting None propagate through boolean logic and produce a misleading
            ## "Inconclusive" verdict that implies both tests ran with mixed results.
            if adf_reject is None or kpss_reject is None:
                decision = "Test failed"
                action   = "Check manually"
            elif adf_reject and not kpss_reject:
                decision, action = "Stationary ✓",     "Use levels"
            elif not adf_reject and kpss_reject:
                decision, action = "Non-stationary",   "Use Δln"
            elif adf_reject and kpss_reject:
                decision, action = "Trend-stationary", "Detrend"
            else:
                decision, action = "Inconclusive",     "Default Δln"

            col_disp = (col[:38] + "..") if len(col) > 40 else col
            print(f"  {col_disp:<38} {freq_tag}  "
                  f"{str(round(adf_p,  4)) if adf_p  is not None else 'N/A':>7}  "
                  f"{str(round(kpss_p, 4)) if kpss_p is not None else 'N/A':>7}  "
                  f"{decision:<18}  {action}")

            results.append({
                "Column"    : col,
                "Frequency" : "monthly" if col in self._monthly_cols else "weekly",
                "ADF_p"     : round(adf_p,  4) if adf_p  is not None else None,
                "KPSS_p"    : round(kpss_p, 4) if kpss_p is not None else None,
                "Decision"  : decision,
                "Action"    : action,
            })

        ## Test y_t
        y_clean = self.y.loc[self._fp_mask].dropna()
        print(f"\n── y (log return salmon price) ──────────────────────────────────────")
        try:
            _, adf_y_p,  *_ = adfuller(y_clean, autolag="AIC", regression="c")
            _, kpss_y_p, *_ = kpss(y_clean, regression="c", nlags="auto")
            print(f"  ADF p={adf_y_p:.4f}   KPSS p={kpss_y_p:.4f}  "
                  f"{'Stationary ✓ — log return confirmed' if adf_y_p < 0.05 and kpss_y_p > 0.05 else '⚠ Check transformation'}")
        except Exception as e:
            print(f"  ⚠ ADF/KPSS on y_t failed — {type(e).__name__}: {e}")
        print(f"\n  → Non-stationary features: feed back to Stage 1 (FeatureEngineer)")
        print(f"\n{self.SEP}\n")

        return pd.DataFrame(results)

    # ─────────────────────────────────────────────────────────────────────────
    # 2.3  TEMPORAL DEPENDENCE ANALYSIS
    # ─────────────────────────────────────────────────────────────────────────

    ##
    #  ACF, PACF, and Ljung-Box tests on y_t (FishPool period only).
    #
    def temporalDependence(self) -> None:

        print(f"\n{self.SEP}")
        print(f"  2.3  TEMPORAL DEPENDENCE ANALYSIS")
        print(f"{self.SEP}")

        y_clean = self.y.loc[self._fp_mask].dropna()
        T       = len(y_clean)
        print(f"\n  Sample: {T} observations (FishPool period)\n")

        ## ACF / PACF plots up to lag 104 (2 years — full salmon production cycle)
        fig, axes = plt.subplots(2, 1, figsize=(12, 7))
        try:
            plot_acf( y_clean, lags=104, ax=axes[0], title="ACF  — y_t log return (up to lag 104 = 2yr)")
            plot_pacf(y_clean, lags=104, ax=axes[1], title="PACF — y_t log return (up to lag 104 = 2yr)")
            plt.tight_layout()
            plt.savefig("eda_acf_pacf_y.pdf")
            print(f"  Saved: eda_acf_pacf_y.pdf")
        except Exception as e:
            print(f"  ⚠ ACF/PACF plot failed — {type(e).__name__}: {e}")
        finally:
            plt.close()
        ## ACF: identifies MA order (slow decay → I(1) candidate).
        ## PACF: identifies AR order (sharp cutoff at lag 1 → AR(1)).
        ## [Ref: GRA 6547, Ch 5, slides 124–129 — ACF and PACF in Box-Jenkins methodology]
        print(f"  → ACF identifies MA order; PACF identifies AR order (Box-Jenkins) [GRA 6547, Ch 5, slides 124–129]")

        ## Ljung-Box at _LAGS_WEEKLY = [4, 26, 52, 104] → 1mo, 6mo, 1yr, 2yr
        ## Consistent with the lag reference structure used across all EDA tests.
        ## Q* = T(T+2) Σ_{k=1}^{m} (τ̂²_k / (T−k)) ~ χ²(m) under H₀ of no autocorrelation.
        ## [Ref: GRA 6547, Ch 5, slide 128 — Ljung-Box Q* statistic]
        lb_lags = self._LAGS_WEEKLY

        print(f"\n── LJUNG-BOX TEST ───────────────────────────────────────────────────")
        print(f"  H₀: no autocorrelation up to lag k")
        print(f"  Lags: 4=1mo  26=6mo  52=1yr  104=2yr (weekly, salmon lifecycle horizon)")
        print(f"  {'Lag':>4}  {'Q-stat':>10}  {'p-value':>10}  Result")
        print(f"  {'─'*4}  {'─'*10}  {'─'*10}  ──────")

        try:
            lb_res = acorr_ljungbox(y_clean, lags=lb_lags, return_df=True)
            for lag in lb_lags:
                row    = lb_res.loc[lag]
                result = "Autocorrelation detected → predictable structure" \
                         if row["lb_pvalue"] < 0.05 else "No autocorrelation"
                print(f"  {lag:>4}  {row['lb_stat']:>10.4f}  {row['lb_pvalue']:>10.4f}  {result}")
        except Exception as e:
            print(f"  ⚠ Ljung-Box failed — {type(e).__name__}: {e}")

        ## Ljung-Box summary for all features
        ## ACF/PACF plots for each feature are in the per-feature PDF (section 2.8).
        ## This table gives a compact printable summary of whether each feature
        ## has significant autocorrelation at the same economic horizons used for y_t.
        ## Persistent autocorrelation in a level series → likely I(1) (confirms stationarity verdict).
        ## [Ref: GRA 6547, Ch 5, slides 124-129 — ACF/PACF identification]
        print(f"\n── LJUNG-BOX SUMMARY — ALL FEATURES ────────────────────────────────")
        print(f"  p-values at reference lags.  * = significant at 5%.")
        print(f"  [M] = monthly lags (1, 6, 12, 24 months)")
        print(f"  [W] = weekly  lags (4, 26, 52, 104 weeks = 1mo, 6mo, 1yr, 2yr)")

        ## Weekly header uses 4 lags; monthly uses 4 lags — same width, different units
        print(f"\n  {'Column':<40}  {'Lag1':>7}  {'Lag2':>7}  {'Lag3':>7}  {'Lag4':>7}")
        print(f"  {'─'*40}  {'─'*7}  {'─'*7}  {'─'*7}  {'─'*7}")

        for col in self._valid_cols:
            is_monthly  = col in self._monthly_cols
            freq_tag    = "[M]" if is_monthly else "[W]"
            lb_ref_lags = self._LAGS_MONTHLY if is_monthly else self._LAGS_WEEKLY

            series = self._toMonthly(col) if is_monthly \
                     else self.X.loc[self._fp_mask, col].dropna()
            series = pd.Series(series.values)   ## strip DatetimeIndex

            max_testable = len(series) - 1
            valid_lags   = [l for l in lb_ref_lags if l < max_testable]
            if len(valid_lags) < 1:
                continue

            try:
                lb = acorr_ljungbox(series, lags=valid_lags, return_df=True)
                p_vals = []
                for l in lb_ref_lags:
                    if l in valid_lags:
                        pv   = lb.loc[l, "lb_pvalue"]
                        flag = "*" if pv < 0.05 else " "
                        p_vals.append(f"{pv:.3f}{flag}")
                    else:
                        p_vals.append("  N/A ")
                col_disp = (col[:38] + "..") if len(col) > 40 else col
                print(f"  {col_disp:<38} {freq_tag}  "
                      + "  ".join(f"{v:>7}" for v in p_vals))
            except Exception as e:
                print(f"  ⚠ [{col}] LB failed — {type(e).__name__}: {e}")

        print(f"\n  → Persistent significance across lags = likely I(1) — confirmed by stationarity tests")
        print(f"  → ACF/PACF plots for each feature: see eda_feature_plots.pdf (section 2.8)")
        print(f"\n{self.SEP}\n")

    # ─────────────────────────────────────────────────────────────────────────
    # 2.4  GRANGER CAUSALITY TESTS
    # ─────────────────────────────────────────────────────────────────────────

    ##
    #  Tests Granger causality of each feature against y_t.
    #  FishPool period only.
    #
    #  Lag selection: bivariate VAR with SBIC (BIC) criterion.
    #  maxlag = 104wk (weekly) or 24mo (monthly) = 2-year horizon (salmon lifecycle).
    #  SBIC selects ONE lag per pair — avoids data snooping from looping over lags
    #  and picking the best p-value.
    #  [Ref: GRA 6547, Ch 6, chapter_6-1_var.R — VARselect(SBIC) → VAR(p) → causality()]
    #  [Ref: GRA 6518, Lesson 5, slide 6 — BIC for parsimony in lag order selection]
    #
    #  @return  DataFrame with SBIC lag, F-stat, p-value per feature
    #
    def grangerCausality(self) -> pd.DataFrame:

        print(f"\n{self.SEP}")
        print(f"  2.4  GRANGER CAUSALITY TESTS")
        print(f"{self.SEP}")
        ## H₀: γ₁=γ₂=...=γ_k=0 in y_t = α + Σφᵢy_{t-i} + Σγⱼx_{t-j} + u_t
        ## Rejection means x Granger-causes y: lagged x adds predictive power beyond y's own lags.
        ## [Ref: GRA 6547, Ch 6, slide 196 — Granger causality as F-test on lagged x coefficients]
        print(f"\n  H₀: lagged X does not predict y beyond y's own lags")
        print(f"  Lag selected by SBIC via bivariate VAR (maxlag=104wk / 24mo = 2yr)")
        print(f"  F-test on joint significance of lagged x coefficients [GRA 6547, Ch 6, slide 196]\n")
        print(f"  [M]=monthly freq (maxlag=24mo=2yr)  [W]=weekly freq (maxlag=104wk=2yr)")
        print(f"  Monthly x uses monthly y (Σ weekly log returns).  Lag unit shown explicitly.")
        print(f"  {'Column':<38}  {'Lag':>6}  {'F-stat':>8}  {'p-value':>8}  Result")
        print(f"  {'─'*38}  {'─'*6}  {'─'*8}  {'─'*8}  ──────")

        results  = []
        skipped  = []   ## I(1) features flagged for Stage 1 correction

        for col in self._valid_cols:

            ## Monthly columns: both x and y must share the same frequency.
            ## Aggregate y to monthly (sum of weekly log returns = monthly log return)
            ## and use the true monthly x series — avoids artificial within-month repetition
            ## inflating the VAR F-statistic.
            ##
            ## maxlags: consistent 1-year economic horizon for both frequencies
            ##   Weekly:  maxlags=104 → lag 104 = 2 years (full salmon production cycle)
            ##   Monthly: maxlags=24  → lag 24  = 2 years
            ## Horizon justified by salmon lifecycle: 14–22 months smolt-to-harvest.
            ## [Ref: GRA 6547, Ch 6, chapter_6-1_var.R — VARselect(lag.max=maxlags, type="const")]
            if col in self._monthly_cols:
                x_series  = self._toMonthly(col)
                y_series  = self._yToMonthly()
                maxlags   = 24   ## 24 months = 2 years
                min_obs   = 36   ## minimum 36 monthly obs (~3 years) for VAR
                freq_tag  = "[M]"
                lag_unit  = "mo"
            else:
                x_series  = self.X.loc[self._fp_mask, col]
                y_series  = self.y.loc[self._fp_mask]
                maxlags   = 104  ## 104 weeks = 2 years
                min_obs   = 60   ## minimum 60 weekly obs for VAR
                freq_tag  = "[W]"
                lag_unit  = "wk"

            combined = pd.DataFrame({"y": y_series, "x": x_series}).dropna()

            if len(combined) < min_obs:
                continue

            ## Stationarity gate: Granger causality requires both series to be I(0).
            ## y_t is a log return — confirmed I(0) by construction (Δln price).
            ## Check x: if ADF fails to reject (I(1)), the VAR F-statistic has a
            ## non-standard distribution → F-test invalid.
            ## Course approach: transform to I(0) before VAR (chapter_6-1_var.R runs
            ## Granger on log returns, never on price levels).
            ## [Ref: GRA 6547, Ch 6, chapter_6-1_var.R lines 33–46]
            ## Action: flag I(1) features for Stage 1 correction (apply Δln in
            ## FeatureEngineer), then re-run EDA on the stationary version.
            try:
                _, adf_x_p, *_ = adfuller(combined["x"], autolag="AIC", regression="c")
            except Exception as e:
                print(f"  ⚠ [{col}] ADF pre-check failed — {type(e).__name__}: {e}")
                continue

            if adf_x_p >= 0.05:
                col_disp = (col[:36] + "..") if len(col) > 38 else col
                print(f"  {col_disp:<36} {freq_tag}  {'─':>3}  {'─':>8}  {'─':>8}"
                      f"  ⚠ I(1) p={adf_x_p:.3f} — apply Δln in Stage 1, re-run")
                skipped.append(col)
                continue

            try:
                ## Build VAR once — reused for lag selection and fitting (50+ features, efficiency).
                ## Equivalent to R: VARselect(cbind(y,x), lag.max=maxlags, type="const")$selection["SC(n)"]
                ##
                ## Cap maxlags to T//3 (common heuristic: need at least 3 obs per lag parameter).
                ## For features with 1000+ obs this cap has no effect (min(52, 333)=52).
                ## At the min_obs boundary (e.g. T=60), it prevents evaluating VAR(52) on 8
                ## effective observations — near-singular and degenerate BIC.
                ## Spirit: course uses lag.max=10 on ~200 obs (T/p≈20) [chapter_6-1_var.R].
                ## statsmodels requires T > 3 × maxlags + 1 (neqs=2, trend="c").
                ## Use (T − 2) // 3 so integer arithmetic never lands exactly on the boundary.
                effective_maxlags = min(maxlags, max(1, (len(combined) - 2) // 3))
                var_model = VAR(combined)
                lag_order = var_model.select_order(maxlags=effective_maxlags)
                p         = int(lag_order.bic)
                if p < 1:
                    p = 1  ## minimum 1 lag

                ## Fit VAR(p) and test Granger causality: does x cause y?
                ## Equivalent to R: causality(var_fit, cause="x")$Granger
                var_fit  = var_model.fit(p)
                gc_test  = var_fit.test_causality(caused="y", causing="x", kind="f")
                f_stat   = float(gc_test.test_statistic)
                p_val    = float(gc_test.pvalue)

                result   = "Granger-causes y ✓" if p_val < 0.05 else "No Granger causality"
                col_disp = (col[:36] + "..") if len(col) > 38 else col
                print(f"  {col_disp:<36} {freq_tag}  {p:>3}{lag_unit}  "
                      f"{f_stat:>8.3f}  {p_val:>8.4f}  {result}")

                results.append({
                    "Column"          : col,
                    "Frequency"       : "monthly" if col in self._monthly_cols else "weekly",
                    "SBIC_Lag"        : p,
                    "SBIC_Lag_Period" : f"{p}{lag_unit}",   ## e.g. "3wk" or "2mo"
                    "F_stat"          : round(f_stat, 4),
                    "p_value"         : round(p_val,  4),
                    "Result"          : result,
                })

            except Exception as e:
                print(f"  ⚠ [{col}] Granger failed — {type(e).__name__}: {e}")
                continue

        ## Append skipped I(1) features to the output DataFrame with Result="I(1) — skipped".
        ## This gives the caller a structured record of which features need Stage 1 correction,
        ## not just a console message that disappears after the run.
        skipped_rows = [{
            "Column"          : c,
            "Frequency"       : "monthly" if c in self._monthly_cols else "weekly",
            "SBIC_Lag"        : None,
            "SBIC_Lag_Period" : None,
            "F_stat"          : None,
            "p_value"         : None,
            "Result"          : "I(1) — apply Δln in Stage 1, re-run",
        } for c in skipped]

        df          = pd.DataFrame(results + skipped_rows)
        significant = df[df["p_value"] < 0.05]["Column"].tolist() if len(df) > 0 else []

        print(f"\n  → {len(significant)} / {len(results)} features Granger-cause y at 5%")
        if skipped:
            print(f"  → {len(skipped)} feature(s) skipped — I(1), invalid F-test:")
            for c in skipped:
                print(f"       · {c}  → apply Δln in FeatureEngineer (Stage 1), re-run EDA")
        print(f"  → OLS: use significant lags to inform lag structure in Stage 1")
        ## CatBoost handles irrelevant features natively — no pre-filtering needed.
        ## [Ref: GRA 6518, Messy Data lecture — "redundant variables example, GBM unaffected"]
        print(f"  → CatBoost: keep all features — handles irrelevant features natively [GRA 6518, Messy Data]")
        ## Full-sample Granger selection then used as CV input = data snooping.
        ## Feature selection is part of the estimation procedure.
        ## [Ref: GRA 6518, Lesson 5 — "variable selection is part of the estimation procedure"]
        print(f"  ⚠ Full-sample selection → data snooping — embed inside expanding window for CV [GRA 6518, Lesson 5]")
        print(f"\n{self.SEP}\n")

        return df

    # ─────────────────────────────────────────────────────────────────────────
    # 2.5  COINTEGRATION ANALYSIS
    # ─────────────────────────────────────────────────────────────────────────

    ##
    #  Engle-Granger cointegration test following GRA 6547, Ch 7 procedure.
    #  [Ref: chapter_7-2_cointegration_ecm.R — OLS in levels → ADF on residuals → ECM]
    #
    #  Candidates: Granger-significant I(1) features from Stage 2.4.
    #  Pairs tested: salmon log price level ~ each candidate.
    #
    #  Restricting to Granger-significant features avoids all-pairs multiple testing
    #  while remaining data-driven (no industry knowledge required).
    #  Justification: if x does not Granger-cause y, a long-run relationship between
    #  their levels is unlikely to improve salmon return forecasts.
    #
    #  @param granger_df  DataFrame from grangerCausality() — filters candidates to
    #                     Granger-significant features. If None, all I(1) features used.
    #  @return  DataFrame with EG results per pair
    #
    def cointegration(self, granger_df: pd.DataFrame = None) -> pd.DataFrame:

        print(f"\n{self.SEP}")
        print(f"  2.5  COINTEGRATION ANALYSIS  (Engle-Granger two-step)")
        print(f"{self.SEP}")
        print(f"  [Ref: GRA 6547, Ch 7, chapter_7-2_cointegration_ecm.R]")

        ## Reconstruct salmon log price level.
        ## y_t = Δln(P_t) → ln(P_t) = cumsum(y_t) + constant (constant irrelevant for cointegration).
        ## ADF confirms I(1) — price levels almost always fail to reject unit root.
        _SALMON_LVL = "__SalmonLogLevel"
        _y_dated    = (
            pd.DataFrame({
                "Date": self._factors.loc[self._fp_mask, "Date"],
                "val" : self.y.loc[self._fp_mask],
            })
            .dropna().set_index("Date")["val"]
        )
        salmon_lvl_w = _y_dated.cumsum()
        salmon_lvl_m = salmon_lvl_w.resample("ME").last()

        try:
            _, _adf_p_lvl, *_ = adfuller(salmon_lvl_w, autolag="AIC", regression="c")
            if _adf_p_lvl < 0.05:
                print(f"  ⚠ Salmon log price level ADF p={_adf_p_lvl:.4f} — stationary, cointegration skipped")
                print(f"\n{self.SEP}\n")
                return pd.DataFrame()
            print(f"  Salmon log price level: ADF p={_adf_p_lvl:.4f} — I(1) ✓")
        except Exception as e:
            print(f"  ⚠ ADF on salmon log price level failed — {e}")
            print(f"\n{self.SEP}\n")
            return pd.DataFrame()

        ## Step 1: restrict candidates to Granger-significant features, then confirm I(1).
        ## Granger filter: if x does not predict y in the short run, testing its long-run
        ## relationship with salmon price levels adds no forecasting value.
        ## [Ref: GRA 6547, Ch 6, slide 196 — Granger causality as predictive relevance filter]
        if granger_df is not None and not granger_df.empty:
            sig_cols   = set(granger_df[granger_df["p_value"] < 0.05]["Column"].tolist())
            candidates = [c for c in self._valid_cols if c in sig_cols]
            print(f"\n  Candidates: {len(candidates)} Granger-significant features (filtered from {len(self._valid_cols)} valid)")
        else:
            candidates = self._valid_cols
            print(f"\n  No granger_df supplied — using all {len(candidates)} valid features as candidates")

        i1_cols = []
        for col in candidates:
            series  = self._toMonthly(col) if col in self._monthly_cols \
                      else self.X.loc[self._fp_mask, col].dropna()
            min_obs = 12 if col in self._monthly_cols else 52
            if len(series) < min_obs:
                continue
            try:
                _, adf_p, *_ = adfuller(series, autolag="AIC", regression="c")
                if adf_p >= 0.05:
                    i1_cols.append(col)
            except Exception as e:
                print(f"  ⚠ [{col}] ADF failed — {type(e).__name__}: {e}")

        print(f"  I(1) candidates (ADF p ≥ 0.05): {len(i1_cols)}")
        for c in i1_cols:
            print(f"    · {c}")

        if not i1_cols:
            print(f"\n  No I(1) candidates — cointegration skipped")
            print(f"\n{self.SEP}\n")
            return pd.DataFrame()

        ## Step 2: EG test — salmon log level ~ each I(1) candidate.
        ## Procedure (GRA 6547, Ch 7, chapter_7-2_cointegration_ecm.R):
        ##   1. OLS in levels: ln(Salmon_t) = α + β·ln(X_t) + û_t
        ##   2. ADF on û_t using MacKinnon critical values (statsmodels.tsa.stattools.coint)
        ##      Standard ADF critical values are too liberal for estimated residuals.
        ##      [Ref: GRA 6547, Ch 7, slide 245 — MacKinnon response surfaces]
        ##   3. Reject H₀ (p < 0.05) → cointegrated → construct ECM term
        print(f"\n── ENGLE-GRANGER  (MacKinnon critical values) ───────────────────────")
        print(f"  H₀: no cointegration  |  Pair: salmon log level ~ X")
        print(f"  {'X Feature':<45}  {'EG stat':>8}  {'p-value':>8}  Result")
        print(f"  {'─'*45}  {'─'*8}  {'─'*8}  ──────")

        results = []
        for x_col in i1_cols:

            ## Downsample to monthly if X is a broadcast-monthly column.
            pair_is_monthly = x_col in self._monthly_cols
            salmon_ser      = salmon_lvl_m if pair_is_monthly else salmon_lvl_w
            x_ser           = self._toMonthly(x_col) if pair_is_monthly \
                              else self.X.loc[self._fp_mask, x_col]
            combined        = pd.DataFrame({"y": salmon_ser, "x": x_ser}).dropna()
            min_obs         = 36 if pair_is_monthly else 52

            if len(combined) < min_obs:
                print(f"  ⚠ [{x_col[:43]}] skipped — {len(combined)} obs < min {min_obs}")
                continue

            try:
                eg_stat, eg_p, _ = coint(combined["y"].values, combined["x"].values)
                eg_result        = "Cointegrated ✓" if eg_p < 0.05 else "Not cointegrated"
            except Exception as e:
                eg_stat, eg_p, eg_result = None, None, f"Error: {e}"

            col_disp = (x_col[:41] + "..") if len(x_col) > 43 else x_col
            if eg_stat is not None:
                print(f"  {col_disp:<45}  {eg_stat:>8.4f}  {eg_p:>8.4f}  {eg_result}")
            else:
                print(f"  {col_disp:<45}  {'N/A':>8}  {'N/A':>8}  {eg_result}")

            results.append({
                "X_col"    : x_col,
                "EG_stat"  : round(eg_stat, 4) if eg_stat is not None else None,
                "EG_p"     : round(eg_p,    4) if eg_p    is not None else None,
                "EG_result": eg_result,
            })

        coint_found = [r for r in results if r["EG_result"] == "Cointegrated ✓"]
        print(f"\n  → {len(coint_found)} cointegrated pair(s) found")
        if coint_found:
            print(f"  → ECM term: z_{{t-1}} = ln(Salmon_{{t-1}}) − β̂·ln(X_{{t-1}})")
            print(f"  → Add z_{{t-1}} to FeatureEngineer (Stage 1)")
            ## Sign constraint: β₂ < 0 in Δy_t = α + β₂·z_{t-1} + ...
            ## z_{t-1} > 0 means salmon is above long-run equilibrium → next return negative.
            ## β₂ ≥ 0 → no mean reversion → relationship may be spurious → drop term.
            ## [Ref: GRA 6547, Ch 7, slide 250 — sign constraint on adjustment coefficient]
            print(f"  → Verify β₂ < 0 in Stage 3 OLS (mean-reversion required) [GRA 6547, Ch 7, slide 250]")
            for r in coint_found:
                print(f"       · {r['X_col']}")
        else:
            print(f"  → No cointegration confirmed — difference all I(1) features in Stage 1")

        print(f"\n{self.SEP}\n")

        return pd.DataFrame(results)

    # ─────────────────────────────────────────────────────────────────────────
    # 2.6  HETEROSCEDASTICITY & VOLATILITY — MOVED TO STAGE 3
    # ─────────────────────────────────────────────────────────────────────────
    #
    #  White's test, Breusch-Godfrey, and ARCH-LM all require residuals from
    #  the actual estimated OLS model — which is not available until Stage 3.
    #  Running them on a preliminary or arbitrary OLS here would produce results
    #  that do not reflect the true model specification.
    #
    #  Stage 3 runs:
    #    White's test  — heteroscedasticity → use HC3 robust SEs if rejected
    #                    [Ref: GRA 6547, Ch 3, slides 79–80 — test stat = TR² from auxiliary regression]
    #    BG test       — serial correlation at q=1,4,13 → use HAC SEs or add lags if rejected
    #                    [Ref: GRA 6547, Ch 3, slides 84–85 — test stat = (T−r)R² ~ χ²(r)]
    #    ARCH-LM       — volatility clustering at q=1,4 → add GARCH σ̂²_t to CatBoost if rejected
    #                    [Ref: GRA 6547, Ch 8, slides 270–271 — regress û² on lagged û²]
    #    GARCH(1,1)    — if ARCH-LM rejects: require α₁+β<1 for covariance stationarity
    #                    [Ref: GRA 6547, Ch 8, slide 275 — IGARCH if constraint violated]
    #

    # ─────────────────────────────────────────────────────────────────────────
    # 2.7  MULTICOLLINEARITY DIAGNOSTICS
    # ─────────────────────────────────────────────────────────────────────────

    ##
    #  Correlation matrix heatmap and VIF for all valid features.
    #  Flags pairs |ρ| > 0.9 and VIF > 10.
    #
    #  @return  DataFrame with VIF scores per feature
    #
    def multicollinearity(self) -> pd.DataFrame:

        print(f"\n{self.SEP}")
        print(f"  2.7  MULTICOLLINEARITY DIAGNOSTICS")
        print(f"{self.SEP}")

        X_fp      = self.X.loc[self._fp_mask, self._valid_cols].copy()
        X_fp      = X_fp.dropna(axis=1, thresh=int(self._fp_mask.sum() * 0.5))
        n_before  = len(X_fp)
        X_fp      = X_fp.dropna()
        n_after   = len(X_fp)
        print(f"\n  Listwise deletion: {n_before} → {n_after} rows "
              f"({round((1 - n_after / n_before) * 100, 1)}% dropped due to NaNs across features)"
              if n_before > 0 else "")

        if X_fp.empty:
            print(f"  ⚠ No complete rows after listwise deletion — "
                  f"multicollinearity diagnostics skipped.")
            print(f"  → Reduce feature set or impute before running multicollinearity.")
            print(f"\n{self.SEP}\n")
            return pd.DataFrame()

        ## Add y_t as the last column so the heatmap shows target correlations.
        ## y_t is aligned to X_fp's index (same rows after listwise deletion).
        ## Label it clearly so it stands out in the plot.
        ## y_t is NOT included in VIF — VIF measures collinearity among regressors only.
        _Y_LABEL  = "── y_t (target)"
        y_aligned = self.y.loc[X_fp.index].rename(_Y_LABEL)
        corr_full = pd.concat([X_fp, y_aligned], axis=1).corr()   ## includes y_t
        corr_X    = X_fp.corr()                                    ## features only (for VIF + pairs)

        ## High correlation pairs — features only (X-X), not X-y
        cols_list  = corr_X.columns.tolist()
        high_pairs = []
        for i in range(len(cols_list)):
            for j in range(i + 1, len(cols_list)):
                rho = corr_X.iloc[i, j]
                if abs(rho) > 0.9:
                    high_pairs.append((cols_list[i], cols_list[j], round(rho, 3)))

        ## Feature-target correlations — printed separately for interpretability
        feat_target = (
            corr_full[_Y_LABEL]
            .drop(index=_Y_LABEL)           ## drop y_t's self-correlation (1.0)
            .sort_values(key=abs, ascending=False)
        )

        ## Multicollinearity inflates Var(β̂) for OLS via (X'X)⁻¹.
        ## [Ref: GRA 6547, Ch 3 — multicollinearity and its effect on OLS variance]
        ## [Ref: GRA 6518, Lesson 6, slide 7 — correlated inputs inflate (X'X)⁻¹]
        print(f"\n── HIGH CORRELATION PAIRS  (|ρ| > 0.9, features only) ───────────────")
        if high_pairs:
            for c1, c2, rho in high_pairs:
                print(f"  ρ={rho:>6}  {c1}  ↔  {c2}")
                ## Drop one from OLS (collinearity inflates variance); keep both for CatBoost.
                ## [Ref: GRA 6518, Messy Data lecture — "high or perfect collinearity... GBM unaffected"]
                print(f"           → Drop one from OLS | Keep both for CatBoost [GRA 6518, Messy Data]")
        else:
            print(f"  None found")

        ## Feature-target correlations — ranked by |ρ|
        ## Useful for understanding sign and magnitude of raw linear association with y_t.
        ## Does NOT replace Granger (which accounts for lags and y's own history),
        ## but complements it as a quick direction check.
        print(f"\n── FEATURE-TARGET CORRELATIONS  ρ(feature, y_t) ────────────────────")
        print(f"  Ranked by |ρ|.  Sign shows direction of raw linear association.")
        print(f"  {'Column':<42}  {'ρ':>7}")
        print(f"  {'─'*42}  {'─'*7}")
        for col, rho in feat_target.items():
            col_disp = (col[:40] + "..") if len(col) > 42 else col
            print(f"  {col_disp:<42}  {rho:>7.4f}")

        ## Correlation heatmap — includes y_t as the last row/column
        ## Sorted so y_t appears at the bottom for easy reading
        fig, ax = plt.subplots(figsize=(16, 14))
        sns.heatmap(corr_full, cmap="RdBu_r", center=0, vmin=-1, vmax=1,
                    ax=ax, annot=False, linewidths=0.3)
        ax.set_title("Correlation Matrix — Features + y_t (target in last row/column)")
        plt.tight_layout()
        plt.savefig("eda_correlation_matrix.pdf")
        plt.close()
        print(f"\n  Saved: eda_correlation_matrix.pdf")

        ## VIF = 1/(1−R²ⱼ), where R²ⱼ is from regressing feature j on all other features.
        ## VIF > 10 indicates severe multicollinearity → inflated OLS standard errors.
        ## [Ref: GRA 6547, Ch 3 — VIF as multicollinearity diagnostic]
        ##
        ## Note on I(1) features: VIF computed on I(1) series in levels is interpretable
        ## as a collinearity diagnostic (correlated trends inflate VIF), but OLS on I(1)
        ## features produces spurious regressions [GRA 6547, Ch 7, slide 218].
        ## Re-run VIF after Stage 1 applies Δln to non-stationary features — VIF scores
        ## will change on the stationary versions.
        print(f"\n── VARIANCE INFLATION FACTORS ───────────────────────────────────────")
        print(f"  VIF > 10 = severe multicollinearity [GRA 6547, Ch 3]")
        print(f"  Note: VIF on I(1) features shown for reference — re-run after Stage 1 applies Δln")
        print(f"  {'Column':<40}  {'VIF':>8}  Flag")
        print(f"  {'─'*40}  {'─'*8}  ────")

        X_vif       = pd.DataFrame(
                          np.column_stack([np.ones(len(X_fp)), X_fp.values]),
                          columns=["const"] + X_fp.columns.tolist()
                      )
        vif_results = []

        for i, col in enumerate(X_vif.columns):
            if col == "const":
                continue   ## VIF is undefined for the constant — skip
            try:
                vif      = variance_inflation_factor(X_vif.values, i)
                flag     = "⚠ Drop from OLS" if vif > 10 else ""
                col_disp = (col[:38] + "..") if len(col) > 40 else col
                print(f"  {col_disp:<40}  {vif:>8.2f}  {flag}")
                vif_results.append({"Column": col, "VIF": round(vif, 2)})
            except Exception as e:
                print(f"  ⚠ [{col}] VIF failed — {type(e).__name__}: {e}")
                continue

        ## PCA as dimensionality reduction if severe multicollinearity among OLS features.
        ## [Ref: GRA 6547, Ch 3, chapter_3-4_pca.R — PCA for collinear regressors]
        ## [Ref: GRA 6518, Lesson 6 — "dimension reduction" as response to multicollinearity]
        print(f"\n  → OLS: remove high-VIF features or apply PCA [GRA 6547, Ch 3; GRA 6518, Lesson 6]")
        ## CatBoost: tree splits are invariant to collinearity — (X'X)⁻¹ is never computed.
        ## [Ref: GRA 6518, Messy Data lecture — "high or perfect collinearity... GBM unaffected"]
        print(f"  → CatBoost: multicollinearity does not inflate variance — keep all [GRA 6518, Messy Data]")
        print(f"\n{self.SEP}\n")

        return pd.DataFrame(vif_results)

    # ─────────────────────────────────────────────────────────────────────────
    # 2.8  FEATURE PLOT PDF
    # ─────────────────────────────────────────────────────────────────────────

    ##
    #  Saves one page per valid feature to a multi-page PDF.
    #
    #  Layout per page (4 panels):
    #    Top (full width) — time series (full history, FishPool shaded, ADF/KPSS annotated)
    #    Bottom left      — histogram + fitted normal overlay (JB annotated)
    #                       [Ref: GRA Quant Risk, Assignment1.R — hist + dnorm overlay]
    #    Bottom centre    — ACF up to lag 104wk / 24mo (Ljung-Box annotated)
    #    Bottom right     — PACF up to lag 104wk / 24mo
    #                       [Ref: GRA Rsrch Meth Ch5, chapter_5-2_acf_and_pacf.R]
    #
    #  ADF, KPSS, and JB test statistics are annotated directly on each page
    #  so the PDF is self-contained (no need to cross-reference the summary table).
    #
    #  Monthly columns use true monthly values (downsampled) in all three panels.
    #
    #  @param path        output PDF file path
    #  @param stat_df     optional DataFrame from stationarityTests() — used to
    #                     annotate ADF/KPSS results on the time series panel.
    #                     If None, ADF/KPSS are re-computed for each column.
    #
    def plotAllFeatures(self,
                        path    : str            = "eda_feature_plots.pdf",
                        stat_df : pd.DataFrame   = None) -> None:

        print(f"\n{self.SEP}")
        print(f"  2.8  FEATURE PLOT PDF")
        print(f"{self.SEP}")
        print(f"\n  Generating {len(self._valid_cols)} pages → {path}")

        ## Build lookup for stationarity results if provided
        stat_lookup = {}
        if stat_df is not None and len(stat_df) > 0:
            for _, row in stat_df.iterrows():
                stat_lookup[row["Column"]] = row

        ## FishPool start date for shading
        fp_dates = self._factors.loc[self._fp_mask, "Date"]
        fp_start = fp_dates.iloc[0] if len(fp_dates) > 0 else None
        fp_end   = fp_dates.iloc[-1] if len(fp_dates) > 0 else None
        dates    = self._factors["Date"]

        with PdfPages(path) as pdf:

            for col in self._valid_cols:

                is_monthly = col in self._monthly_cols
                freq_tag   = "[Monthly]" if is_monthly else "[Weekly]"
                ## max_lag consistent with _LAGS_MONTHLY/_LAGS_WEEKLY maximum (2-year horizon)
                max_lag    = 24 if is_monthly else 104

                ## Series for plotting
                series_full = self.X[col]
                series_plot = self._toMonthly(col) if is_monthly \
                              else self.X.loc[self._fp_mask, col].dropna()
                clean = series_plot.dropna()

                ## Layout: time series full-width top row,
                ##         histogram | ACF | PACF in bottom row
                ## [Ref: GRA Rsrch Meth Ch5, chapter_5-2_acf_and_pacf.R — ACF and PACF together]
                fig = plt.figure(figsize=(16, 7))
                fig.suptitle(f"{col}  {freq_tag}", fontsize=9, fontweight="bold")
                gs     = gridspec.GridSpec(2, 3, figure=fig, hspace=0.35, wspace=0.3)
                ax_ts   = fig.add_subplot(gs[0, :])    ## top row: full width
                ax_hist = fig.add_subplot(gs[1, 0])    ## bottom left
                ax_acf  = fig.add_subplot(gs[1, 1])    ## bottom centre
                ax_pacf = fig.add_subplot(gs[1, 2])    ## bottom right

                # ── Time series (top, full width) ────────────────────────────
                ax_ts.plot(dates, series_full, lw=0.8, color="steelblue")
                if fp_start is not None:
                    ax_ts.axvspan(fp_start, fp_end, alpha=0.12, color="orange")
                    ax_ts.text(0.98, 0.03, "● FishPool period",
                               transform=ax_ts.transAxes, fontsize=6,
                               va="bottom", ha="right", color="darkorange")

                ## Annotate ADF/KPSS decision from stationarity table
                if col in stat_lookup:
                    r   = stat_lookup[col]
                    txt = (f"ADF p={r['ADF_p']}  KPSS p={r['KPSS_p']}  "
                           f"→ {r['Decision']}  Action: {r['Action']}")
                else:
                    try:
                        _, adf_p, *_ = adfuller(clean, autolag="AIC", regression="c")
                        _, kss_p, *_ = kpss(clean, regression="c", nlags="auto")
                        txt = f"ADF p={adf_p:.3f}  KPSS p={kss_p:.3f}"
                    except Exception as e:
                        print(f"  ⚠ [{col}] ADF/KPSS plot annotation failed — {type(e).__name__}: {e}")
                        txt = ""
                ax_ts.set_title(f"Time Series  |  {txt}", fontsize=7.5)
                ax_ts.tick_params(labelsize=7)

                # ── Histogram + normal overlay (bottom left) ─────────────────
                ## Course approach: hist + dnorm overlay (GRA Quant Risk, Assignment1.R)
                ## Red curve = JB H₀: if data were normal it would follow this line
                if len(clean) > 8:
                    clean.hist(bins=min(40, len(clean) // 3), density=True,
                               alpha=0.5, ax=ax_hist, color="steelblue",
                               label="Empirical")
                    x_range = np.linspace(clean.min(), clean.max(), 300)
                    ax_hist.plot(x_range,
                                 stats.norm.pdf(x_range,
                                                loc=clean.mean(),
                                                scale=clean.std()),
                                 color="red", lw=1.5, label="Normal (H₀)")
                    jb_stat, jb_p = stats.jarque_bera(clean)
                    jb_txt = (f"JB p={jb_p:.3f} "
                              f"{'Non-normal ✗' if jb_p < 0.05 else 'Normal ✓'}\n"
                              f"Skew={clean.skew():.2f}  Kurt={clean.kurtosis():.2f}")
                    ax_hist.text(0.02, 0.97, jb_txt, transform=ax_hist.transAxes,
                                 fontsize=6, va="top",
                                 bbox=dict(boxstyle="round,pad=0.2",
                                           fc="white", alpha=0.7))
                    ax_hist.legend(fontsize=6)
                ax_hist.set_title("Distribution (FishPool)", fontsize=8)
                ax_hist.tick_params(labelsize=7)

                # ── ACF (bottom centre) ──────────────────────────────────────
                ## [Ref: GRA Rsrch Meth Ch5, chapter_5-2_acf_and_pacf.R]
                ## ACF identifies MA order; slow decay → I(1) candidate
                ##
                ## Ljung-Box annotation uses the same lag reference structure
                ## as all other EDA tests (_LAGS_MONTHLY / _LAGS_WEEKLY)
                if is_monthly:
                    lb_lag   = self._LAGS_MONTHLY        ## [1, 6, 12]
                    lag_note = "lag 1=1mo | 6=6mo | 12=1yr"
                else:
                    lb_lag   = self._LAGS_WEEKLY         ## [4, 26, 52, 104]
                    lag_note = "lag 4=1mo | 26=6mo | 52=1yr | 104=2yr"

                if len(clean) >= max_lag + 2:
                    try:
                        plot_acf(clean, lags=max_lag, ax=ax_acf, zero=False)
                    except Exception as e:
                        print(f"  ⚠ [{col}] ACF plot failed — {type(e).__name__}: {e}")
                        ax_acf.text(0.5, 0.5, "ACF failed", ha="center", va="center",
                                    transform=ax_acf.transAxes, fontsize=8)
                    try:
                        lb_lag_valid = [l for l in lb_lag if l <= max_lag]
                        lb = acorr_ljungbox(clean, lags=lb_lag_valid, return_df=True)
                        lb_txt = "  ".join(
                            [f"LB({lg}) p={lb.loc[lg,'lb_pvalue']:.3f}"
                             for lg in lb_lag_valid])
                        ax_acf.text(0.02, 0.97, lb_txt,
                                    transform=ax_acf.transAxes, fontsize=6,
                                    va="top",
                                    bbox=dict(boxstyle="round,pad=0.2",
                                              fc="white", alpha=0.7))
                    except Exception as e:
                        print(f"  ⚠ [{col}] LB annotation failed — {type(e).__name__}: {e}")
                else:
                    ax_acf.text(0.5, 0.5, "Insufficient data",
                                ha="center", va="center",
                                transform=ax_acf.transAxes, fontsize=8)
                ax_acf.set_title(
                    f"ACF  (max lag={max_lag})  |  {lag_note}", fontsize=7)
                ax_acf.tick_params(labelsize=7)

                # ── PACF (bottom right) ──────────────────────────────────────
                ## [Ref: GRA Rsrch Meth Ch5, chapter_5-2_acf_and_pacf.R]
                ## PACF identifies AR order; sharp cutoff after lag 1 → AR(1)
                ## ACF slow decay + PACF sharp cutoff at lag 1 → likely I(1)
                if len(clean) >= max_lag + 2:
                    try:
                        plot_pacf(clean, lags=max_lag, ax=ax_pacf, zero=False,
                                  method="ywm")
                    except Exception as e:
                        print(f"  ⚠ [{col}] PACF plot failed — {type(e).__name__}: {e}")
                        ax_pacf.text(0.5, 0.5, "PACF failed", ha="center", va="center",
                                     transform=ax_pacf.transAxes, fontsize=8)
                else:
                    ax_pacf.text(0.5, 0.5, "Insufficient data",
                                 ha="center", va="center",
                                 transform=ax_pacf.transAxes, fontsize=8)
                ax_pacf.set_title(
                    f"PACF  (max lag={max_lag})  |  {lag_note}", fontsize=7)
                ax_pacf.tick_params(labelsize=7)

                pdf.savefig(fig, bbox_inches="tight")
                plt.close(fig)

        print(f"  Saved: {path}  ({len(self._valid_cols)} pages)\n")
        print(f"  Each page: time series (top) | histogram + normal overlay | ACF | PACF")
        print(f"  ADF, KPSS, JB, Ljung-Box results annotated directly on plots")
        print(f"\n{self.SEP}\n")

    # ─────────────────────────────────────────────────────────────────────────
    # 2.9  FULL REPORT
    # ─────────────────────────────────────────────────────────────────────────

    ##
    #  Runs all EDA sections in sequence.
    #
    def report(self) -> None:

        n_fp  = int(self._fp_mask.sum())
        n_col = len(self._valid_cols)

        print(f"\n{'█'*72}")
        print(f"  EDA REPORT — Salmon Price Forecasting")
        print(f"  FishPool period: {n_fp} obs  |  Valid features: {n_col}")
        print(f"{'█'*72}")

        self.distributionAnalysis()
        stat_df = self.stationarityTests()
        self.temporalDependence()
        gc_df   = self.grangerCausality()
        self.cointegration(granger_df=gc_df)
        self.multicollinearity()
        self.plotAllFeatures(stat_df=stat_df)

        ## Final gate summary
        gc_skipped = gc_df[gc_df["Result"] == "I(1) — apply Δln in Stage 1, re-run"][
            "Column"].tolist() if len(gc_df) > 0 else []

        print(f"\n{'█'*72}")
        print(f"  EDA COMPLETE — Iteration gate:")
        print(f"  → Non-stationary features:   return to Stage 1 (FeatureEngineer)")
        if gc_skipped:
            print(f"  → Granger I(1) skipped ({len(gc_skipped)}): apply Δln in Stage 1, re-run EDA")
            for c in gc_skipped:
                print(f"       · {c}")
        print(f"  → Significant Granger lags:   update lag structure in Stage 1")
        print(f"  → Cointegrated pairs:         add ECM term in Stage 1")
        print(f"  → High VIF / |ρ| > 0.9:       inform OLS pre-specification decision")
        print(f"  → White's, BG, ARCH-LM:       run in Stage 3 on actual OLS residuals")
        print(f"{'█'*72}\n")
