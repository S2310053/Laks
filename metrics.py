# This module constructs the Metrics class

##
# This module defines the metrics class, used as a helper in the model scripts
# It creates a shared library of evaluation functions for consistent performance measurement across all models
##

import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, r2_score as _sklearn_r2
from scipy import stats

# Shared evaluation metrics and formatting helpers
class Metrics:

    # RMSE of model predictions vs actual values
    def rmse(self, actual, pred):
        return np.sqrt(mean_squared_error(actual, pred))

    # MAE of model predictions vs actual values
    def mae(self, actual, pred):
        return np.mean(np.abs(actual - pred))

    # RMSE of baseline (Random walk predicts zero for log-return targets)
    def rw_rmse(self, actual):
        return np.sqrt(np.mean(actual ** 2))

    # MAE of baseline (Random walk predicts zero for log-return targets)
    def rw_mae(self, actual):
        return np.mean(np.abs(actual))

    # R² of baseline (Random walk predicts zero for log-return targets)
    def rw_r2(self, actual):
        return _sklearn_r2(actual, np.zeros_like(actual))

    # R² of model predictions vs actual values
    def r2(self, actual, pred):
        return _sklearn_r2(actual, pred)

    # Hit rate: percentage of predictions with correct sign (directional accuracy)
    def hit_rate(self, actual, pred):
        return np.mean(np.sign(pred) == np.sign(actual))

    # Skill score vs random walk: percentage improvement in RMSE over RW baseline (positive = better than RW)
    def skill_score(self, hold_rmse, rw_rmse):
        if hold_rmse is None or rw_rmse is None or rw_rmse == 0:
            return None
        return (1 - hold_rmse / rw_rmse) * 100

    # Diebold-Mariano test with Harvey, Leybourne & Newbold (1997) small-sample correction.
    # H0: equal predictive accuracy vs benchmark
    # H1: model beats benchmark (one-sided)
    # benchmark_pred: pass None to compare vs RW (predicts zero); pass predictions to compare vs any benchmark (e.g. OLS)
    # Correction factor sqrt((n+1-2h+h(h-1)/n)/n) adjusts for finite-sample bias;
    # p-value drawn from t(n-1) rather than N(0,1) — Harvey et al. (1997)
    def diebold_mariano(self, actual, pred, horizon=1, benchmark_pred=None):
        n       = len(actual)
        e_model = actual - pred
        e_bench = actual if benchmark_pred is None else (actual - np.asarray(benchmark_pred))

        # Squared-error loss differential: positive means model beats benchmark
        d       = e_bench ** 2 - e_model ** 2
        d_mean  = np.mean(d)

        # Newey-West HAC variance with Bartlett weights (bandwidth = h-1)
        # Bartlett weights guarantee a positive estimate, preventing degenerate results
        # at long horizons with limited observations (same approach as clark_west)
        bandwidth = max(horizon - 1, 0)
        gamma_0   = np.mean((d - d_mean) ** 2)
        hac_var   = gamma_0
        for k in range(1, bandwidth + 1):
            weight    = 1 - k / (bandwidth + 1)
            gamma_k   = np.mean((d[k:] - d_mean) * (d[:-k] - d_mean))
            hac_var  += 2 * weight * gamma_k

        # Standard DM statistic
        dm_var  = hac_var / n
        dm_stat = d_mean / np.sqrt(dm_var) if dm_var > 0 else 0.0

        # Harvey, Leybourne & Newbold (1997) finite-sample correction factor
        hlr_factor = np.sqrt((n + 1 - 2 * horizon + horizon * (horizon - 1) / n) / n)
        dm_star    = hlr_factor * dm_stat

        # One-sided p-value from t(n-1): H1 = model beats benchmark
        p_value = stats.t.sf(dm_star, df=n - 1)
        return dm_star, p_value

    # Clark-West (2006/2007) test — correct test when benchmark is nested inside the model.
    # Adjusts for the upward bias in the larger model's MSE under the null, giving better size
    # and power than DM in small samples and at long horizons with overlapping returns.
    # benchmark_pred: predictions from the nested benchmark (e.g. OLS/futures); pass None for RW (predicts zero)
    def clark_west(self, actual, pred, horizon=1, benchmark_pred=None):
        n        = len(actual)
        e_model  = actual - pred
        e_bench  = actual if benchmark_pred is None else (actual - np.asarray(benchmark_pred))
        adj      = pred   if benchmark_pred is None else (pred   - np.asarray(benchmark_pred))

        # CW adjustment: f_t = e_bench^2 - e_model^2 + (pred - bench_pred)^2  (CW eq. 3)
        f       = e_bench ** 2 - e_model ** 2 + adj ** 2
        f_mean  = np.mean(f)

        # Newey-West HAC variance of f (bandwidth = horizon - 1 covers overlap autocorrelation)
        bandwidth = max(horizon - 1, 0)
        gamma_0   = np.mean((f - f_mean) ** 2)
        nw_var    = gamma_0
        for k in range(1, bandwidth + 1):
            weight  = 1 - k / (bandwidth + 1)
            gamma_k = np.mean((f[k:] - f_mean) * (f[:-k] - f_mean))
            nw_var += 2 * weight * gamma_k

        # One-sided t(n-1): H0 equal accuracy, H1 model beats benchmark
        cw_stat = f_mean / np.sqrt(nw_var / n) if nw_var > 0 else 0.0
        p_value = stats.t.sf(cw_stat, df=n - 1)
        return cw_stat, p_value

    # Formats a metric value for display in tables, handling NaN/None
    def fmt(self, x, fmt=".4f"):
        if pd.isna(x) or x is None:
            return "—"
        return f"{x:{fmt}}"
