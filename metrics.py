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

    # RMSE of baseline (Random walk)
    def rw_rmse(self, actual):
        return np.sqrt(np.mean(actual ** 2))

    # R² of baseline (Random walk predicts zero for log-return targets)
    def rw_r2(self, actual):
        return _sklearn_r2(actual, np.zeros_like(actual))

    # R² of model predictions vs actual values
    def r2(self, actual, pred):
        return _sklearn_r2(actual, pred)

    # Hit rate: percentage of predictions with correct sign (directional accuracy)
    def hit_rate(self, actual, pred):
        return np.mean(np.sign(pred) == np.sign(actual))

    # Skill score of models vs random walk: percentage improvement in RMSE over RW baseline (Pos = better than RW)
    def skill_score(self, hold_rmse, rw_rmse):
        if hold_rmse is None or rw_rmse is None or rw_rmse == 0:
            return None
        return (1 - hold_rmse / rw_rmse) * 100

    # Clark-West test (2006/2007) — correct test when benchmark (RW=0) is nested inside the model
    # Adjusts for the upward bias in the larger model's MSE under the null, giving better size and
    # power than DM in small samples and at long horizons with overlapping returns
    def diebold_mariano(self, actual, pred, horizon=1):
        n       = len(actual)
        e_model = actual - pred
        e_rw    = actual          # RW predicts zero

        # Clark-West adjustment: add back the expected overfitting penalty of the larger model
        # f_t = e_rw^2 - e_model^2 + (pred - 0)^2  (CW eq. 3)
        f       = e_rw ** 2 - e_model ** 2 + pred ** 2
        f_mean  = np.mean(f)

        # Newey-West HAC variance of f (bandwidth = horizon - 1 covers overlap autocorrelation)
        bandwidth = max(horizon - 1, 0)
        gamma_0   = np.mean((f - f_mean) ** 2)
        nw_var    = gamma_0
        for k in range(1, bandwidth + 1):
            weight  = 1 - k / (bandwidth + 1)
            gamma_k = np.mean((f[k:] - f_mean) * (f[:-k] - f_mean))
            nw_var += 2 * weight * gamma_k

        # One-sided t(n-1): H0 equal accuracy, H1 model beats RW
        cw_stat = f_mean / np.sqrt(nw_var / n) if nw_var > 0 else 0.0
        p_value = stats.t.sf(cw_stat, df=n - 1)
        return cw_stat, p_value

    # Formats a metric value for display in tables, handling NaN/None
    def fmt(self, x, fmt=".4f"):
        if pd.isna(x) or x is None:
            return "—"
        return f"{x:{fmt}}"
