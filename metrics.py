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

    # DM test to check statistical significance of different model performance vs RW
    def diebold_mariano(self, actual, pred, horizon=1):
        e_model = actual - pred
        e_rw    = actual
        d       = e_model ** 2 - e_rw ** 2
        n       = len(d)
        d_mean  = np.mean(d)

        # Newey-West HAC variance estimator
        bandwidth = max(horizon - 1, 0)
        gamma_0   = np.mean((d - d_mean) ** 2)
        nw_var    = gamma_0
        for k in range(1, bandwidth + 1):
            weight  = 1 - k / (bandwidth + 1) 
            gamma_k = np.mean((d[k:] - d_mean) * (d[:-k] - d_mean))
            nw_var += 2 * weight * gamma_k

        dm_stat = d_mean / np.sqrt(nw_var / n) if nw_var > 0 else 0.0
        p_value = 2 * stats.norm.sf(np.abs(dm_stat))
        return dm_stat, p_value

    # Formats a metric value for display in tables, handling NaN/None
    def fmt(self, x, fmt=".4f"):
        if pd.isna(x) or x is None:
            return "—"
        return f"{x:{fmt}}"
