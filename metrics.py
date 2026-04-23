##
#  Metrics — shared evaluation functions used across all model scripts
#
#  All methods are static: no instance needed, call as Metrics.rmse(...) etc.
##

import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, r2_score as _sklearn_r2
from scipy import stats


class Metrics:
    """Shared evaluation metrics and formatting helpers."""

    @staticmethod
    def rmse(actual, pred):
        return np.sqrt(mean_squared_error(actual, pred))

    @staticmethod
    def rw_rmse(actual):
        """RMSE of the random walk baseline (predicts 0 for log-returns)."""
        return np.sqrt(np.mean(actual ** 2))

    @staticmethod
    def r2(actual, pred):
        return _sklearn_r2(actual, pred)

    @staticmethod
    def hit_rate(actual, pred):
        return np.mean(np.sign(pred) == np.sign(actual))

    @staticmethod
    def skill_score(hold_rmse, rw_rmse):
        """RMSE skill score vs random walk in percent. Positive = better than RW."""
        if hold_rmse is None or rw_rmse is None or rw_rmse == 0:
            return None
        return (1 - hold_rmse / rw_rmse) * 100

    @staticmethod
    def diebold_mariano(actual, pred):
        """DM test vs random walk (RW predicts 0 for log-returns)."""
        e_model = actual - pred
        e_rw    = actual
        d       = e_model ** 2 - e_rw ** 2
        n       = len(d)
        dm_stat = np.mean(d) / (np.std(d, ddof=1) / np.sqrt(n))
        p_value = 2 * stats.norm.sf(np.abs(dm_stat))
        return dm_stat, p_value

    @staticmethod
    def fmt(x, fmt=".4f"):
        if pd.isna(x) or x is None:
            return "—"
        return f"{x:{fmt}}"
