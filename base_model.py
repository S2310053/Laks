##
#  BaseModel — superclass for all forecasting models
#
#  Holds shared constants and composes Metrics + Plotter utilities.
#  Subclasses override predict() with their specific fitting logic.
#
#  Inheritance hierarchy:
#    BaseModel
#      ├── CatBoostModel   (catboost_model.py)
#      ├── LassoModel      (lasso_model.py)
#      ├── SARIMAModel     (sarima_model.py)
#      └── OLSModel        (ols_model.py)
#
#  ModelComparison (model_comparison.py) uses composition:
#    it holds a list of BaseModel objects and calls evaluate() on each.
##

import os
from metrics import Metrics
from plotter import Plotter


class BaseModel:
    """Abstract base for walk-forward forecasting models.

    Class variables
    ---------------
    HOLDOUT_START : str   — first date of the holdout period (shared across all models)
    HORIZON_ORDER : list  — canonical horizon ordering for tables and plots
    """

    HOLDOUT_START = "2022-01-01"
    HORIZON_ORDER = ["0w", "1w", "2w", "1m", "3m", "6m", "12m"]

    def __init__(self, results_dir):
        self._results_dir = results_dir
        self._metrics     = Metrics()
        self._plotter     = Plotter()
        self._summary     = []
        os.makedirs(results_dir, exist_ok=True)

    def predict(self, X_train, y_train, X_test):
        """Override in subclass: fit on (X_train, y_train), return predictions for X_test."""
        raise NotImplementedError

    @property
    def summary(self):
        """Read-only access to accumulated per-horizon results."""
        return self._summary
