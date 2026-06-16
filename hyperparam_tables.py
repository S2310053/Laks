# This module builds the boosting hyperparameter tables for the appendix
#
##
# Supervisor requested a hyperparameter table for the boosting models. These read the tuned
# values already saved by the walk-forward runs (catboost_summary.csv / htboost_summary.csv) —
# no model is re-fit. Because the purged k-fold reuses CatBoost's walk-forward depth, the same
# table documents both methods; HTBoost auto-tunes per fit, so the per-fold values live in the
# PurgedKFold/perfold_metrics_*.csv files and the headline (final-model) values are shown here.
##

import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
from plotter import Plotter
matplotlib.use("Agg", force=True)

OUT = "Results/Appendix"
os.makedirs(OUT, exist_ok=True)
HZ = ["0w", "1w", "2w", "1m", "3m", "6m", "12m"]


def _col(series, conv=lambda v: str(int(v))):
    return [conv(series[h]) if h in series.index and pd.notna(series[h]) else "—" for h in HZ]


# ---- CatBoost ----
cb_r = pd.read_csv("Results/CatBoost/RMSE/catboost_summary.csv").set_index("Horizon")
cb_m = pd.read_csv("Results/CatBoost/MAE/catboost_summary.csv").set_index("Horizon")
cb = pd.DataFrame({
    "Horizon":      HZ,
    "Depth (RMSE)": _col(cb_r["Depth"]),
    "Depth (MAE)":  _col(cb_m["Depth"]),
})
cb.to_csv(f"{OUT}/catboost_hyperparams.csv", index=False)
print("CatBoost hyperparameters:")
print(cb.to_string(index=False))
Plotter().results_table(
    cb,
    "CatBoost — Selected Hyperparameters by Horizon\n"
    "Tree depth selected by purged CV over {2,…,9}. Fixed: iterations=1000 (early stopping 50 "
    "rounds), learning_rate = CatBoost auto, validation fraction = 0.15, random_seed = 42.",
    f"{OUT}/catboost_hyperparams.pdf", width=14)

# ---- HTBoost ----
ht_r = pd.read_csv("Results/HTBoost/RMSE/htboost_summary.csv").set_index("Horizon")
ht_m = pd.read_csv("Results/HTBoost/MAE/htboost_summary.csv").set_index("Horizon")
ht = pd.DataFrame({
    "Horizon":        HZ,
    "Depth (RMSE)":   _col(ht_r["Depth"]),
    "nTrees (RMSE)":  _col(ht_r["nTrees"]),
    "Depth (MAE)":    _col(ht_m["Depth"]),
    "nTrees (MAE)":   _col(ht_m["nTrees"]),
})
ht.to_csv(f"{OUT}/htboost_hyperparams.csv", index=False)
print("\nHTBoost hyperparameters:")
print(ht.to_string(index=False))
Plotter().results_table(
    ht,
    "HTBoost — Auto-tuned Hyperparameters by Horizon\n"
    "Depth and number of trees auto-tuned by HTBoost internal CV (modality = compromise). "
    "Fixed: max trees = 2000, randomizecv = false, overlap = h−1, loss = L2 (RMSE) / quantile@0.5 (MAE).",
    f"{OUT}/htboost_hyperparams.pdf", width=16)

print(f"\nHyperparameter tables → {OUT}/")
