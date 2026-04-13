##
#  FWD-only benchmark — tests Y 1m, 3m, 6m, 12m using only the matching
#  forward price feature. Reveals whether other features add value beyond FWDs.
##

import pandas as pd
import numpy as np
from catboost import CatBoostRegressor
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import os

## ── Load data ────────────────────────────────────────────────────────────────
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
df = df.sort_values("Date").reset_index(drop=True)

HOLDOUT_START = "2022-01-01"

os.makedirs("Results", exist_ok=True)

## ── Horizon → matching FWD feature ──────────────────────────────────────────
HORIZONS = {
    "Y 1m ∆ Salmon (NOK/KG)":  {"purge_weeks":  4, "n_folds": 10, "fwd": "FWD 1m"},
    "Y 3m ∆ Salmon (NOK/KG)":  {"purge_weeks": 13, "n_folds":  8, "fwd": "FWD 3m"},
    "Y 6m ∆ Salmon (NOK/KG)":  {"purge_weeks": 26, "n_folds":  6, "fwd": "FWD 6m"},
    "Y 12m ∆ Salmon (NOK/KG)": {"purge_weeks": 52, "n_folds":  4, "fwd": "FWD 12m"},
}

CB_PARAMS = dict(
    iterations          = 500,
    learning_rate       = 0.05,
    depth               = 6,
    loss_function       = "RMSE",
    random_seed         = 42,
    verbose             = False,
    allow_writing_files = False,
)

def purged_wf_cv(data, target, features, purge_weeks, n_folds):
    n         = len(data)
    fold_size = n // n_folds
    results   = []

    for i in range(1, n_folds):
        test_start_idx = i * fold_size
        test_end_idx   = (i + 1) * fold_size if i < n_folds - 1 else n
        train_end_idx  = test_start_idx - purge_weeks

        if train_end_idx < fold_size:
            continue

        train = data.iloc[:train_end_idx].dropna(subset=[target])
        test  = data.iloc[test_start_idx:test_end_idx].dropna(subset=[target])

        if len(train) < 50 or len(test) == 0:
            continue

        model = CatBoostRegressor(**CB_PARAMS)
        model.fit(train[features], train[target])
        preds = model.predict(test[features])

        results.append({
            "dates":   test["Date"].values,
            "actuals": test[target].values,
            "preds":   preds,
        })

    return results

## ── Run benchmark ────────────────────────────────────────────────────────────
summary = []

print("── FWD-only benchmark ───────────────────────────────────────────────")

for target, cfg in HORIZONS.items():
    fwd_feat    = cfg["fwd"]
    purge_wks   = cfg["purge_weeks"]
    n_folds     = cfg["n_folds"]
    features    = [fwd_feat]

    if fwd_feat not in df.columns:
        print(f"[SKIP] {target} — {fwd_feat} not in data")
        continue

    cv_data   = df[df["Date"] < HOLDOUT_START].dropna(subset=[target, fwd_feat]).copy().reset_index(drop=True)
    hold_data = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target, fwd_feat]).copy().reset_index(drop=True)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    print(f"\n{target}  (fwd only: {fwd_feat}, purge={purge_wks}w, folds={n_folds})")

    ## CV
    fold_results = purged_wf_cv(cv_data, target, features, purge_wks, n_folds)

    if fold_results:
        cv_preds   = np.concatenate([f["preds"]   for f in fold_results])
        cv_actuals = np.concatenate([f["actuals"] for f in fold_results])
        cv_rmse    = np.sqrt(mean_squared_error(cv_actuals, cv_preds))
        cv_r2      = r2_score(cv_actuals, cv_preds)
        cv_hit     = np.mean(np.sign(cv_preds) == np.sign(cv_actuals))
        print(f"  CV       RMSE={cv_rmse:.4f}  R²={cv_r2:.4f}  Hit={cv_hit:.1%}  (n={len(cv_actuals)})")
    else:
        cv_rmse, cv_r2, cv_hit = None, None, None
        print("  CV       insufficient data")

    ## Holdout
    final_model = CatBoostRegressor(**CB_PARAMS)
    final_model.fit(cv_data[features], cv_data[target])

    hold_preds   = final_model.predict(hold_data[features])
    hold_actuals = hold_data[target].values
    hold_rmse    = np.sqrt(mean_squared_error(hold_actuals, hold_preds))
    hold_r2      = r2_score(hold_actuals, hold_preds)
    hold_hit     = np.mean(np.sign(hold_preds) == np.sign(hold_actuals))
    print(f"  Holdout  RMSE={hold_rmse:.4f}  R²={hold_r2:.4f}  Hit={hold_hit:.1%}  (n={len(hold_data)})")

    summary.append({
        "Y":          target,
        "FWD":        fwd_feat,
        "CV RMSE":    cv_rmse,
        "CV R2":      cv_r2,
        "CV Hit":     cv_hit,
        "Hold RMSE":  hold_rmse,
        "Hold R2":    hold_r2,
        "Hold Hit":   hold_hit,
    })

## ── Summary ──────────────────────────────────────────────────────────────────
print("\n── Summary ──────────────────────────────────────────────────────────")
summary_df = pd.DataFrame(summary).set_index("Y")
print(summary_df.to_string())
summary_df.to_csv("Results/fwd_benchmark_summary.csv")
