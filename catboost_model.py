##
#  CatBoost model — all Y horizons
#  Purged walk-forward CV (folds and purge window adjusted per horizon)
#  Final holdout: 2022–2025
##

import pandas as pd
import numpy as np
import re
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

Y_COLS   = [c for c in df.columns if c.startswith("Y ")]
NON_FEAT = {"Date"} | set(Y_COLS)
ALL_FEAT = [c for c in df.columns if c not in NON_FEAT]

os.makedirs("Results", exist_ok=True)

## ── Horizon config ───────────────────────────────────────────────────────────
# purge_weeks: rows removed before each test fold to prevent label overlap
# n_folds:     fewer folds for longer horizons to preserve training data
HORIZON_CONFIG = {
    "0w":  {"purge_weeks":  0, "n_folds": 10},
    "1w":  {"purge_weeks":  1, "n_folds": 10},
    "2w":  {"purge_weeks":  2, "n_folds": 10},
    "1m":  {"purge_weeks":  4, "n_folds": 10},
    "3m":  {"purge_weeks": 13, "n_folds":  8},
    "6m":  {"purge_weeks": 26, "n_folds":  6},
    "12m": {"purge_weeks": 52, "n_folds":  4},
}

def parse_horizon(target):
    """Extract horizon key from target name, e.g. 'Y 12m ∆ Salmon' → '12m'."""
    m = re.search(r"Y (\d+(?:w|m)) ", target)
    return m.group(1) if m else "0w"

## ── CatBoost settings ────────────────────────────────────────────────────────
CB_PARAMS = dict(
    iterations          = 500,
    learning_rate       = 0.05,
    depth               = 6,
    loss_function       = "RMSE",
    random_seed         = 42,
    verbose             = False,
    allow_writing_files = False,
)

## ── Purged walk-forward CV ───────────────────────────────────────────────────
def purged_wf_cv(data, target, features, purge_weeks, n_folds):
    """
    Expanding-window walk-forward CV with purging.

    The CV period is split into n_folds equal time blocks.
    For fold i: train on all data before fold i start minus purge_weeks rows,
    test on fold i block. First fold is used as seed training only (no test).
    Returns list of (dates, actuals, preds) per fold.
    """
    dates = data["Date"].values
    n     = len(data)

    fold_size = n // n_folds
    fold_results = []

    for i in range(1, n_folds):           # fold 0 = seed training window only
        test_start_idx = i * fold_size
        test_end_idx   = (i + 1) * fold_size if i < n_folds - 1 else n

        train_end_idx  = test_start_idx - purge_weeks   # purge gap
        if train_end_idx < fold_size:                   # need at least 1 fold of training
            continue

        train = data.iloc[:train_end_idx]
        test  = data.iloc[test_start_idx:test_end_idx]

        train = train.dropna(subset=[target])
        test  = test.dropna(subset=[target])
        if len(train) < 50 or len(test) == 0:
            continue

        model = CatBoostRegressor(**CB_PARAMS)
        model.fit(train[features], train[target])
        preds = model.predict(test[features])

        fold_results.append({
            "dates":   test["Date"].values,
            "actuals": test[target].values,
            "preds":   preds,
            "fold":    i,
        })

    return fold_results

## ── Run per Y horizon ────────────────────────────────────────────────────────
summary = []

for target in Y_COLS:
    horizon    = parse_horizon(target)
    cfg        = HORIZON_CONFIG.get(horizon, {"purge_weeks": 0, "n_folds": 10})
    purge_wks  = cfg["purge_weeks"]
    n_folds    = cfg["n_folds"]

    # CV data: everything before holdout
    cv_data    = df[df["Date"] < HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)
    # Holdout: 2022 onwards
    hold_data  = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    print(f"\n{target}  (horizon={horizon}, purge={purge_wks}w, folds={n_folds})")

    ## ── Purged CV ────────────────────────────────────────────────────────────
    fold_results = purged_wf_cv(cv_data, target, ALL_FEAT, purge_wks, n_folds)

    if fold_results:
        cv_preds   = np.concatenate([f["preds"]   for f in fold_results])
        cv_actuals = np.concatenate([f["actuals"] for f in fold_results])
        cv_dates   = np.concatenate([f["dates"]   for f in fold_results])

        cv_rmse = np.sqrt(mean_squared_error(cv_actuals, cv_preds))
        cv_r2   = r2_score(cv_actuals, cv_preds)
        print(f"  CV       RMSE={cv_rmse:.4f}  R²={cv_r2:.4f}  "
              f"(n_obs={len(cv_actuals)}, folds={len(fold_results)})")
    else:
        cv_rmse, cv_r2, cv_preds, cv_actuals, cv_dates = None, None, [], [], []
        print("  CV       insufficient data for all folds")

    ## ── Final model: train on full CV period, evaluate on holdout ────────────
    final_model = CatBoostRegressor(**CB_PARAMS)
    final_model.fit(cv_data[ALL_FEAT], cv_data[target])

    hold_preds   = final_model.predict(hold_data[ALL_FEAT])
    hold_actuals = hold_data[target].values
    hold_rmse    = np.sqrt(mean_squared_error(hold_actuals, hold_preds))
    hold_r2      = r2_score(hold_actuals, hold_preds)
    print(f"  Holdout  RMSE={hold_rmse:.4f}  R²={hold_r2:.4f}  "
          f"(n={len(hold_data)})")

    summary.append({
        "Y":           target,
        "Horizon":     horizon,
        "CV RMSE":     cv_rmse,
        "CV R2":       cv_r2,
        "Hold RMSE":   hold_rmse,
        "Hold R2":     hold_r2,
        "n_train":     len(cv_data),
        "n_holdout":   len(hold_data),
    })

    ## ── Feature importance ───────────────────────────────────────────────────
    importance = pd.Series(
        final_model.get_feature_importance(),
        index=ALL_FEAT
    ).sort_values(ascending=False)

    ## ── Plot ─────────────────────────────────────────────────────────────────
    n_axes = 3 if len(cv_preds) > 0 else 2
    fig, axes = plt.subplots(n_axes, 1, figsize=(14, 5 * n_axes))
    fig.suptitle(target, fontsize=13)

    ax_idx = 0
    if len(cv_preds) > 0:
        axes[ax_idx].plot(cv_dates, cv_actuals, label="Actual",    alpha=0.8)
        axes[ax_idx].plot(cv_dates, cv_preds,   label="Predicted", alpha=0.8)
        axes[ax_idx].set_title(f"Purged CV  (RMSE={cv_rmse:.4f}, R²={cv_r2:.4f})")
        axes[ax_idx].legend(); axes[ax_idx].grid(True, alpha=0.3)
        ax_idx += 1

    axes[ax_idx].plot(hold_data["Date"].values, hold_actuals, label="Actual",    alpha=0.8)
    axes[ax_idx].plot(hold_data["Date"].values, hold_preds,   label="Predicted", alpha=0.8)
    axes[ax_idx].set_title(f"Holdout 2022–2025  (RMSE={hold_rmse:.4f}, R²={hold_r2:.4f})")
    axes[ax_idx].legend(); axes[ax_idx].grid(True, alpha=0.3)
    ax_idx += 1

    importance.head(20).sort_values().plot(kind="barh", ax=axes[ax_idx])
    axes[ax_idx].set_title("Top 20 feature importances (trained on full CV period)")
    axes[ax_idx].grid(True, alpha=0.3)

    plt.tight_layout()
    safe_name = target.replace("/", "-").replace(" ", "_")
    plt.savefig(f"Results/{safe_name}.png", dpi=120)
    plt.close()

## ── Summary table ────────────────────────────────────────────────────────────
print("\n── Summary ──────────────────────────────────────────────────────────")
summary_df = pd.DataFrame(summary).set_index("Y")
print(summary_df.to_string())
summary_df.to_csv("Results/model_summary.csv")
