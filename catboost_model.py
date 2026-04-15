##
#  CatBoost model — all Y horizons
#  Purged walk-forward CV (folds and purge window adjusted per horizon)
#  Includes: random walk baseline, Diebold-Mariano test, early stopping
#  Final holdout: 2022–2025
#
#  Note: Y 0w is a nowcast (S_t and F_t are Wednesday closes, simultaneously
#  determined). Results for Y 0w should be labelled nowcast, not forecast.
##

import pandas as pd
import numpy as np
import re
from catboost import CatBoostRegressor
from sklearn.metrics import mean_squared_error, r2_score
from scipy import stats
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
HORIZON_CONFIG = {
    "0w":  {"purge_weeks":  0, "n_folds": 10},
    "1w":  {"purge_weeks":  1, "n_folds": 10},
    "2w":  {"purge_weeks":  2, "n_folds": 10},
    "1m":  {"purge_weeks":  4, "n_folds": 10},
    "3m":  {"purge_weeks": 13, "n_folds":  8},
    "6m":  {"purge_weeks": 26, "n_folds":  6},
    "12m": {"purge_weeks": 52, "n_folds":  4},  # Note: only ~2 valid CV folds — CV R² unreliable; rely on holdout
}

def parse_horizon(target):
    m = re.search(r"Y (\d+(?:w|m)) ", target)
    return m.group(1) if m else "0w"

## ── CatBoost settings ────────────────────────────────────────────────────────
CB_PARAMS = dict(
    iterations          = 1000,     # higher ceiling; early stopping will cut this
    learning_rate       = 0.05,
    depth               = 6,
    loss_function       = "RMSE",
    random_seed         = 42,
    verbose             = False,
    allow_writing_files = False,
)
EARLY_STOP_ROUNDS = 50
VAL_FRAC          = 0.15            # fraction of training used as early-stopping validation

## ── Diebold-Mariano test (MSE loss, RW predicts 0 log return) ────────────────
def diebold_mariano(actual, pred_model):
    """
    H0: equal MSE vs random walk (which predicts 0 log return).
    Negative DM stat → model is more accurate than RW.
    Returns (dm_stat, p_value).
    """
    e_model = actual - pred_model
    e_rw    = actual                        # RW error = actual - 0
    d       = e_model**2 - e_rw**2
    n       = len(d)
    dm_stat = np.mean(d) / (np.std(d, ddof=1) / np.sqrt(n))
    p_value = 2 * stats.norm.sf(np.abs(dm_stat))
    return dm_stat, p_value

## ── Purged walk-forward CV ───────────────────────────────────────────────────
def purged_wf_cv(data, target, features, purge_weeks, n_folds):
    """
    Expanding-window walk-forward CV with purging and early stopping.
    Early stopping uses the last VAL_FRAC of each training slice as validation.
    """
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

        # Early stopping: hold out last VAL_FRAC of training
        val_size    = max(int(len(train) * VAL_FRAC), 10)
        train_inner = train.iloc[:-val_size]
        val_inner   = train.iloc[-val_size:]

        model = CatBoostRegressor(**CB_PARAMS)
        model.fit(
            train_inner[features], train_inner[target],
            eval_set   = (val_inner[features].values, val_inner[target].values),
            early_stopping_rounds = EARLY_STOP_ROUNDS,
        )
        preds = model.predict(test[features])

        results.append({
            "dates":   test["Date"].values,
            "actuals": test[target].values,
            "preds":   preds,
            "fold":    i,
        })

    return results

## ── Run per Y horizon ────────────────────────────────────────────────────────
summary = []

for target in Y_COLS:
    horizon   = parse_horizon(target)
    cfg       = HORIZON_CONFIG.get(horizon, {"purge_weeks": 0, "n_folds": 10})
    purge_wks = cfg["purge_weeks"]
    n_folds   = cfg["n_folds"]
    is_nowcast = (horizon == "0w")

    cv_data   = df[df["Date"] < HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)
    hold_data = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    label = f"{target}  [NOWCAST]" if is_nowcast else target
    print(f"\n{label}  (horizon={horizon}, purge={purge_wks}w, folds={n_folds})")

    ## ── Purged CV ────────────────────────────────────────────────────────────
    fold_results = purged_wf_cv(cv_data, target, ALL_FEAT, purge_wks, n_folds)

    if fold_results:
        cv_preds   = np.concatenate([f["preds"]   for f in fold_results])
        cv_actuals = np.concatenate([f["actuals"] for f in fold_results])
        cv_dates   = np.concatenate([f["dates"]   for f in fold_results])

        cv_rmse    = np.sqrt(mean_squared_error(cv_actuals, cv_preds))
        cv_rw_rmse = np.sqrt(np.mean(cv_actuals**2))          # RW predicts 0
        cv_r2      = r2_score(cv_actuals, cv_preds)
        cv_hitrate = np.mean(np.sign(cv_preds) == np.sign(cv_actuals))
        print(f"  CV       RMSE={cv_rmse:.4f}  RW_RMSE={cv_rw_rmse:.4f}  "
              f"R²={cv_r2:.4f}  Hit={cv_hitrate:.1%}  "
              f"(n_obs={len(cv_actuals)}, folds={len(fold_results)})")
    else:
        cv_rmse, cv_rw_rmse, cv_r2, cv_hitrate = None, None, None, None
        cv_preds, cv_actuals, cv_dates = [], [], []
        print("  CV       insufficient data for all folds")

    ## ── Final model: train on full CV period, evaluate on holdout ────────────
    val_size_final    = max(int(len(cv_data) * VAL_FRAC), 10)
    cv_train_final    = cv_data.iloc[:-val_size_final]
    cv_val_final      = cv_data.iloc[-val_size_final:]

    final_model = CatBoostRegressor(**CB_PARAMS)
    final_model.fit(
        cv_train_final[ALL_FEAT], cv_train_final[target],
        eval_set              = (cv_val_final[ALL_FEAT].values, cv_val_final[target].values),
        early_stopping_rounds = EARLY_STOP_ROUNDS,
    )

    hold_preds    = final_model.predict(hold_data[ALL_FEAT])
    hold_actuals  = hold_data[target].values
    hold_rmse     = np.sqrt(mean_squared_error(hold_actuals, hold_preds))
    hold_rw_rmse  = np.sqrt(np.mean(hold_actuals**2))
    hold_r2       = r2_score(hold_actuals, hold_preds)
    hold_hitrate  = np.mean(np.sign(hold_preds) == np.sign(hold_actuals))
    dm_stat, dm_p = diebold_mariano(hold_actuals, hold_preds)

    print(f"  Holdout  RMSE={hold_rmse:.4f}  RW_RMSE={hold_rw_rmse:.4f}  "
          f"R²={hold_r2:.4f}  Hit={hold_hitrate:.1%}  "
          f"DM={dm_stat:.2f}  p={dm_p:.3f}")

    summary.append({
        "Y":            target,
        "Horizon":      horizon,
        "Nowcast":      is_nowcast,
        "CV RMSE":      cv_rmse,
        "CV RW RMSE":   cv_rw_rmse,
        "CV R2":        cv_r2,
        "CV Hit":       cv_hitrate,
        "Hold RMSE":    hold_rmse,
        "Hold RW RMSE": hold_rw_rmse,
        "Hold R2":      hold_r2,
        "Hold Hit":     hold_hitrate,
        "Hold DM":      dm_stat,
        "Hold DM p":    dm_p,
        "n_train":      len(cv_data),
        "n_holdout":    len(hold_data),
    })

    ## ── Feature importance ───────────────────────────────────────────────────
    importance = pd.Series(
        final_model.get_feature_importance(),
        index=ALL_FEAT
    ).sort_values(ascending=False)

    print(f"  Top 10 features:")
    for feat, score in importance.head(10).items():
        print(f"    {score:6.2f}  {feat}")

    ## ── Plot ─────────────────────────────────────────────────────────────────
    n_axes = 3 if len(cv_preds) > 0 else 2
    fig, axes = plt.subplots(n_axes, 1, figsize=(14, 5 * n_axes))
    fig.suptitle(f"{target}{' [NOWCAST]' if is_nowcast else ''}", fontsize=13)

    ax_idx = 0
    if len(cv_preds) > 0:
        axes[ax_idx].plot(cv_dates, cv_actuals, label="Actual",    alpha=0.8)
        axes[ax_idx].plot(cv_dates, cv_preds,   label="Predicted", alpha=0.8)
        axes[ax_idx].set_title(f"Purged CV  (RMSE={cv_rmse:.4f}, RW={cv_rw_rmse:.4f}, R²={cv_r2:.4f})")
        axes[ax_idx].legend(); axes[ax_idx].grid(True, alpha=0.3)
        ax_idx += 1

    axes[ax_idx].plot(hold_data["Date"].values, hold_actuals, label="Actual",    alpha=0.8)
    axes[ax_idx].plot(hold_data["Date"].values, hold_preds,   label="Predicted", alpha=0.8)
    axes[ax_idx].set_title(f"Holdout 2022–2025  (RMSE={hold_rmse:.4f}, RW={hold_rw_rmse:.4f}, "
                           f"R²={hold_r2:.4f}, DM p={dm_p:.3f})")
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
