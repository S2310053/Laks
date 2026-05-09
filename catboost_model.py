# This module construct the CatBoost model

##
# We construct our CatBoost model per Y horizon, using purged (=horizon) k-fold CV (Prado, 2018) with early stopping to select tree depth and evaluate performance
# As it's time series data, we split into folds sequentially (not randomly), as this might result in leakage
# The final model is trained on all training data and evaluated on the holdout (2022–2026)
##

import pandas as pd
import numpy as np
import re
from catboost import CatBoostRegressor
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from metrics import Metrics
metrics = Metrics()
from plotter import Plotter

# Plot style based on Thesis template
plt.rcParams["font.family"]      = "Times New Roman"
plt.rcParams["mathtext.fontset"] = "custom"
plt.rcParams["mathtext.rm"]      = "Times New Roman"
plt.rcParams["mathtext.it"]      = "Times New Roman:italic"
plt.rcParams["mathtext.bf"]      = "Times New Roman:bold"

_BLUE = "#1A6B8A"
_DARK = "#0D3B5E"
_GREY = "dimgrey"

# Helper to style axes consistently
def _style_ax(ax, ylabel=""):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, linestyle="--", alpha=0.1)
    if ylabel:
        ax.set_ylabel(ylabel, fontweight="bold")
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

# Load data (feature_model must be run first to create this file)
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])

# Final test set: 01-22 to 04-26
HOLDOUT_START = "2022-01-01"

# Identify Y columns and features
Y_COLS   = [c for c in df.columns if c.startswith("Y ")]
NON_FEAT = {"Date"} | set(Y_COLS)
ALL_FEAT = [c for c in df.columns if c not in NON_FEAT]

# Create folder to store results
RESULTS_DIR = "Results/CatBoost"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Defines purge_weeks, n_folds, and tree depth per horizon
# Purge_weeks is the number of weeks to exclude between train and test sets in CV to prevent leakage (= horizon length)
# n_folds is the number of CV folds (adjusted bawsed on horizon to ensure enough data per fold after purging)
# Note: We are open to tune these further, to improve performance. Especially n_folds for the longer horizons,
# as 12m only produces 2 reliable CV folds
HORIZON_CONFIG = {
    "0w":  {"purge_weeks":  0, "n_folds": 10},
    "1w":  {"purge_weeks":  1, "n_folds": 10},
    "2w":  {"purge_weeks":  2, "n_folds": 10},
    "1m":  {"purge_weeks":  4, "n_folds": 10},
    "3m":  {"purge_weeks": 13, "n_folds":  8},
    "6m":  {"purge_weeks": 26, "n_folds":  6},
    "12m": {"purge_weeks": 52, "n_folds":  3}}

# CatBoost settings
# Note: We used base parameters, but these can be changed if it can improve the model
CB_BASE = dict(
    iterations          = 1000, 
    loss_function       = "RMSE", 
    random_seed         = 42,
    verbose             = False,
    allow_writing_files = False,
)
EARLY_STOP_ROUNDS = 50
VAL_FRAC          = 0.15
DEPTH_GRID        = [2, 3, 4, 5, 6, 7, 8, 9]

# Helper to extract horizon from Y column name
def parse_horizon(target):
    m = re.search(r"Y (\d+(?:w|m)) ", target)
    return m.group(1) if m else "0w"

# Purged k-fold CV (Prado et al. 2018 article on purged CV)
# Note: As it's time-series data, we split into folds sequentially (not randomly), as this might result in leakage
def purged_cv(data, target, features, purge_weeks, n_folds, depth):
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

        val_size    = max(int(len(train) * VAL_FRAC), 10)
        train_inner = train.iloc[:-val_size]
        val_inner   = train.iloc[-val_size:]

        model = CatBoostRegressor(**CB_BASE, depth=depth)
        model.fit(
            train_inner[features], train_inner[target],
            eval_set              = (val_inner[features].values, val_inner[target].values),
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

# CV depth: We choose depth based on CV with RMSE as selection metric
def cv_select_depth(data, target, features, purge_weeks, n_folds, depth_grid):
    best_depth   = depth_grid[0]
    best_rmse    = np.inf
    best_results = []

    for d in depth_grid:
        results = purged_cv(data, target, features, purge_weeks, n_folds, d)
        if not results:
            continue
        preds   = np.concatenate([f["preds"]   for f in results])
        actuals = np.concatenate([f["actuals"] for f in results])
        rmse    = np.sqrt(np.mean((actuals - preds) ** 2))
        print(f"depth={d}  CV RMSE={rmse:.4f}  (folds={len(results)})")
        if rmse < best_rmse:
            best_rmse    = rmse
            best_depth   = d
            best_results = results

    return best_depth, best_results

# Run model per Y horizon
summary = []

for target in Y_COLS:
    horizon    = parse_horizon(target)
    cfg        = HORIZON_CONFIG.get(horizon)
    if cfg is None:
        continue
    purge_wks  = cfg["purge_weeks"]
    n_folds    = cfg["n_folds"]
    is_nowcast = (horizon == "0w") # Labelled nowcast, since we are estimating current week returns and not future returns
    label = f"{target} [NOWCAST]" if is_nowcast else target

    cv_data   = df[df["Date"] < HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)
    hold_data = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)

    # Embargo: final model training ends purge_wks before holdout so no training target overlaps holdout returns
    embargo_date = pd.Timestamp(HOLDOUT_START) - pd.Timedelta(weeks=purge_wks)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    print(f"\n{label}  (horizon={horizon}, purge={purge_wks}w, folds={n_folds})")

    # Purged CV with depth selection
    print(f"Depth CV over {DEPTH_GRID}:")
    depth, fold_results = cv_select_depth(cv_data, target, ALL_FEAT, purge_wks, n_folds, DEPTH_GRID)
    print(f"Selected depth={depth}")

    if fold_results:
        cv_preds   = np.concatenate([f["preds"]   for f in fold_results])
        cv_actuals = np.concatenate([f["actuals"] for f in fold_results])
        cv_dates   = np.concatenate([f["dates"]   for f in fold_results])

        cv_rmse    = metrics.rmse(cv_actuals, cv_preds)
        cv_rw_rmse = metrics.rw_rmse(cv_actuals)
        cv_r2      = metrics.r2(cv_actuals, cv_preds)
        cv_rw_r2   = metrics.rw_r2(cv_actuals)
        cv_hitrate = metrics.hit_rate(cv_actuals, cv_preds)
        print(f"CV RMSE={cv_rmse:.4f}  RW_RMSE={cv_rw_rmse:.4f}  "
              f"R²={cv_r2:.4f}  Hit={cv_hitrate:.1%}  "
              f"(n_obs={len(cv_actuals)}, folds={len(fold_results)})")
    else:
        cv_rmse, cv_rw_rmse, cv_r2, cv_rw_r2, cv_hitrate = None, None, None, None, None
        cv_preds, cv_actuals, cv_dates = [], [], []
        print("CV: insufficient data for all folds")

    # Final model trained on embargoed training data (no target overlap with holdout)
    train_final    = cv_data[cv_data["Date"] < embargo_date].copy()
    val_size_final = max(int(len(train_final) * VAL_FRAC), 10)
    cv_train_final = train_final.iloc[:-val_size_final]
    cv_val_final   = train_final.iloc[-val_size_final:]

    final_model = CatBoostRegressor(**CB_BASE, depth=depth)
    final_model.fit(
        cv_train_final[ALL_FEAT], cv_train_final[target],
        eval_set              = (cv_val_final[ALL_FEAT].values, cv_val_final[target].values),
        early_stopping_rounds = EARLY_STOP_ROUNDS,
    )

    hold_preds    = final_model.predict(hold_data[ALL_FEAT])
    hold_actuals  = hold_data[target].values
    hold_rmse     = metrics.rmse(hold_actuals, hold_preds)
    hold_rw_rmse  = metrics.rw_rmse(hold_actuals)
    hold_r2       = metrics.r2(hold_actuals, hold_preds)
    hold_rw_r2    = metrics.rw_r2(hold_actuals)
    hold_hitrate  = metrics.hit_rate(hold_actuals, hold_preds)
    dm_stat, dm_p = metrics.diebold_mariano(hold_actuals, hold_preds, horizon=max(purge_wks, 1))

    # Print holdout results
    print(f"  Holdout  RMSE={hold_rmse:.4f}  RW_RMSE={hold_rw_rmse:.4f}  "
          f"R²={hold_r2:.4f}  RW_R²={hold_rw_r2:.4f}  Hit={hold_hitrate:.1%}  "
          f"DM={dm_stat:.2f}  p={dm_p:.3f}")

    # Save holdout predictions for model comparison plots
    pd.DataFrame({
        "Date": hold_data["Date"].values,
        "Actual": hold_actuals,
        "Predicted": hold_preds,
    }).to_csv(f"{RESULTS_DIR}/holdout_preds_{target.replace('/', '-').replace(' ', '_')}.csv", index=False)

    # Summary of results for per horizon
    summary.append({
        "Y":             target,
        "Horizon":       horizon,
        "Nowcast":       is_nowcast,
        "CV RMSE":       cv_rmse,
        "CV RW RMSE":    cv_rw_rmse,
        "CV R2":         cv_r2,
        "CV RW R2":      cv_rw_r2,
        "CV Hit":        cv_hitrate,
        "Hold RMSE":     hold_rmse,
        "Hold RW RMSE":  hold_rw_rmse,
        "Hold R2":       hold_r2,
        "Hold RW R2":    hold_rw_r2,
        "Hold Hit":      hold_hitrate,
        "Hold DM":       dm_stat,
        "Hold DM p":     dm_p,
        "Depth":         depth,
        "n_train":       len(cv_data),
        "n_holdout":     len(hold_data),
    })

    # Feature importance
    importance = pd.Series(
        final_model.get_feature_importance(),
        index=ALL_FEAT
    ).sort_values(ascending=False)

    print(f"  Top 10 features:")
    for feat, score in importance.head(10).items():
        print(f"    {score:6.2f}  {feat}")

    # Plot
    n_axes = 3 if len(cv_preds) > 0 else 2
    fig, axes = plt.subplots(n_axes, 1, figsize=(14, 5 * n_axes), facecolor="white")
    fig.suptitle(f"{target}{'[NOWCAST]' if is_nowcast else ''}",
                 fontsize=13, fontweight="bold")

    ax_idx = 0
    if len(cv_preds) > 0:
        axes[ax_idx].plot(cv_dates, cv_actuals, label="Actual",    color=_DARK, lw=1.5, alpha=0.85)
        axes[ax_idx].plot(cv_dates, cv_preds,   label="Predicted", color=_BLUE, lw=1.2, alpha=0.85)
        axes[ax_idx].axhline(0, color=_GREY, lw=0.8, ls="--")
        axes[ax_idx].set_title(f"Purged CV — RMSE={cv_rmse:.4f}  |  RW={cv_rw_rmse:.4f}  |  R²={cv_r2:.4f}",
                               fontsize=10)
        axes[ax_idx].legend(frameon=False, fontsize=9)
        _style_ax(axes[ax_idx], ylabel="∆ Price (NOK/kg)")
        ax_idx += 1

    axes[ax_idx].plot(hold_data["Date"].values, hold_actuals, label="Actual",    color=_DARK, lw=1.5, alpha=0.85)
    axes[ax_idx].plot(hold_data["Date"].values, hold_preds,   label="Predicted", color=_BLUE, lw=1.2, alpha=0.85)
    axes[ax_idx].axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
    axes[ax_idx].set_title(f"Holdout 2022–2025 — RMSE={hold_rmse:.4f} | RW={hold_rw_rmse:.4f}  |  "
                           f"R²={hold_r2:.4f} | DM p={dm_p:.3f}", fontsize=10)
    axes[ax_idx].legend(frameon=False, fontsize=9)
    _style_ax(axes[ax_idx], ylabel="∆ Price (NOK/kg)")
    ax_idx += 1

    imp_vals = importance.head(20).sort_values()
    axes[ax_idx].barh(imp_vals.index, imp_vals.values, color=_BLUE, alpha=0.85)
    axes[ax_idx].set_title(f"Top 20 Feature Importances  (depth={depth})", fontsize=10)
    axes[ax_idx].set_xlabel("Importance", fontweight="bold")
    axes[ax_idx].spines["top"].set_visible(False)
    axes[ax_idx].spines["right"].set_visible(False)
    axes[ax_idx].grid(True, linestyle="--", alpha=0.1)

    plt.tight_layout()
    safe_name = target.replace("/", "-").replace(" ", "_")
    plt.savefig(f"{RESULTS_DIR}/{safe_name}.pdf", format="pdf", bbox_inches="tight")
    plt.close()

#Summary table
print("\n Summary of CatBoost model results:")
summary_df = pd.DataFrame(summary).set_index("Y")
print(summary_df.to_string())
summary_df.to_csv(f"{RESULTS_DIR}/catboost_summary.csv")

# PDF results table
disp = pd.DataFrame({
    "Horizon":   [r["Horizon"] for r in summary],
    "CV RMSE":   [metrics.fmt(r["CV RMSE"]) for r in summary],
    "CV R²":     [metrics.fmt(r["CV R2"], ".3f") for r in summary],
    "CV Hit":    [f'{r["CV Hit"]:.1%}' if r["CV Hit"] else "—" for r in summary],
    "Hold RMSE": [metrics.fmt(r["Hold RMSE"]) for r in summary],
    "Hold R²":   [metrics.fmt(r["Hold R2"], ".3f") for r in summary],
    "Hold Hit":  [f'{r["Hold Hit"]:.1%}' if r["Hold Hit"] else "—" for r in summary],
    "RW RMSE":   [metrics.fmt(r["Hold RW RMSE"]) for r in summary],
    "Skill %":   [f'{(1 - r["Hold RMSE"]/r["Hold RW RMSE"])*100:+.1f}%' for r in summary],
    "DM":        [metrics.fmt(r["Hold DM"], ".2f") for r in summary],
    "p-value":   [f'{r["Hold DM p"]:.4f}' if r["Hold DM p"] >= 0.001
                  else "< 0.001" for r in summary],
    "Depth":     [r["Depth"] for r in summary],
})

Plotter().results_table(
    disp,
    "CatBoost — Results Summary\nHoldout: 2022–2026 | Purged CV | Depth CV'd over {2,...,9}",
    f"{RESULTS_DIR}/catboost_results.pdf",
    width=16,
)
