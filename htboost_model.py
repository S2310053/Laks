# This module constructs the HTBoost model

##
# We construct the HTBoost (Hybrid Tree Boosting) model per Y horizon, using the same purged k-fold CV setup as CatBoost
# HTBoost (Giordani, 2025) enhances gradient boosting by applying nonlinear transformations to tree fitted values,
# making it more data-efficient for smooth relationships — relevant for seasonal salmon price dynamics
# HTBoost handles NaN internally and auto-tunes tree depth via its own internal CV
# The model is implemented in Julia and accessed via the juliacall Python-Julia bridge
##

import pandas as pd
import numpy as np
import re
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from metrics import Metrics
metrics = Metrics()
from plotter import Plotter

# Initialise Julia and load HTBoost
print("Loading Julia + HybridTreeBoosting...")
from juliacall import Main as jl
jl.seval("using HybridTreeBoosting")
jl.seval("using DataFrames")
jl.seval("import Logging; Logging.disable_logging(Logging.Warn)")
print("Julia loaded successfully")

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
RESULTS_DIR = "Results/HTBoost"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Same horizon/fold structure as CatBoost for comparability
HORIZON_CONFIG = {
    "0w":  {"purge_weeks":  0, "n_folds": 10},
    "1w":  {"purge_weeks":  1, "n_folds": 10},
    "2w":  {"purge_weeks":  2, "n_folds": 10},
    "1m":  {"purge_weeks":  4, "n_folds": 10},
    "3m":  {"purge_weeks": 13, "n_folds":  8},
    "6m":  {"purge_weeks": 26, "n_folds":  6},
    "12m": {"purge_weeks": 52, "n_folds":  3},
}

# HTBoost settings
# modality="compromise" auto-tunes depth via internal CV (grid [2,3,5,6])
# randomizecv=false ensures block-CV (time series safe) within HTBoost's internal tuning
# loss="L2" for RMSE-equivalent; can switch to "Huber" or "t" for heavy tails
# Note: Settings can be adjusted if seen fit
HT_LOSS     = "L2"
HT_MODALITY = "compromise"
HT_NTREES   = 2000

# Helper to extract horizon from Y column name
def parse_horizon(target):
    m = re.search(r"Y (\d+(?:w|m)) ", target)
    return m.group(1) if m else "0w"

# Helper to fit HTBoost model and return predictions
def _fit_htboost(X_train, y_train, X_test, feature_names, purge_weeks=0):
    # Drop columns with >90% NaN in training — HTBoost handles NaN internally,
    # but its internal CV splits can produce sub-folds where a high-NaN column
    # becomes 100% NaN, crashing robust_mean_std on an empty vector
    nan_frac   = np.mean(np.isnan(X_train), axis=0)
    valid_cols = nan_frac < 0.3
    X_train_v  = X_train[:, valid_cols]
    X_test_v   = X_test[:, valid_cols]
    feat_v     = [f for f, v in zip(feature_names, valid_cols) if v]

    # Build pandas DataFrames with explicit float64 dtype to avoid Missing-only columns
    df_train = pd.DataFrame(X_train_v, columns=feat_v).astype(np.float64)
    df_test  = pd.DataFrame(X_test_v,  columns=feat_v).astype(np.float64)
    x_train_jl = jl.DataFrame(df_train)
    x_test_jl  = jl.DataFrame(df_test)

    param = jl.HTBparam(
        loss       = HT_LOSS,
        modality   = HT_MODALITY,
        ntrees     = HT_NTREES,
        randomizecv = False,
        nofullsample = False,
        overlap    = max(purge_weeks - 1, 0),
        verbose    = "Off",
        warnings   = "Off",
    )

    data   = jl.HTBdata(np.array(y_train, dtype=np.float64), x_train_jl, param)
    output = jl.HTBfit(data, param)
    preds  = np.array(jl.HTBpredict(x_test_jl, output))

    best_depth = int(output.bestvalue) if hasattr(output, 'bestvalue') else -1
    ntrees     = int(output.ntrees) if hasattr(output, 'ntrees') else -1

    return preds, output, data, best_depth, ntrees

# Purged k-fold CV (same structure as CatBoost for comparability)
def purged_cv(data, target, features, purge_weeks, n_folds):
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

        try:
            preds, _, _, best_depth, ntrees = _fit_htboost(
                train[features].values, train[target].values,
                test[features].values, features, purge_weeks
            )
            print(f"  fold {i}/{n_folds-1}  depth={best_depth}  ntrees={ntrees}  "
                  f"(n_train={len(train)}, n_test={len(test)})")

        except Exception as e:
            print(f"  fold {i}/{n_folds-1} FAILED: {e}")
            continue

        results.append({
            "dates":   test["Date"].values,
            "actuals": test[target].values,
            "preds":   preds,
            "fold":    i,
        })

    return results

# Run model per Y horizon
summary = []

for target in Y_COLS:
    horizon    = parse_horizon(target)
    cfg        = HORIZON_CONFIG.get(horizon)
    if cfg is None:
        continue
    purge_wks  = cfg["purge_weeks"]
    n_folds    = cfg["n_folds"]
    is_nowcast = (horizon == "0w")
    label = f"{target} [NOWCAST]" if is_nowcast else target

    cv_data   = df[df["Date"] < HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)
    hold_data = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)

    # Embargo: final model training ends purge_wks before holdout so no training target overlaps holdout returns
    embargo_date = pd.Timestamp(HOLDOUT_START) - pd.Timedelta(weeks=purge_wks)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    print(f"\n{label}  (horizon={horizon}, purge={purge_wks}w, folds={n_folds})")

    # Purged CV (HTBoost auto-tunes depth internally via modality="compromise")
    print("Running purged CV:")
    fold_results = purged_cv(cv_data, target, ALL_FEAT, purge_wks, n_folds)

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
    train_final = cv_data[cv_data["Date"] < embargo_date].copy()

    print(f"Fitting final model (n_train={len(train_final)})...")
    hold_preds, final_output, final_data, final_depth, final_ntrees = _fit_htboost(
        train_final[ALL_FEAT].values, train_final[target].values,
        hold_data[ALL_FEAT].values, ALL_FEAT, purge_wks
    )

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
          f"DM={dm_stat:.2f}  p={dm_p:.3f}  (depth={final_depth}, ntrees={final_ntrees})")

    # Save holdout predictions for model comparison plots
    pd.DataFrame({
        "Date": hold_data["Date"].values,
        "Actual": hold_actuals,
        "Predicted": hold_preds,
    }).to_csv(f"{RESULTS_DIR}/holdout_preds_{target.replace('/', '-').replace(' ', '_')}.csv", index=False)

    # Summary of results per horizon
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
        "Depth":         final_depth,
        "nTrees":        final_ntrees,
        "n_train":       len(cv_data),
        "n_holdout":     len(hold_data),
    })

    # Feature importance
    try:
        fnames, fi, fnames_sorted, fi_sorted, sortedindx = jl.HTBrelevance(
            final_output, final_data, verbose=False)
        importance = pd.Series(
            np.array(fi_sorted),
            index=[str(f) for f in fnames_sorted]
        ).sort_values(ascending=False)

        print(f"  Top 10 features:")
        for feat, score in importance.head(10).items():
            print(f"    {score:6.2f}  {feat}")
    except Exception as e:
        print(f"  Feature importance failed: {e}")
        importance = pd.Series(dtype=float)

    # Plot
    n_axes = 3 if len(cv_preds) > 0 else 2
    fig, axes = plt.subplots(n_axes, 1, figsize=(14, 5 * n_axes), facecolor="white")
    fig.suptitle(f"HTBoost — {target}{'[NOWCAST]' if is_nowcast else ''}",
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

    if len(importance) > 0:
        imp_vals = importance.head(20).sort_values()
        axes[ax_idx].barh(imp_vals.index, imp_vals.values, color=_BLUE, alpha=0.85)
        axes[ax_idx].set_title(f"Top 20 Feature Importances  (depth={final_depth})", fontsize=10)
        axes[ax_idx].set_xlabel("Importance", fontweight="bold")
    else:
        axes[ax_idx].text(0.5, 0.5, "Feature importance unavailable",
                          ha="center", va="center", transform=axes[ax_idx].transAxes)
    axes[ax_idx].spines["top"].set_visible(False)
    axes[ax_idx].spines["right"].set_visible(False)
    axes[ax_idx].grid(True, linestyle="--", alpha=0.1)

    plt.tight_layout()
    safe_name = target.replace("/", "-").replace(" ", "_")
    plt.savefig(f"{RESULTS_DIR}/{safe_name}.pdf", format="pdf", bbox_inches="tight")
    plt.close()

# Summary table
print("\n Summary of HTBoost model results:")
summary_df = pd.DataFrame(summary).set_index("Y")
print(summary_df.to_string())
summary_df.to_csv(f"{RESULTS_DIR}/htboost_summary.csv")

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
    "HTBoost — Results Summary\nHoldout: 2022–2026 | Purged CV | Depth auto-tuned",
    f"{RESULTS_DIR}/htboost_results.pdf",
    width=16,
)
