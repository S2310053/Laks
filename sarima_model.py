# This module constructs the sARIMA model

##
# We construct the sARIMA model for all y horizons (using same setup as with OLS to get results for CV splits as well)
# Parameters are selected by BIC. Reason for choosing sARIMA, is that salmon prices show strong seasonality,
# and sARIMA is a natural choice for univariate time series with seasonality
# Note: We ran into an issue with estimation in holdout period, where estimates went to zero and became constant
# We added the Kalman filter, to re-run on observed history, to condition predictions on real data without re-estimating parameters
##

import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
import re, os, time
from pmdarima import auto_arima
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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
from metrics import Metrics
metrics = Metrics()
from plotter import Plotter

# Configuration
HOLDOUT_START  = "2022-01-01"
WEEKLY_RET_COL = "Y 0w ∆ Salmon (NOK/KG)"   # r_t = ln(S_t / S_{t-1})

# Create folder to store results
RESULTS_DIR = "Results/SARIMA"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Load data
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])

# Identify Y columns
Y_COLS = [c for c in df.columns if c.startswith("Y ")]

# Same structure as OLS to get results comparison with CatBoost for CV splits, as well as final holdout period
# steps = number of consecutive weekly log-returns summed in the target
HORIZON_CONFIG = {
    "0w":  {"purge_weeks":  0, "n_folds": 10, "steps":  1},
    "1w":  {"purge_weeks":  1, "n_folds": 10, "steps":  2},
    "2w":  {"purge_weeks":  2, "n_folds": 10, "steps":  3},
    "1m":  {"purge_weeks":  4, "n_folds": 10, "steps":  5},
    "3m":  {"purge_weeks": 13, "n_folds":  8, "steps": 14},
    "6m":  {"purge_weeks": 26, "n_folds":  6, "steps": 27},
    "12m": {"purge_weeks": 52, "n_folds":  4, "steps": 53},
}

# SARIMA settings 
# m=52: weekly data, annual seasonal cycle
# d=0, D=0: log-returns are already stationary
# Note: We use BIC as target. This and other settings can be adjusted if seen fit
# NB! Using max_p/q leads to long estimations times for some folds — adjust if needed
def _fit_sarima(series):
    return auto_arima(
        series,
        seasonal              = True,
        m                     = 52,
        d                     = 0,
        D                     = 0,
        max_p                 = 5,
        max_q                 = 5,
        max_P                 = 2,
        max_Q                 = 2,
        stepwise              = True,
        information_criterion = "bic",
        error_action          = "ignore",
        suppress_warnings     = True,
    )

# Helper to extract horizon from Y column name
def parse_horizon(target):
    m = re.search(r"Y (\d+(?:w|m)) ", target)
    return m.group(1) if m else "0w"

# Purged k-fold CV 
def cv_sarima(df_all, cv_data, target, weekly_ret_col, purge_weeks, n_folds, steps):
    n         = len(cv_data)
    fold_size = n // n_folds
    results   = []

    ret_series = df_all.dropna(subset=[weekly_ret_col])[["Date", weekly_ret_col]].reset_index(drop=True)

    for i in range(1, n_folds):
        test_start = i * fold_size
        test_end   = (i + 1) * fold_size if i < n_folds - 1 else n
        train_end  = test_start - purge_weeks
        if train_end < fold_size:
            continue

        test = cv_data.iloc[test_start:test_end].dropna(subset=[target])
        if len(test) == 0:
            continue

        train_cutoff_date = cv_data.iloc[train_end - 1]["Date"]
        train_mask       = ret_series["Date"] <= train_cutoff_date
        weekly_ret_train = ret_series.loc[train_mask, weekly_ret_col].values
        if len(weekly_ret_train) < max(52, 2 * steps):
            continue

        try:
            t0     = time.time()
            model  = _fit_sarima(weekly_ret_train)
            order  = f"SARIMA{model.order}x{model.seasonal_order}"
            sm_res = model.arima_res_

            preds = np.zeros(len(test))
            for j, t_date in enumerate(test["Date"].values):
                mask_t  = ret_series["Date"] < t_date
                history = ret_series.loc[mask_t, weekly_ret_col].values
                applied = sm_res.apply(history, refit=False)
                fc      = applied.forecast(steps=steps)
                preds[j] = float(np.sum(fc))

            print(f"fold {i}/{n_folds-1}  {order}  ({time.time()-t0:.0f}s)")

        except Exception as e:
            print(f"fold {i} FAILED: {e}")
            continue

        results.append({
            "dates":   test["Date"].values,
            "actuals": test[target].values,
            "preds":   preds,
            "fold":    i,
            "order":   order,
        })

    return results

# Run model for all horizons
summary = []

for target in Y_COLS:
    horizon    = parse_horizon(target)
    cfg        = HORIZON_CONFIG.get(horizon)
    if cfg is None:
        continue
    purge_wks  = cfg["purge_weeks"]
    n_folds    = cfg["n_folds"]
    steps      = cfg["steps"]
    is_nowcast = (horizon == "0w")

    # Embargo: end training purge_wks before holdout so no training target overlaps holdout returns
    embargo_date = pd.Timestamp(HOLDOUT_START) - pd.Timedelta(weeks=purge_wks)
    cv_data   = df[df["Date"] < embargo_date].dropna(subset=[target]).copy().reset_index(drop=True)
    hold_data = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    label = f"{target} [NOWCAST]" if is_nowcast else target
    print(f"\n{label} (horizon={horizon}, purge={purge_wks}w, folds={n_folds}, steps={steps})")

    # Performance evaluation on Purged CV splits
    print(f"Running CV (m=52, seasonal)...")
    t0 = time.time()
    fold_results = cv_sarima(df, cv_data, target, WEEKLY_RET_COL,
                              purge_wks, n_folds, steps)

    if fold_results:
        cv_preds   = np.concatenate([f["preds"]   for f in fold_results])
        cv_actuals = np.concatenate([f["actuals"] for f in fold_results])
        cv_dates   = np.concatenate([f["dates"]   for f in fold_results])

        cv_rmse    = metrics.rmse(cv_actuals, cv_preds)
        cv_rw_rmse = metrics.rw_rmse(cv_actuals)
        cv_r2      = metrics.r2(cv_actuals, cv_preds)
        cv_hitrate = metrics.hit_rate(cv_actuals, cv_preds)
        print(f"CV RMSE={cv_rmse:.4f} RW_RMSE={cv_rw_rmse:.4f}  "
              f"R²={cv_r2:.4f}  Hit={cv_hitrate:.1%}  "
              f"(n_obs={len(cv_actuals)}, folds={len(fold_results)}, {time.time()-t0:.0f}s)")
    else:
        cv_rmse = cv_rw_rmse = cv_r2 = cv_hitrate = None
        cv_preds = cv_actuals = cv_dates = []
        print("CV: insufficient data")

    # Final sARIMA on full training set
    print(f"Running holdout ...")
    t0 = time.time()

    weekly_ret_full = (
        df[df["Date"] < embargo_date][WEEKLY_RET_COL]
        .dropna()
        .reset_index(drop=True)
    )
    ret_series_full = (
        df.dropna(subset=[WEEKLY_RET_COL])[["Date", WEEKLY_RET_COL]]
        .reset_index(drop=True)
    )

    hold_preds = None
    hold_order = "N/A"
    try:
        sa_model = _fit_sarima(weekly_ret_full.values)
        hold_order = f"SARIMA{sa_model.order}x{sa_model.seasonal_order}"
        sm_res   = sa_model.arima_res_

        hold_preds = np.zeros(len(hold_data))
        for j, t_date in enumerate(hold_data["Date"].values):
            mask_t  = ret_series_full["Date"] < t_date
            history = ret_series_full.loc[mask_t, WEEKLY_RET_COL].values
            applied = sm_res.apply(history, refit=False)
            fc      = applied.forecast(steps=steps)
            hold_preds[j] = float(np.sum(fc))

        # Holdout period performance
        hold_actuals  = hold_data[target].values
        hold_rmse     = metrics.rmse(hold_actuals, hold_preds)
        hold_rw_rmse  = metrics.rw_rmse(hold_actuals)
        hold_r2       = metrics.r2(hold_actuals, hold_preds)
        hold_hitrate  = metrics.hit_rate(hold_actuals, hold_preds)
        dm_stat, dm_p = metrics.diebold_mariano(hold_actuals, hold_preds, horizon=max(purge_wks, 1))

        print(f"Holdout RMSE={hold_rmse:.4f} RW_RMSE={hold_rw_rmse:.4f}  "
              f"R²={hold_r2:.4f}  Hit={hold_hitrate:.1%}  "
              f"DM={dm_stat:.2f}  p={dm_p:.3f}  ({hold_order}, {time.time()-t0:.0f}s)")

    except Exception as e:
        hold_rmse = hold_rw_rmse = hold_r2 = hold_hitrate = None
        dm_stat = dm_p = None
        hold_actuals = hold_data[target].values
        print(f"Holdout FAILED: {e}")

    summary.append({
        "Y":             target,
        "Horizon":       horizon,
        "Nowcast":       is_nowcast,
        "CV RMSE":       cv_rmse,
        "CV RW RMSE":    cv_rw_rmse,
        "CV R2":         cv_r2,
        "CV Hit":        cv_hitrate,
        "Hold RMSE":     hold_rmse,
        "Hold RW RMSE":  hold_rw_rmse,
        "Hold R2":       hold_r2,
        "Hold Hit":      hold_hitrate,
        "Hold DM":       dm_stat,
        "Hold DM p":     dm_p,
        "Order":         hold_order,
        "n_train":       len(cv_data),
        "n_holdout":     len(hold_data),
    })

    # Plot
    n_axes = 2 if len(cv_preds) > 0 else 1
    fig, axes = plt.subplots(n_axes, 1, figsize=(14, 5 * n_axes), facecolor="white")
    if n_axes == 1:
        axes = [axes]
    fig.suptitle(f"SARIMA  —  {target}{' [NOWCAST]' if is_nowcast else ''}",
                 fontsize=13, fontweight="bold")

    ax_idx = 0
    if len(cv_preds) > 0:
        axes[ax_idx].plot(cv_dates, cv_actuals, label="Actual",       color=_DARK, lw=1.5, alpha=0.85)
        axes[ax_idx].plot(cv_dates, cv_preds,   label="SARIMA",       color=_BLUE, lw=1.2, alpha=0.85)
        axes[ax_idx].axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
        axes[ax_idx].set_title(f"Purged CV  —  RMSE={cv_rmse:.4f}  |  RW={cv_rw_rmse:.4f}  |  R²={cv_r2:.4f}",
                               fontsize=10)
        axes[ax_idx].legend(frameon=False, fontsize=9)
        _style_ax(axes[ax_idx], ylabel="∆ Price (NOK/kg)")
        ax_idx += 1

    axes[ax_idx].plot(hold_data["Date"].values, hold_actuals, label="Actual",
                      color=_DARK, lw=1.5, alpha=0.85)
    if hold_preds is not None:
        axes[ax_idx].plot(hold_data["Date"].values, hold_preds, label="SARIMA",
                          color=_BLUE, lw=1.2, alpha=0.85)
    axes[ax_idx].axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
    _title = f"Holdout 2022–2025  —  {hold_order}"
    if hold_r2 is not None:
        _title += f"  |  R²={hold_r2:.4f}"
    axes[ax_idx].set_title(_title, fontsize=10)
    axes[ax_idx].legend(frameon=False, fontsize=9)
    _style_ax(axes[ax_idx], ylabel="∆ Price (NOK/kg)")

    plt.tight_layout()
    safe_name = target.replace("/", "-").replace(" ", "_").replace("∆", "d")
    plt.savefig(f"{RESULTS_DIR}/sarima_{safe_name}.pdf", format="pdf", bbox_inches="tight")
    plt.close()

#Summary table
print("\n Summary of sARIMA results")
summary_df = pd.DataFrame(summary).set_index("Y")
print(summary_df.to_string())
summary_df.to_csv(f"{RESULTS_DIR}/sarima_summary.csv")

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
    "Skill %":   [f'{(1 - r["Hold RMSE"]/r["Hold RW RMSE"])*100:+.1f}%'
                  if r["Hold RMSE"] and r["Hold RW RMSE"] else "—" for r in summary],
    "DM":        [metrics.fmt(r["Hold DM"], ".2f") for r in summary],
    "p-value":   [f'{r["Hold DM p"]:.4f}' if r["Hold DM p"] is not None and r["Hold DM p"] >= 0.001
                  else ("< 0.001" if r["Hold DM p"] is not None else "—") for r in summary],
    "Order":     [r["Order"] for r in summary],
})

Plotter().results_table(
    disp,
    "SARIMA — Results Summary\nHoldout: 2022–2025 | Purged CV | BIC Selection (m=52)",
    f"{RESULTS_DIR}/sarima_results.pdf",
    width=18,
)
