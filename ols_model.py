# This module constructs the OLS model

##
# We construct the OLS model using only the forward price as a predictor for the corresponding horizon's return,
# to serve as benchmark for evaluating performance of the other models. Forward prices are the market's consensus forecasts,
# so it gives a natural baseline of the market expectations, suitable for comparison
# For each horizon h, fits:  Y_h = α + β · FWD_h + ε
# Tests H0: β=1 (Expectations Hypothesis — forward is unbiased predictor)
# Only horizons with a matching forward contract are run (1m, 3m, 6m, 12m)
##

import pandas as pd
import numpy as np
import os
from scipy import stats
import statsmodels.api as sm
from metrics import Metrics
metrics = Metrics()
from plotter import Plotter
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

# Create folder to store results
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])

HOLDOUT_START = "2022-01-01"

RESULTS_DIR = "Results/OLS"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Each horizon is matched to its corresponding forward basis ln(F_h / S)
# Note: The purged K-fold CV is included in this code, so we can get a comparison for all the CV splits with the CatBoost results
HORIZONS = {
    "Y 1m ∆ Salmon (NOK/KG)":  {"purge_weeks":  4, "n_folds": 10, "fwd": "FWD 1m",  "horizon": "1m"},
    "Y 3m ∆ Salmon (NOK/KG)":  {"purge_weeks": 13, "n_folds":  8, "fwd": "FWD 3m",  "horizon": "3m"},
    "Y 6m ∆ Salmon (NOK/KG)":  {"purge_weeks": 26, "n_folds":  6, "fwd": "FWD 6m",  "horizon": "6m"},
    "Y 12m ∆ Salmon (NOK/KG)": {"purge_weeks": 52, "n_folds":  3, "fwd": "FWD 12m", "horizon": "12m"},
}

# Purged k-fold CV
def purged_cv(data, target, fwd_feat, purge_weeks, n_folds):
    n         = len(data)
    fold_size = n // n_folds
    results   = []

    for i in range(1, n_folds):
        test_start_idx = i * fold_size
        test_end_idx   = (i + 1) * fold_size if i < n_folds - 1 else n
        train_end_idx  = test_start_idx - purge_weeks

        if train_end_idx < fold_size:
            continue

        train = data.iloc[:train_end_idx].dropna(subset=[target, fwd_feat])
        test  = data.iloc[test_start_idx:test_end_idx].dropna(subset=[target, fwd_feat])

        if len(train) < 50 or len(test) == 0:
            continue

        X_train = sm.add_constant(train[fwd_feat].values)
        ols     = sm.OLS(train[target].values, X_train).fit(
                      cov_type='HAC', cov_kwds={'maxlags': purge_weeks})
        X_test  = sm.add_constant(test[fwd_feat].values, has_constant="add")
        preds   = ols.predict(X_test)

        results.append({
            "dates":   test["Date"].values,
            "actuals": test[target].values,
            "preds":   preds,
            "fold":    i,
        })

    return results

# Run model for all horizons
summary = []

print("OLS Forward Benchmark")

for target, cfg in HORIZONS.items():
    fwd_feat  = cfg["fwd"]
    purge_wks = cfg["purge_weeks"]
    n_folds   = cfg["n_folds"]
    horizon   = cfg["horizon"]

    if fwd_feat not in df.columns:
        print(f"[SKIP] {target} — {fwd_feat} not in data")
        continue

    cv_data   = df[df["Date"] < HOLDOUT_START].dropna(subset=[target, fwd_feat]).copy().reset_index(drop=True)
    hold_data = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target, fwd_feat]).copy().reset_index(drop=True)

    # Embargo: final model training ends purge_wks before holdout so no training target overlaps holdout returns
    embargo_date = pd.Timestamp(HOLDOUT_START) - pd.Timedelta(weeks=purge_wks)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    print(f"\n{target}  (Y_{horizon} ~ FWD_{horizon}, purge={purge_wks}w, folds={n_folds})")

    # Performance evaluation on Purged CV splits
    fold_results = purged_cv(cv_data, target, fwd_feat, purge_wks, n_folds)

    if fold_results:
        cv_preds   = np.concatenate([f["preds"]   for f in fold_results])
        cv_actuals = np.concatenate([f["actuals"] for f in fold_results])
        cv_dates   = np.concatenate([f["dates"]   for f in fold_results])

        cv_rmse    = metrics.rmse(cv_actuals, cv_preds)
        cv_rw_rmse = metrics.rw_rmse(cv_actuals)
        cv_r2      = metrics.r2(cv_actuals, cv_preds)
        cv_hitrate = metrics.hit_rate(cv_actuals, cv_preds)
        print(f"CV RMSE={cv_rmse:.4f}  RW_RMSE={cv_rw_rmse:.4f}  "
              f"R²={cv_r2:.4f}  Hit={cv_hitrate:.1%}  (n={len(cv_actuals)})")
    else:
        cv_rmse = cv_rw_rmse = cv_r2 = cv_hitrate = None
        cv_preds = cv_actuals = cv_dates = []
        print("CV: insufficient data")

    # Final OLS on embargoed training set (no target overlap with holdout)
    train_final = cv_data[cv_data["Date"] < embargo_date].copy()
    X_cv = sm.add_constant(train_final[fwd_feat].values)
    ols  = sm.OLS(train_final[target].values, X_cv).fit(
               cov_type='HAC', cov_kwds={'maxlags': purge_wks})

    alpha_hat = ols.params[0]
    beta_hat  = ols.params[1]
    beta_se   = ols.bse[1]

    # β=1 t-test (Expectations Hypothesis - Is the forward price an unbiased predictor of the future spot price?)
    t_beta1 = (beta_hat - 1) / beta_se
    p_beta1 = 2 * stats.t.sf(np.abs(t_beta1), df=ols.df_resid)

    print(f"OLS α={alpha_hat:.4f}  β={beta_hat:.4f}  SE(β)={beta_se:.4f}  "
          f"t(β=1)={t_beta1:.2f}  p(β=1)={p_beta1:.3f}")

    # Holdout period performance
    X_hold       = sm.add_constant(hold_data[fwd_feat].values, has_constant="add")
    hold_preds   = ols.predict(X_hold)
    hold_actuals = hold_data[target].values
    hold_rmse    = metrics.rmse(hold_actuals, hold_preds)
    hold_rw_rmse = metrics.rw_rmse(hold_actuals)
    hold_r2      = metrics.r2(hold_actuals, hold_preds)
    hold_hitrate = metrics.hit_rate(hold_actuals, hold_preds)
    dm_stat, dm_p = metrics.diebold_mariano(hold_actuals, hold_preds, horizon=max(purge_wks, 1))

    print(f"Holdout RMSE={hold_rmse:.4f} RW_RMSE={hold_rw_rmse:.4f}"
          f"R²={hold_r2:.4f}  Hit={hold_hitrate:.1%}  DM={dm_stat:.2f}  p={dm_p:.3f}")

    summary.append({
        "Y":             target,
        "Horizon":       horizon,
        "FWD":           fwd_feat,
        "α":             alpha_hat,
        "β":             beta_hat,
        "SE(β)":         beta_se,
        "p(β=1)":        p_beta1,
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
        "n_train":       len(cv_data),
        "n_holdout":     len(hold_data),
    })

    # Plot
    n_axes = 2 if len(cv_preds) > 0 else 1
    fig, axes = plt.subplots(n_axes, 1, figsize=(14, 5 * n_axes), facecolor="white")
    if n_axes == 1:
        axes = [axes]
    fig.suptitle(f"OLS  —  {target}  ~  {fwd_feat}   (β={beta_hat:.3f},  p(β=1)={p_beta1:.3f})",
                 fontsize=13, fontweight="bold")

    ax_idx = 0
    if len(cv_preds) > 0:
        axes[ax_idx].plot(cv_dates, cv_actuals, label="Actual", color=_DARK, lw=1.5, alpha=0.85)
        axes[ax_idx].plot(cv_dates, cv_preds,   label="OLS",    color=_BLUE, lw=1.2, alpha=0.85)
        axes[ax_idx].axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
        axes[ax_idx].set_title(f"Purged CV  —  RMSE={cv_rmse:.4f}  |  RW={cv_rw_rmse:.4f}  |  R²={cv_r2:.4f}",
                               fontsize=10)
        axes[ax_idx].legend(frameon=False, fontsize=9)
        _style_ax(axes[ax_idx], ylabel="∆ Price (NOK/kg)")
        ax_idx += 1

    axes[ax_idx].plot(hold_data["Date"].values, hold_actuals, label="Actual",
                      color=_DARK, lw=1.5, alpha=0.85)
    axes[ax_idx].plot(hold_data["Date"].values, hold_preds,   label="OLS",
                      color=_BLUE, lw=1.2, alpha=0.85)
    axes[ax_idx].axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
    axes[ax_idx].set_title(f"Holdout 2022–2025 — RMSE={hold_rmse:.4f} | RW={hold_rw_rmse:.4f}  |  "
                           f"R²={hold_r2:.4f} | DM p={dm_p:.3f}", fontsize=10)
    axes[ax_idx].legend(frameon=False, fontsize=9)
    _style_ax(axes[ax_idx], ylabel="∆ Price (NOK/kg)")

    plt.tight_layout()
    safe_name = target.replace("/", "-").replace(" ", "_").replace("∆", "d")
    plt.savefig(f"{RESULTS_DIR}/ols_{safe_name}.pdf", format="pdf", bbox_inches="tight")
    plt.close()

# Summary table
print("\n Summary of OLS Forward Benchmark Results")
summary_df = pd.DataFrame(summary).set_index("Y")
print(summary_df.to_string())
summary_df.to_csv(f"{RESULTS_DIR}/ols_summary.csv")

# PDF results table
disp = pd.DataFrame({
    "Horizon":   [r["Horizon"] for r in summary],
    "FWD":       [r["FWD"] for r in summary],
    "β":         [f'{r["β"]:.3f}' for r in summary],
    "p(β=1)":    [f'{r["p(β=1)"]:.3f}' if r["p(β=1)"] >= 0.001 else "< 0.001" for r in summary],
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
})

Plotter().results_table(
    disp,
    "OLS Forward Benchmark — Results Summary\n"
    "Holdout: 2022–2025 | Purged CV | Y_h ~ α + β·FWD_h  (EH: β=1)",
    f"{RESULTS_DIR}/ols_results.pdf",
    width=18,
)
