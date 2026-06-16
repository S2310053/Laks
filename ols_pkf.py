# This module runs the OLS forward benchmark under the purged k-fold method
#
##
# Additive companion to ols_model.py — the walk-forward + holdout OLS is left untouched.
# Here OLS (Y_h ~ α + β·FWD_h) is evaluated with the true purged k-fold over the full sample
# (from PKF_START), pooling out-of-sample predictions across the 4 folds. OLS is one of the
# two benchmarks, so its pooled OOS predictions are saved for the other models' DM-vs-OLS.
# Only the 4 horizons with a forward contract are run (1m, 3m, 6m, 12m).
##

import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")                       # PDF-only output; portable + headless-safe
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import statsmodels.api as sm
from metrics import Metrics
from plotter import Plotter
matplotlib.use("Agg", force=True)           # plotter.py forces TkAgg on import; re-assert Agg

from purged_kfold import (PKF_START, N_FOLDS, H_WEEKS, purge_weeks_for,
                          make_fold_specs, split_by_dates, pool_fold_results)

metrics = Metrics()
plt.rcParams["font.family"]      = "Times New Roman"
plt.rcParams["mathtext.fontset"] = "custom"
plt.rcParams["mathtext.rm"]      = "Times New Roman"

_BLUE, _DARK, _GREY = "#1A6B8A", "#0D3B5E", "dimgrey"

DF = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
RESULTS_DIR = "Results/OLS/PurgedKFold"
os.makedirs(RESULTS_DIR, exist_ok=True)

# OLS only where a forward contract exists
HORIZONS = {
    "Y 1m ∆ Salmon (NOK/KG)":  {"fwd": "FWD 1m",  "horizon": "1m"},
    "Y 3m ∆ Salmon (NOK/KG)":  {"fwd": "FWD 3m",  "horizon": "3m"},
    "Y 6m ∆ Salmon (NOK/KG)":  {"fwd": "FWD 6m",  "horizon": "6m"},
    "Y 12m ∆ Salmon (NOK/KG)": {"fwd": "FWD 12m", "horizon": "12m"},
}


def _safe(t):
    return t.replace("/", "-").replace(" ", "_")


def _style_ax(ax, ylabel=""):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, linestyle="--", alpha=0.1)
    if ylabel:
        ax.set_ylabel(ylabel, fontweight="bold")
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))


def _pooled_plot(target, horizon, pooled, specs, pl_rmse, pl_rw_rmse, dm_p):
    """Pooled OOS predicted-vs-actual + cumulative squared error (wrapped by caller)."""
    dates = pooled["Date"].values
    a     = pooled["Actual"].values
    p     = pooled["Predicted"].values

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), facecolor="white")
    fig.suptitle(f"OLS — Purged k-fold (pooled OOS)  —  {target}", fontsize=13, fontweight="bold")

    ax = axes[0]
    ax.plot(dates, a, label="Actual", color=_DARK, lw=1.4, alpha=0.9)
    ax.plot(dates, p, label="OLS",    color=_BLUE, lw=1.1, alpha=0.85)
    ax.axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
    for s in specs[1:]:                      # fold boundaries
        ax.axvline(s["test_start"], color=_GREY, lw=0.6, ls=":", alpha=0.6)
    ax.set_title(f"Pooled OOS — RMSE={pl_rmse:.4f} | RW={pl_rw_rmse:.4f} | DM p(vs RW)={dm_p:.3f}",
                 fontsize=10)
    ax.legend(frameon=False, fontsize=9)
    _style_ax(ax, ylabel="∆ Price (NOK/kg)")

    ax = axes[1]
    sse_m = np.cumsum((a - p) ** 2)
    sse_rw = np.cumsum(a ** 2)
    ax.plot(dates, sse_m,  label="OLS",         color=_BLUE, lw=1.2)
    ax.plot(dates, sse_rw, label="Random Walk", color=_GREY, lw=1.2, ls="--")
    ax.set_title("Cumulative Squared Error  (lower = better)", fontsize=10)
    ax.legend(frameon=False, fontsize=9)
    _style_ax(ax, ylabel="Cumulative SE")

    plt.tight_layout()
    plt.savefig(f"{RESULTS_DIR}/pooled_{_safe(target)}.pdf", format="pdf", bbox_inches="tight")
    plt.close()


print("OLS — Purged k-fold (pooled over full sample)")
perfold_all, summary = [], []

for target, cfg in HORIZONS.items():
    fwd, horizon = cfg["fwd"], cfg["horizon"]
    if fwd not in DF.columns:
        print(f"[SKIP] {target} — {fwd} missing")
        continue
    h = H_WEEKS[horizon]
    left, right = purge_weeks_for(horizon)

    # Shared per-horizon fold grid (target valid, from the first forward week)
    master = DF[(DF["Date"] >= PKF_START) & DF[target].notna()]["Date"]
    specs  = make_fold_specs(master, N_FOLDS, left, right)

    # OLS rows need a valid target AND forward
    data = DF[DF["Date"] >= PKF_START].dropna(subset=[target, fwd]).reset_index(drop=True)

    print(f"\n{target}  (h={h}w, purge L/R = {left}/{right}w, folds={N_FOLDS})")

    fold_results, perfold_rows = [], []
    for spec in specs:
        tr_m, te_m = split_by_dates(data["Date"], spec)
        train, test = data[tr_m], data[te_m]
        if len(train) < 50 or len(test) == 0:
            print(f"  fold {spec['fold']}: insufficient data (n_train={len(train)}, n_test={len(test)})")
            perfold_rows.append({"Horizon": horizon, "Fold": spec["fold"],
                                 "Test Start": spec["test_start"].date(), "Test End": spec["test_end"].date(),
                                 "n_train": len(train), "n_test": len(test),
                                 "RMSE": None, "MAE": None, "R2": None, "Hit": None,
                                 "RW RMSE": None, "Skill%": None})
            continue

        ols   = sm.OLS(train[target].values, sm.add_constant(train[fwd].values)).fit(
                    cov_type="HAC", cov_kwds={"maxlags": max(h, 1)})
        preds = ols.predict(sm.add_constant(test[fwd].values, has_constant="add"))
        a     = test[target].values

        fold_results.append({"fold": spec["fold"], "dates": test["Date"].values,
                             "actuals": a, "preds": preds})
        rmse, rw = metrics.rmse(a, preds), metrics.rw_rmse(a)
        perfold_rows.append({
            "Horizon": horizon, "Fold": spec["fold"],
            "Test Start": spec["test_start"].date(), "Test End": spec["test_end"].date(),
            "n_train": len(train), "n_test": len(test),
            "RMSE": rmse, "MAE": metrics.mae(a, preds),
            "R2": metrics.r2(a, preds), "Hit": metrics.hit_rate(a, preds),
            "RW RMSE": rw, "Skill%": metrics.skill_score(rmse, rw),
        })
        print(f"  fold {spec['fold']}: n_train={len(train):4d} n_test={len(test):3d}  "
              f"RMSE={rmse:.4f}  R²={metrics.r2(a, preds):+.3f}")

    pd.DataFrame(perfold_rows).to_csv(f"{RESULTS_DIR}/perfold_metrics_{_safe(target)}.csv", index=False)
    perfold_all.extend(perfold_rows)

    # Pooled OOS (every test obs once, date-sorted)
    pooled = pool_fold_results(fold_results)
    pooled.to_csv(f"{RESULTS_DIR}/pooled_preds_{_safe(target)}.csv", index=False)

    a, p     = pooled["Actual"].values, pooled["Predicted"].values
    pl_rmse  = metrics.rmse(a, p)
    pl_rwrm  = metrics.rw_rmse(a)
    pl_mae   = metrics.mae(a, p)
    pl_rwmae = metrics.rw_mae(a)
    dm_stat, dm_p = metrics.diebold_mariano(a, p, horizon=max(h, 1))
    n_indep = len(a) / max(h, 1)

    summary.append({
        "Y": target, "Horizon": horizon, "FWD": fwd,
        "Pooled RMSE": pl_rmse, "Pooled MAE": pl_mae,
        "Pooled R2": metrics.r2(a, p), "Pooled Hit": metrics.hit_rate(a, p),
        "RW RMSE": pl_rwrm, "RW MAE": pl_rwmae,
        "Skill%": metrics.skill_score(pl_rmse, pl_rwrm),
        "MAE Skill%": metrics.skill_score(pl_mae, pl_rwmae),
        "DM stat (RW)": dm_stat, "DM p (RW)": dm_p,
        "n_obs": len(a), "n_indep": round(n_indep, 1),
    })
    print(f"  POOLED RMSE={pl_rmse:.4f}  MAE={pl_mae:.4f}  R²={metrics.r2(a, p):+.3f}  "
          f"DM p(RW)={dm_p:.3f}  (n={len(a)}, ~{n_indep:.0f} indep)")

    try:
        _pooled_plot(target, horizon, pooled, specs, pl_rmse, pl_rwrm, dm_p)
    except Exception as e:
        print(f"  [warn] pooled plot failed: {type(e).__name__}: {e}")

# Save combined tables
pd.DataFrame(perfold_all).to_csv(f"{RESULTS_DIR}/perfold_metrics_all.csv", index=False)
summary_df = pd.DataFrame(summary)
summary_df.to_csv(f"{RESULTS_DIR}/pooled_summary.csv", index=False)
print("\nPooled summary:")
print(summary_df.to_string(index=False))

try:
    disp = pd.DataFrame({
        "Horizon":   [r["Horizon"] for r in summary],
        "RMSE":      [metrics.fmt(r["Pooled RMSE"]) for r in summary],
        "MAE":       [metrics.fmt(r["Pooled MAE"]) for r in summary],
        "R²":        [metrics.fmt(r["Pooled R2"], ".3f") for r in summary],
        "Hit":       [f'{r["Pooled Hit"]:.1%}' for r in summary],
        "RW RMSE":   [metrics.fmt(r["RW RMSE"]) for r in summary],
        "Skill%":    [f'{r["Skill%"]:+.1f}%' for r in summary],
        "DM p(RW)":  [f'{r["DM p (RW)"]:.4f}' if r["DM p (RW)"] >= 0.001 else "< 0.001" for r in summary],
        "n / indep": [f'{r["n_obs"]} / {r["n_indep"]:.0f}' for r in summary],
    })
    Plotter().results_table(
        disp,
        "OLS — Purged k-fold (pooled OOS, full sample)\n"
        "Y_h ~ α + β·FWD_h | 4 folds | DM = Harvey et al. (1997) vs RW",
        f"{RESULTS_DIR}/pooled_results.pdf", width=20)
except Exception as e:
    print(f"[warn] results table failed: {type(e).__name__}: {e}")

print(f"\nAll OLS purged-k-fold outputs → {RESULTS_DIR}/")
