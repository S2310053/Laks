# This module runs the LASSO model under the purged k-fold method
#
##
# Additive companion to lasso_model.py — the walk-forward + holdout run is left untouched.
# Lasso is evaluated with the true purged k-fold over the full sample (from PKF_START),
# pooling OOS predictions across the 4 folds, for all 7 horizons.
#
# NaN policy (differs from the walk-forward, by request):
#   * TRAIN: drop columns that are entirely NaN in the fold, impute the rest with the
#     training-fold column means, standardise. (Keeps every training row — complete-case
#     would erase 2006-2014 entirely, since with 110 features almost every early row has a
#     NaN.) Set IMPUTE_TRAIN=False for strict complete-case instead.
#   * TEST: drop any test row with a NaN in a used feature — Lasso is scored only on real
#     (non-imputed) inputs. Early NA-heavy rows naturally fall out, which is expected and
#     means Lasso's pooled coverage starts later than the boosters'.
# The regularisation strength alpha is REUSED from the walk-forward selection
# (lasso_summary.csv) rather than re-tuned per fold.
# DM vs RW at every horizon; DM vs OLS at 1m/3m/6m/12m on common dates.
##

import os
import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.linear_model import Lasso
from sklearn.preprocessing import StandardScaler
from metrics import Metrics
from plotter import Plotter
matplotlib.use("Agg", force=True)

from purged_kfold import (PKF_START, N_FOLDS, H_WEEKS, purge_weeks_for,
                          make_fold_specs, split_by_dates, pool_fold_results)

metrics = Metrics()
plt.rcParams["font.family"] = "Times New Roman"
_BLUE, _DARK, _GREY = "#1A6B8A", "#0D3B5E", "dimgrey"

IMPUTE_TRAIN = True          # False → strict complete-case (drops 2006-2014 from Lasso)
MAX_ITER     = 10_000

DF = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
Y_COLS   = [c for c in DF.columns if c.startswith("Y ")]
NON_FEAT = {"Date"} | set(Y_COLS)
ALL_FEAT = [c for c in DF.columns if c not in NON_FEAT]

OLS_PKF         = "Results/OLS/PurgedKFold"
RESULTS_DIR     = "Results/Lasso/PurgedKFold"
DM_OLS_HORIZONS = {"1m", "3m", "6m", "12m"}
os.makedirs(RESULTS_DIR, exist_ok=True)

# Reuse alpha from the walk-forward selection
lasso_sum = pd.read_csv("Results/Lasso/lasso_summary.csv")
ALPHA_BY  = {str(r["Horizon"]): float(r["Best Alpha"]) for _, r in lasso_sum.iterrows()}


def parse_horizon(target):
    m = re.search(r"Y (\d+(?:w|m)) ", target)
    return m.group(1) if m else "0w"

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


def _fit_predict(train, test, target, alpha):
    """Returns (test_dates_kept, test_actuals_kept, preds, n_dropped). Test rows with any NaN
    in a used feature are dropped (not imputed)."""
    Xtr = train[ALL_FEAT].to_numpy(dtype=float)
    ytr = train[target].to_numpy(dtype=float)

    valid = ~np.all(np.isnan(Xtr), axis=0)        # drop columns all-NaN in this fold
    Xtr   = Xtr[:, valid]
    if IMPUTE_TRAIN:
        col_means = np.nanmean(np.where(np.isfinite(Xtr), Xtr, np.nan), axis=0)
        col_means = np.where(np.isfinite(col_means), col_means, 0.0)
        Xtr = np.where(np.isnan(Xtr), col_means, Xtr)
    else:
        row_ok = ~np.isnan(Xtr).any(axis=1)
        Xtr, ytr = Xtr[row_ok], ytr[row_ok]
    Xtr = np.nan_to_num(Xtr, nan=0.0)             # safety net (fold-empty columns)

    scaler = StandardScaler().fit(Xtr)
    model  = Lasso(alpha=alpha, max_iter=MAX_ITER).fit(scaler.transform(Xtr), ytr)

    Xte_full  = test[ALL_FEAT].to_numpy(dtype=float)[:, valid]
    keep_rows = ~np.isnan(Xte_full).any(axis=1)   # test: drop NaN rows, never impute
    n_dropped = int((~keep_rows).sum())
    if keep_rows.sum() == 0:
        return np.array([]), np.array([]), np.array([]), n_dropped
    preds   = model.predict(scaler.transform(Xte_full[keep_rows]))
    test_k  = test[keep_rows]
    return test_k["Date"].values, test_k[target].to_numpy(dtype=float), preds, n_dropped


def _dm_vs_ols(pooled, target, h):
    ols_csv = f"{OLS_PKF}/pooled_preds_{_safe(target)}.csv"
    if not os.path.exists(ols_csv):
        return None, None
    ols = pd.read_csv(ols_csv, parse_dates=["Date"]).rename(columns={"Predicted": "OLS"})
    m   = pooled.merge(ols[["Date", "OLS"]], on="Date", how="inner")
    if len(m) < 10:
        return None, None
    return metrics.diebold_mariano(m["Actual"].values, m["Predicted"].values,
                                   horizon=max(h, 1), benchmark_pred=m["OLS"].values)


print(f"Lasso — purged k-fold  (IMPUTE_TRAIN={IMPUTE_TRAIN})")
perfold_all, summary = [], []

for target in Y_COLS:
    horizon = parse_horizon(target)
    if horizon not in ALPHA_BY:
        print(f"[SKIP] {target} — no walk-forward alpha on record")
        continue
    alpha = ALPHA_BY[horizon]
    h     = H_WEEKS[horizon]
    left, right = purge_weeks_for(horizon)

    master = DF[(DF["Date"] >= PKF_START) & DF[target].notna()]["Date"]
    specs  = make_fold_specs(master, N_FOLDS, left, right)
    data   = DF[DF["Date"] >= PKF_START].dropna(subset=[target]).reset_index(drop=True)

    print(f"\n{target}  (h={h}w, purge {left}/{right}w, alpha={alpha:.5f})")
    fold_results, perfold_rows = [], []
    for spec in specs:
        tr_m, te_m = split_by_dates(data["Date"], spec)
        train, test = data[tr_m], data[te_m]
        if len(train) < 50 or len(test) == 0:
            print(f"  fold {spec['fold']}: insufficient data")
            continue
        td, ta, tp, n_drop = _fit_predict(train, test, target, alpha)
        n_kept = len(td)
        if n_kept == 0:
            print(f"  fold {spec['fold']}: all {len(test)} test rows dropped (NaN) — no preds")
            perfold_rows.append({"Horizon": horizon, "Fold": spec["fold"],
                                 "Test Start": spec["test_start"].date(), "Test End": spec["test_end"].date(),
                                 "n_train": len(train), "n_test": 0, "n_dropped": n_drop,
                                 "RMSE": None, "MAE": None, "R2": None, "Hit": None,
                                 "RW RMSE": None, "Skill%": None})
            continue
        fold_results.append({"fold": spec["fold"], "dates": td, "actuals": ta, "preds": tp})
        rmse, rw = metrics.rmse(ta, tp), metrics.rw_rmse(ta)
        perfold_rows.append({
            "Horizon": horizon, "Fold": spec["fold"],
            "Test Start": spec["test_start"].date(), "Test End": spec["test_end"].date(),
            "n_train": len(train), "n_test": n_kept, "n_dropped": n_drop,
            "RMSE": rmse, "MAE": metrics.mae(ta, tp),
            "R2": metrics.r2(ta, tp), "Hit": metrics.hit_rate(ta, tp),
            "RW RMSE": rw, "Skill%": metrics.skill_score(rmse, rw),
        })
        print(f"  fold {spec['fold']}: n_train={len(train):4d} n_test={n_kept:3d} "
              f"(dropped {n_drop:3d})  RMSE={rmse:.4f}  R²={metrics.r2(ta, tp):+.3f}")

    pd.DataFrame(perfold_rows).to_csv(f"{RESULTS_DIR}/perfold_metrics_{_safe(target)}.csv", index=False)
    perfold_all.extend(perfold_rows)

    if not fold_results:
        print("  no usable folds — skipping pooled")
        continue
    pooled = pool_fold_results(fold_results)
    pooled.to_csv(f"{RESULTS_DIR}/pooled_preds_{_safe(target)}.csv", index=False)
    a, p = pooled["Actual"].values, pooled["Predicted"].values
    pl_rmse, pl_rwrm = metrics.rmse(a, p), metrics.rw_rmse(a)
    pl_mae,  pl_rwmae = metrics.mae(a, p), metrics.rw_mae(a)
    dm_stat, dm_p = metrics.diebold_mariano(a, p, horizon=max(h, 1))
    dm_ols_stat, dm_ols_p = (_dm_vs_ols(pooled, target, h) if horizon in DM_OLS_HORIZONS else (None, None))

    summary.append({
        "Y": target, "Horizon": horizon, "Alpha": alpha,
        "Pooled RMSE": pl_rmse, "Pooled MAE": pl_mae,
        "Pooled R2": metrics.r2(a, p), "Pooled Hit": metrics.hit_rate(a, p),
        "RW RMSE": pl_rwrm, "RW MAE": pl_rwmae,
        "Skill%": metrics.skill_score(pl_rmse, pl_rwrm),
        "MAE Skill%": metrics.skill_score(pl_mae, pl_rwmae),
        "DM stat (RW)": dm_stat, "DM p (RW)": dm_p,
        "DM stat (OLS)": dm_ols_stat, "DM p (OLS)": dm_ols_p,
        "n_obs": len(a), "n_indep": round(len(a) / max(h, 1), 1),
    })
    print(f"  POOLED RMSE={pl_rmse:.4f}  MAE={pl_mae:.4f}  R²={metrics.r2(a, p):+.3f}  "
          f"DM p(RW)={dm_p:.3f}" + (f"  DM p(OLS)={dm_ols_p:.3f}" if dm_ols_p is not None else "")
          + f"  (n={len(a)})")

    try:
        dates = pooled["Date"].values
        fig, axes = plt.subplots(2, 1, figsize=(14, 9), facecolor="white")
        fig.suptitle(f"Lasso — Purged k-fold (pooled OOS)  —  {target}", fontsize=13, fontweight="bold")
        ax = axes[0]
        ax.plot(dates, a, label="Actual", color=_DARK, lw=1.4, alpha=0.9)
        ax.plot(dates, p, label="Lasso", color=_BLUE, lw=1.1, alpha=0.85)
        ax.axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
        for s in specs[1:]:
            ax.axvline(s["test_start"], color=_GREY, lw=0.6, ls=":", alpha=0.6)
        ax.set_title(f"Pooled OOS — RMSE={pl_rmse:.4f} | RW={pl_rwrm:.4f} | DM p(RW)={dm_p:.3f}", fontsize=10)
        ax.legend(frameon=False, fontsize=9)
        _style_ax(ax, ylabel="∆ Price (NOK/kg)")
        ax = axes[1]
        ax.plot(dates, np.cumsum((a - p) ** 2), label="Lasso", color=_BLUE, lw=1.2)
        ax.plot(dates, np.cumsum(a ** 2), label="Random Walk", color=_GREY, lw=1.2, ls="--")
        ax.set_title("Cumulative Squared Error  (lower = better)", fontsize=10)
        ax.legend(frameon=False, fontsize=9)
        _style_ax(ax, ylabel="Cumulative SE")
        plt.tight_layout()
        plt.savefig(f"{RESULTS_DIR}/pooled_{_safe(target)}.pdf", format="pdf", bbox_inches="tight")
        plt.close()
    except Exception as e:
        print(f"  [warn] pooled plot failed: {type(e).__name__}: {e}")

pd.DataFrame(perfold_all).to_csv(f"{RESULTS_DIR}/perfold_metrics_all.csv", index=False)
summary_df = pd.DataFrame(summary)
summary_df.to_csv(f"{RESULTS_DIR}/pooled_summary.csv", index=False)
print("\nLasso pooled summary:")
print(summary_df[["Horizon", "Pooled RMSE", "Pooled MAE", "Pooled R2", "Skill%",
                  "DM p (RW)", "DM p (OLS)", "n_obs", "n_indep"]].to_string(index=False))

try:
    def _fp(x):
        return "—" if x is None or (isinstance(x, float) and np.isnan(x)) else (f"{x:.4f}" if x >= 0.001 else "< 0.001")
    disp = pd.DataFrame({
        "Horizon":   [r["Horizon"] for r in summary],
        "RMSE":      [metrics.fmt(r["Pooled RMSE"]) for r in summary],
        "MAE":       [metrics.fmt(r["Pooled MAE"]) for r in summary],
        "R²":        [metrics.fmt(r["Pooled R2"], ".3f") for r in summary],
        "Skill%":    [f'{r["Skill%"]:+.1f}%' for r in summary],
        "MAE Skill%":[f'{r["MAE Skill%"]:+.1f}%' for r in summary],
        "p(DM RW)":  [_fp(r["DM p (RW)"]) for r in summary],
        "p(DM OLS)": [_fp(r["DM p (OLS)"]) for r in summary],
        "n/indep":   [f'{r["n_obs"]}/{r["n_indep"]:.0f}' for r in summary],
    })
    Plotter().results_table(
        disp,
        "Lasso — Purged k-fold (pooled OOS, full sample)\n"
        "4 folds | alpha reused from walk-forward | train imputed, test NaN rows dropped | "
        "DM (HLN+NW) vs RW and vs OLS",
        f"{RESULTS_DIR}/pooled_results.pdf", width=24)
except Exception as e:
    print(f"[warn] results table failed: {type(e).__name__}: {e}")

print(f"\nLasso purged-k-fold done → {RESULTS_DIR}/")
