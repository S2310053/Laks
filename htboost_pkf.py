# This module runs the HTBoost model under the purged k-fold method
#
##
# Additive companion to htboost_model.py — the walk-forward + holdout run is left untouched.
# HTBoost (Julia, via juliacall) is evaluated with the true purged k-fold over the full sample
# (from PKF_START), pooling OOS predictions across the 4 folds, for both loss variants
# (L2→RMSE, quantile@0.5→MAE) and all 7 horizons. HTBoost ingests NaN natively (drops columns
# >30% NaN in a fold) and auto-tunes its depth internally per fit (modality="compromise"), so —
# unlike CatBoost — nothing is reused; the selected depth/ntrees are recorded per fold.
# DM vs RW at every horizon; DM vs OLS at 1m/3m/6m/12m on common dates.
#
# Set PKF_SMOKE=1 to run a single horizon/loss/fold as a quick bridge check.
##

import os
import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

print("Loading Julia + HybridTreeBoosting...")
from juliacall import Main as jl
jl.seval("using HybridTreeBoosting")
jl.seval("using DataFrames")
jl.seval("import Logging; Logging.disable_logging(Logging.Warn)")
print("Julia loaded.")

from metrics import Metrics
from plotter import Plotter
matplotlib.use("Agg", force=True)

from purged_kfold import (PKF_START, N_FOLDS, H_WEEKS, purge_weeks_for,
                          make_fold_specs, split_by_dates, pool_fold_results)

metrics = Metrics()
plt.rcParams["font.family"] = "Times New Roman"
_BLUE, _DARK, _GREY = "#1A6B8A", "#0D3B5E", "dimgrey"

DF = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
Y_COLS   = [c for c in DF.columns if c.startswith("Y ")]
NON_FEAT = {"Date"} | set(Y_COLS)
ALL_FEAT = [c for c in DF.columns if c not in NON_FEAT]

OLS_PKF         = "Results/OLS/PurgedKFold"
DM_OLS_HORIZONS = {"1m", "3m", "6m", "12m"}
LOSS_FNS        = [("L2", "RMSE"), ("quantile", "MAE")]   # (HTBoost loss, output label)
HT_MODALITY     = "compromise"
HT_NTREES       = 2000

SMOKE = bool(os.environ.get("PKF_SMOKE"))
if SMOKE:
    Y_COLS   = [c for c in Y_COLS if " 1m " in c]
    LOSS_FNS = [("L2", "RMSE")]


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


def _fit_htboost(X_train, y_train, X_test, feature_names, ht_loss, purge_weeks=0, nan_thresh=0.3):
    """Identical fitting recipe to htboost_model.py (proven to work).
    nan_thresh defaults to 0.3 → behaviour is exactly the holdout/other-machine recipe.
    A fallback caller may pass nan_thresh>1 to keep ALL columns; this dodges a
    HybridTreeBoosting bug on the SharedArray/@distributed path (some Windows setups) where
    dropping columns desyncs the internal feature count and throws a BoundsError. HTBoost
    handles NaN natively, so keeping all columns is safe for fitting."""
    nan_frac   = np.mean(np.isnan(X_train), axis=0)
    valid_cols = nan_frac < nan_thresh
    if not valid_cols.any():                 # never hand HTBoost an empty matrix
        valid_cols = nan_frac < 1.0
    feat_v     = [f for f, v in zip(feature_names, valid_cols) if v]
    df_train   = pd.DataFrame(X_train[:, valid_cols], columns=feat_v).astype(np.float64)
    df_test    = pd.DataFrame(X_test[:, valid_cols],  columns=feat_v).astype(np.float64)
    x_train_jl = jl.DataFrame(df_train)
    x_test_jl  = jl.DataFrame(df_test)

    htb_kwargs = dict(loss=ht_loss, modality=HT_MODALITY, ntrees=HT_NTREES,
                      randomizecv=False, nofullsample=False,
                      overlap=max(purge_weeks - 1, 0), verbose="Off", warnings="Off")
    if ht_loss == "quantile":
        htb_kwargs["coeff"] = [0.5]
    param  = jl.HTBparam(**htb_kwargs)
    data   = jl.HTBdata(np.array(y_train, dtype=np.float64), x_train_jl, param)
    output = jl.HTBfit(data, param)
    preds  = np.array(jl.HTBpredict(x_test_jl, output))
    best_depth = int(output.bestvalue) if hasattr(output, "bestvalue") else -1
    ntrees     = int(output.ntrees)    if hasattr(output, "ntrees")    else -1
    return preds, best_depth, ntrees, output, data


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


for ht_loss, loss_label in LOSS_FNS:
    RESULTS_DIR = f"Results/HTBoost/{loss_label}/PurgedKFold"
    os.makedirs(RESULTS_DIR, exist_ok=True)
    print(f"\n========== HTBoost [{loss_label}] — purged k-fold ==========")
    perfold_all, summary = [], []

    for target in Y_COLS:
        horizon = parse_horizon(target)
        h       = H_WEEKS[horizon]
        left, right = purge_weeks_for(horizon)

        master = DF[(DF["Date"] >= PKF_START) & DF[target].notna()]["Date"]
        specs  = make_fold_specs(master, N_FOLDS, left, right)
        data   = DF[DF["Date"] >= PKF_START].dropna(subset=[target]).reset_index(drop=True)

        print(f"\n[{loss_label}] {target}  (h={h}w, purge {left}/{right}w)")
        fold_results, perfold_rows = [], []
        rel_frames = []   # per-fold HTBrelevance (native permutation importance)
        for spec in specs:
            if SMOKE and spec["fold"] != 2:
                continue
            tr_m, te_m = split_by_dates(data["Date"], spec)
            train, test = data[tr_m], data[te_m]
            if len(train) < 50 or len(test) == 0:
                print(f"  fold {spec['fold']}: insufficient data")
                continue
            try:
                # Attempt 1 — the proven recipe (drops >30% NaN cols), works on the other machine
                preds, depth, ntrees, ht_output, ht_data = _fit_htboost(
                    train[ALL_FEAT].values, train[target].values,
                    test[ALL_FEAT].values, ALL_FEAT, ht_loss, h)
            except Exception as e:
                # Attempt 2 — fallback only if the proven path threw (e.g. the Windows
                # SharedArray/@distributed BoundsError). Keep all columns so HTBoost's
                # internal feature count stays in sync. Untouched on machines where attempt 1 works.
                print(f"  fold {spec['fold']} fit error ({type(e).__name__}); retrying with all columns...")
                try:
                    preds, depth, ntrees, ht_output, ht_data = _fit_htboost(
                        train[ALL_FEAT].values, train[target].values,
                        test[ALL_FEAT].values, ALL_FEAT, ht_loss, h, nan_thresh=2.0)
                except Exception as e2:
                    print(f"  fold {spec['fold']} FAILED after fallback: {type(e2).__name__}: {e2}")
                    continue
            a = test[target].values
            fold_results.append({"fold": spec["fold"], "dates": test["Date"].values,
                                 "actuals": a, "preds": preds})

            # HTBrelevance on this fold's fitted model (same call as the holdout run)
            try:
                _, _, fns_sorted, fi_sorted, _ = jl.HTBrelevance(ht_output, ht_data, verbose=False)
                rel_frames.append(pd.Series(np.array(fi_sorted),
                                            index=[str(f) for f in fns_sorted]))
            except Exception as e:
                print(f"    [warn] HTBrelevance fold {spec['fold']} failed: {type(e).__name__}: {e}")
            rmse, rw = metrics.rmse(a, preds), metrics.rw_rmse(a)
            perfold_rows.append({
                "Horizon": horizon, "Fold": spec["fold"],
                "Test Start": spec["test_start"].date(), "Test End": spec["test_end"].date(),
                "n_train": len(train), "n_test": len(test), "depth": depth, "ntrees": ntrees,
                "RMSE": rmse, "MAE": metrics.mae(a, preds),
                "R2": metrics.r2(a, preds), "Hit": metrics.hit_rate(a, preds),
                "RW RMSE": rw, "Skill%": metrics.skill_score(rmse, rw),
            })
            print(f"  fold {spec['fold']}: n_train={len(train):4d} n_test={len(test):3d}  "
                  f"depth={depth} ntrees={ntrees:4d}  RMSE={rmse:.4f}  R²={metrics.r2(a, preds):+.3f}")

        if not fold_results:
            print("  no usable folds")
            continue
        pd.DataFrame(perfold_rows).to_csv(f"{RESULTS_DIR}/perfold_metrics_{_safe(target)}.csv", index=False)
        perfold_all.extend(perfold_rows)

        pooled = pool_fold_results(fold_results)
        pooled.to_csv(f"{RESULTS_DIR}/pooled_preds_{_safe(target)}.csv", index=False)

        # ---- Pooled HTBrelevance (averaged across folds) ----
        # Same CSV schema as the holdout run (htboost_model.py) so paper_plots.py renders the
        # "HTB Relevance" bar straight from the full-sample PKF. Features dropped in a fold
        # (>30% NaN) count as 0 relevance there, so the mean is over all folds.
        if rel_frames:
            rel = pd.concat(rel_frames, axis=1).fillna(0.0).mean(axis=1).sort_values(ascending=False)
            imp_df = pd.DataFrame({"Feature": rel.index, "Importance": rel.values})
            imp_df.insert(0, "Horizon", horizon)
            imp_df.to_csv(f"{RESULTS_DIR}/feature_importance_{_safe(target)}.csv", index=False)
            print(f"  HTBrelevance pooled over {len(rel_frames)} folds | top: "
                  + ", ".join(rel.head(3).index))

        a, p = pooled["Actual"].values, pooled["Predicted"].values
        pl_rmse, pl_rwrm = metrics.rmse(a, p), metrics.rw_rmse(a)
        pl_mae,  pl_rwmae = metrics.mae(a, p), metrics.rw_mae(a)
        dm_stat, dm_p = metrics.diebold_mariano(a, p, horizon=max(h, 1))
        dm_ols_stat, dm_ols_p = (_dm_vs_ols(pooled, target, h) if horizon in DM_OLS_HORIZONS else (None, None))

        summary.append({
            "Y": target, "Horizon": horizon, "Loss": loss_label,
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
              f"DM p(RW)={dm_p:.3f}" + (f"  DM p(OLS)={dm_ols_p:.3f}" if dm_ols_p is not None else ""))

        try:
            dates = pooled["Date"].values
            fig, axes = plt.subplots(2, 1, figsize=(14, 9), facecolor="white")
            fig.suptitle(f"HTBoost [{loss_label}] — Purged k-fold (pooled OOS)  —  {target}",
                         fontsize=13, fontweight="bold")
            ax = axes[0]
            ax.plot(dates, a, label="Actual", color=_DARK, lw=1.4, alpha=0.9)
            ax.plot(dates, p, label="HTBoost", color=_BLUE, lw=1.1, alpha=0.85)
            ax.axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
            for s in specs[1:]:
                ax.axvline(s["test_start"], color=_GREY, lw=0.6, ls=":", alpha=0.6)
            ax.set_title(f"Pooled OOS — RMSE={pl_rmse:.4f} | RW={pl_rwrm:.4f} | DM p(RW)={dm_p:.3f}", fontsize=10)
            ax.legend(frameon=False, fontsize=9)
            _style_ax(ax, ylabel="∆ Price (NOK/kg)")
            ax = axes[1]
            ax.plot(dates, np.cumsum((a - p) ** 2), label="HTBoost", color=_BLUE, lw=1.2)
            ax.plot(dates, np.cumsum(a ** 2), label="Random Walk", color=_GREY, lw=1.2, ls="--")
            ax.set_title("Cumulative Squared Error  (lower = better)", fontsize=10)
            ax.legend(frameon=False, fontsize=9)
            _style_ax(ax, ylabel="Cumulative SE")
            plt.tight_layout()
            plt.savefig(f"{RESULTS_DIR}/pooled_{_safe(target)}.pdf", format="pdf", bbox_inches="tight")
            plt.close()
        except Exception as e:
            print(f"  [warn] pooled plot failed: {type(e).__name__}: {e}")

    if perfold_all:
        pd.DataFrame(perfold_all).to_csv(f"{RESULTS_DIR}/perfold_metrics_all.csv", index=False)
    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(f"{RESULTS_DIR}/pooled_summary.csv", index=False)
    print(f"\nHTBoost [{loss_label}] pooled summary:")
    if len(summary_df):
        print(summary_df[["Horizon", "Pooled RMSE", "Pooled MAE", "Pooled R2",
                          "Skill%", "DM p (RW)", "DM p (OLS)", "n_indep"]].to_string(index=False))

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
        if len(disp):
            Plotter().results_table(
                disp,
                f"HTBoost [{loss_label}] — Purged k-fold (pooled OOS, full sample)\n"
                f"4 folds | depth auto-tuned per fold | DM (HLN+NW) vs RW and vs OLS",
                f"{RESULTS_DIR}/pooled_results.pdf", width=24)
    except Exception as e:
        print(f"[warn] results table failed: {type(e).__name__}: {e}")

print("\nHTBoost purged-k-fold done.")
