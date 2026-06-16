# This module runs the CatBoost model under the purged k-fold method
#
##
# Additive companion to catboost_model.py — the walk-forward + holdout run is left untouched.
# CatBoost is evaluated with the true purged k-fold over the full sample (from PKF_START),
# pooling OOS predictions across the 4 folds, for both the RMSE and MAE loss variants and all
# 7 horizons. CatBoost ingests NaN natively, so the full feature matrix is used (no imputation,
# no row dropping). Tree depth is REUSED from the walk-forward selection (catboost_summary.csv)
# rather than re-searched per fold — agreed, to keep runtime sane.
# DM vs RW at every horizon; DM vs OLS at 1m/3m/6m/12m using the OLS purged-k-fold pooled preds.
##

import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from catboost import CatBoostRegressor, Pool
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

OLS_PKF        = "Results/OLS/PurgedKFold"
DM_OLS_HORIZONS = {"1m", "3m", "6m", "12m"}
LOSS_FNS       = ["RMSE", "MAE"]

# Same fixed CatBoost settings as the walk-forward script (only depth is reused per horizon)
CB_BASE = dict(iterations=1000, random_seed=42, verbose=False, allow_writing_files=False)
EARLY_STOP_ROUNDS = 50
VAL_FRAC          = 0.15

import re
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


def _fit_predict(train, test, target, depth, loss_fn):
    """Fit CatBoost on a (possibly non-contiguous) training block, predict the test fold.
    Validation slice for early stopping = most recent VAL_FRAC of the training rows (date-sorted)."""
    val_size    = max(int(len(train) * VAL_FRAC), 10)
    train_inner = train.iloc[:-val_size]
    val_inner   = train.iloc[-val_size:]
    model = CatBoostRegressor(**CB_BASE, loss_function=loss_fn, depth=int(depth))
    model.fit(
        train_inner[ALL_FEAT], train_inner[target],
        eval_set=(val_inner[ALL_FEAT].values, val_inner[target].values),
        early_stopping_rounds=EARLY_STOP_ROUNDS,
    )
    return model.predict(test[ALL_FEAT]), int(model.tree_count_), model


def _dm_vs_ols(pooled, target, h):
    """DM vs OLS on common dates, using the OLS purged-k-fold pooled predictions."""
    ols_csv = f"{OLS_PKF}/pooled_preds_{_safe(target)}.csv"
    if not os.path.exists(ols_csv):
        return None, None
    ols = pd.read_csv(ols_csv, parse_dates=["Date"]).rename(columns={"Predicted": "OLS"})
    m = pooled.merge(ols[["Date", "OLS"]], on="Date", how="inner")
    if len(m) < 10:
        return None, None
    return metrics.diebold_mariano(m["Actual"].values, m["Predicted"].values,
                                   horizon=max(h, 1), benchmark_pred=m["OLS"].values)


for loss_fn in LOSS_FNS:
    RESULTS_DIR = f"Results/CatBoost/{loss_fn}/PurgedKFold"
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Reuse depth selected by the walk-forward run for this loss variant
    cb_sum   = pd.read_csv(f"Results/CatBoost/{loss_fn}/catboost_summary.csv")
    depth_by = {str(r["Horizon"]): int(r["Depth"]) for _, r in cb_sum.iterrows()}

    print(f"\n========== CatBoost [{loss_fn}] — purged k-fold ==========")
    perfold_all, summary = [], []

    for target in Y_COLS:
        horizon = parse_horizon(target)
        if horizon not in depth_by:
            print(f"[SKIP] {target} — no walk-forward depth on record")
            continue
        depth = depth_by[horizon]
        h     = H_WEEKS[horizon]
        left, right = purge_weeks_for(horizon)

        master = DF[(DF["Date"] >= PKF_START) & DF[target].notna()]["Date"]
        specs  = make_fold_specs(master, N_FOLDS, left, right)
        data   = DF[DF["Date"] >= PKF_START].dropna(subset=[target]).reset_index(drop=True)

        print(f"\n[{loss_fn}] {target}  (h={h}w, purge {left}/{right}w, depth={depth})")
        fold_results, perfold_rows = [], []
        shap_frames, val_frames   = [], []   # per-fold OOS SHAP + raw feature values
        for spec in specs:
            tr_m, te_m = split_by_dates(data["Date"], spec)
            train, test = data[tr_m], data[te_m]
            if len(train) < 50 or len(test) == 0:
                print(f"  fold {spec['fold']}: insufficient data")
                continue
            preds, ntrees, model = _fit_predict(train, test, target, depth, loss_fn)
            a = test[target].values
            fold_results.append({"fold": spec["fold"], "dates": test["Date"].values,
                                 "actuals": a, "preds": preds})

            # OOS SHAP on this fold's test block (same TreeSHAP recipe as the holdout run);
            # every observation is scored by the fold model that never trained on it.
            idx = pd.to_datetime(test["Date"].values)
            sv  = model.get_feature_importance(
                      data=Pool(test[ALL_FEAT].values, feature_names=ALL_FEAT),
                      type="ShapValues")[:, :-1]      # drop trailing bias column
            shap_frames.append(pd.DataFrame(sv, columns=ALL_FEAT, index=idx))
            val_frames.append(pd.DataFrame(test[ALL_FEAT].values, columns=ALL_FEAT, index=idx))
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
                  f"ntrees={ntrees:4d}  RMSE={rmse:.4f}  R²={metrics.r2(a, preds):+.3f}")

        if not fold_results:
            print("  no usable folds")
            continue

        pd.DataFrame(perfold_rows).to_csv(f"{RESULTS_DIR}/perfold_metrics_{_safe(target)}.csv", index=False)
        perfold_all.extend(perfold_rows)

        pooled = pool_fold_results(fold_results)
        pooled.to_csv(f"{RESULTS_DIR}/pooled_preds_{_safe(target)}.csv", index=False)

        # ---- Pooled OOS SHAP (stacked across folds, date-sorted) ----
        # Same CSV schema as the holdout run (catboost_model.py) so paper_plots.py renders the
        # beeswarm / importance straight from the full-sample PKF with no plotting changes.
        if shap_frames:
            shap_oos = pd.concat(shap_frames).sort_index()
            val_oos  = pd.concat(val_frames).sort_index()
            shap_oos.rename_axis("Date").to_csv(f"{RESULTS_DIR}/shap_over_time_{_safe(target)}.csv")

            bee_shap = shap_oos.rename_axis("Date").reset_index().melt(
                          id_vars="Date", var_name="Feature", value_name="SHAP")
            bee_val  = val_oos.rename_axis("Date").reset_index().melt(
                          id_vars="Date", var_name="Feature", value_name="Value")
            bee_shap.merge(bee_val, on=["Date", "Feature"]).to_csv(
                f"{RESULTS_DIR}/shap_beeswarm_{_safe(target)}.csv", index=False)

            mean_shap = shap_oos.abs().mean().sort_values(ascending=False)
            imp_df = pd.DataFrame({"Feature": mean_shap.index, "Importance": mean_shap.values})
            imp_df.insert(0, "Horizon", horizon)
            imp_df.to_csv(f"{RESULTS_DIR}/feature_importance_{_safe(target)}.csv", index=False)
            print(f"  SHAP pooled over {len(shap_oos)} OOS obs | top: "
                  + ", ".join(mean_shap.head(3).index))

        a, p = pooled["Actual"].values, pooled["Predicted"].values
        pl_rmse, pl_rwrm = metrics.rmse(a, p), metrics.rw_rmse(a)
        pl_mae,  pl_rwmae = metrics.mae(a, p), metrics.rw_mae(a)
        dm_stat, dm_p = metrics.diebold_mariano(a, p, horizon=max(h, 1))
        dm_ols_stat, dm_ols_p = (_dm_vs_ols(pooled, target, h) if horizon in DM_OLS_HORIZONS else (None, None))

        summary.append({
            "Y": target, "Horizon": horizon, "Loss": loss_fn, "Depth": depth,
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
            fig.suptitle(f"CatBoost [{loss_fn}] — Purged k-fold (pooled OOS)  —  {target}",
                         fontsize=13, fontweight="bold")
            ax = axes[0]
            ax.plot(dates, a, label="Actual", color=_DARK, lw=1.4, alpha=0.9)
            ax.plot(dates, p, label="CatBoost", color=_BLUE, lw=1.1, alpha=0.85)
            ax.axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
            for s in specs[1:]:
                ax.axvline(s["test_start"], color=_GREY, lw=0.6, ls=":", alpha=0.6)
            ax.set_title(f"Pooled OOS — RMSE={pl_rmse:.4f} | RW={pl_rwrm:.4f} | DM p(RW)={dm_p:.3f}", fontsize=10)
            ax.legend(frameon=False, fontsize=9)
            _style_ax(ax, ylabel="∆ Price (NOK/kg)")
            ax = axes[1]
            ax.plot(dates, np.cumsum((a - p) ** 2), label="CatBoost", color=_BLUE, lw=1.2)
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
    print(f"\nCatBoost [{loss_fn}] pooled summary:")
    print(summary_df[["Horizon", "Depth", "Pooled RMSE", "Pooled MAE", "Pooled R2",
                      "Skill%", "DM p (RW)", "DM p (OLS)", "n_indep"]].to_string(index=False))

    try:
        def _fp(x):
            return "—" if x is None or (isinstance(x, float) and np.isnan(x)) else (f"{x:.4f}" if x >= 0.001 else "< 0.001")
        disp = pd.DataFrame({
            "Horizon":   [r["Horizon"] for r in summary],
            "Depth":     [r["Depth"] for r in summary],
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
            f"CatBoost [{loss_fn}] — Purged k-fold (pooled OOS, full sample)\n"
            f"4 folds | depth reused from walk-forward | DM (HLN+NW) vs RW and vs OLS",
            f"{RESULTS_DIR}/pooled_results.pdf", width=24)
    except Exception as e:
        print(f"[warn] results table failed: {type(e).__name__}: {e}")

print("\nCatBoost purged-k-fold done.")
