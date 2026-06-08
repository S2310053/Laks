# This module construct the CatBoost model

##
# We construct our CatBoost model per Y horizon, using purged (=horizon) k-fold CV (Prado, 2018) with early stopping to select tree depth and evaluate performance
# As it's time series data, we split into folds sequentially (not randomly), as this might result in leakage
# The final model is trained on all training data and evaluated on the holdout (2022–2026)
# We run the model twice: once optimising RMSE, once optimising MAE, saving results to separate subdirectories
##

import pandas as pd
import numpy as np
import re
from catboost import CatBoostRegressor, Pool
import shap
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
OLS_RESULTS   = "Results/OLS"          # for Clark-West vs OLS benchmark

# Identify Y columns and features
Y_COLS   = [c for c in df.columns if c.startswith("Y ")]
NON_FEAT = {"Date"} | set(Y_COLS)
ALL_FEAT = [c for c in df.columns if c not in NON_FEAT]

# DM vs OLS only for horizons where forward contracts exist
DM_OLS_HORIZONS = {"1m", "3m", "6m", "12m"}

# Defines purge_weeks, n_folds, and tree depth per horizon
# Purge_weeks is the number of weeks to exclude between train and test sets in CV to prevent leakage (= horizon length)
# n_folds is the number of CV folds (adjusted based on horizon to ensure enough data per fold after purging)
HORIZON_CONFIG = {
    "0w":  {"purge_weeks":  0, "n_folds": 10},
    "1w":  {"purge_weeks":  1, "n_folds": 10},
    "2w":  {"purge_weeks":  2, "n_folds": 10},
    "1m":  {"purge_weeks":  4, "n_folds": 10},
    "3m":  {"purge_weeks": 13, "n_folds":  8},
    "6m":  {"purge_weeks": 26, "n_folds":  6},
    "12m": {"purge_weeks": 52, "n_folds":  3}}
# Note: We are open to tune these further, to improve performance. Especially n_folds for the longer horizons,
# as 12m only produces 2 reliable CV folds

# CatBoost settings
# Note: We used base parameters, but these can be changed if it can improve the model
CB_BASE = dict(
    iterations          = 1000,
    random_seed         = 42,
    verbose             = False,
    allow_writing_files = False,
)
EARLY_STOP_ROUNDS = 50
VAL_FRAC          = 0.15
DEPTH_GRID        = [2, 3, 4, 5, 6, 7, 8, 9]

# Loss functions to run — each produces results in its own subdirectory
LOSS_FNS = ["RMSE", "MAE"]

# Helper to extract horizon from Y column name
def parse_horizon(target):
    m = re.search(r"Y (\d+(?:w|m)) ", target)
    return m.group(1) if m else "0w"

# Purged k-fold CV (Prado et al. 2018 article on purged CV)
# Note: As it's time-series data, we split into folds sequentially (not randomly), as this might result in leakage
def purged_cv(data, target, features, purge_weeks, n_folds, depth, loss_fn):
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

        model = CatBoostRegressor(**CB_BASE, loss_function=loss_fn, depth=depth)
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
def cv_select_depth(data, target, features, purge_weeks, n_folds, depth_grid, loss_fn):
    best_depth   = depth_grid[0]
    best_metric  = np.inf
    best_results = []

    for d in depth_grid:
        results = purged_cv(data, target, features, purge_weeks, n_folds, d, loss_fn)
        if not results:
            continue
        preds   = np.concatenate([f["preds"]   for f in results])
        actuals = np.concatenate([f["actuals"] for f in results])
        cv_val  = (np.mean(np.abs(actuals - preds)) if loss_fn == "MAE"
                   else np.sqrt(np.mean((actuals - preds) ** 2)))
        print(f"depth={d}  CV {loss_fn}={cv_val:.4f}  (folds={len(results)})")
        if cv_val < best_metric:
            best_metric  = cv_val
            best_depth   = d
            best_results = results

    return best_depth, best_results


# Run model per Y horizon
for loss_fn in LOSS_FNS:

    RESULTS_DIR = f"Results/CatBoost/{loss_fn}"
    os.makedirs(RESULTS_DIR, exist_ok=True)

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

        print(f"\n[{loss_fn}] {label}  (horizon={horizon}, purge={purge_wks}w, folds={n_folds})")

        # Purged CV with depth selection
        print(f"Depth CV over {DEPTH_GRID}:")
        depth, fold_results = cv_select_depth(cv_data, target, ALL_FEAT, purge_wks, n_folds, DEPTH_GRID, loss_fn)
        print(f"Selected depth={depth}")

        if fold_results:
            cv_preds   = np.concatenate([f["preds"]   for f in fold_results])
            cv_actuals = np.concatenate([f["actuals"] for f in fold_results])
            cv_dates   = np.concatenate([f["dates"]   for f in fold_results])

            cv_rmse    = metrics.rmse(cv_actuals, cv_preds)
            cv_rw_rmse = metrics.rw_rmse(cv_actuals)
            cv_mae     = metrics.mae(cv_actuals, cv_preds)
            cv_rw_mae  = metrics.rw_mae(cv_actuals)
            cv_r2      = metrics.r2(cv_actuals, cv_preds)
            cv_rw_r2   = metrics.rw_r2(cv_actuals)
            cv_hitrate = metrics.hit_rate(cv_actuals, cv_preds)
            print(f"CV RMSE={cv_rmse:.4f}  MAE={cv_mae:.4f}  RW_RMSE={cv_rw_rmse:.4f}  "
                  f"R²={cv_r2:.4f}  Hit={cv_hitrate:.1%}  "
                  f"(n_obs={len(cv_actuals)}, folds={len(fold_results)})")
        else:
            cv_rmse = cv_rw_rmse = cv_mae = cv_rw_mae = cv_r2 = cv_rw_r2 = cv_hitrate = None
            cv_preds, cv_actuals, cv_dates = [], [], []
            print("CV: insufficient data for all folds")

        # Final model trained on embargoed training data (no target overlap with holdout)
        train_final    = cv_data[cv_data["Date"] < embargo_date].copy()
        val_size_final = max(int(len(train_final) * VAL_FRAC), 10)
        cv_train_final = train_final.iloc[:-val_size_final]
        cv_val_final   = train_final.iloc[-val_size_final:]

        final_model = CatBoostRegressor(**CB_BASE, loss_function=loss_fn, depth=depth)
        final_model.fit(
            cv_train_final[ALL_FEAT], cv_train_final[target],
            eval_set              = (cv_val_final[ALL_FEAT].values, cv_val_final[target].values),
            early_stopping_rounds = EARLY_STOP_ROUNDS,
        )

        hold_preds    = final_model.predict(hold_data[ALL_FEAT])
        hold_actuals  = hold_data[target].values
        hold_rmse     = metrics.rmse(hold_actuals, hold_preds)
        hold_rw_rmse  = metrics.rw_rmse(hold_actuals)
        hold_mae      = metrics.mae(hold_actuals, hold_preds)
        hold_rw_mae   = metrics.rw_mae(hold_actuals)
        hold_r2       = metrics.r2(hold_actuals, hold_preds)
        hold_rw_r2    = metrics.rw_r2(hold_actuals)
        hold_hitrate  = metrics.hit_rate(hold_actuals, hold_preds)
        dm_stat, dm_p = metrics.diebold_mariano(hold_actuals, hold_preds, horizon=max(purge_wks, 1))

        # DM test vs OLS benchmark (only for horizons where forward contracts exist)
        dm_ols_stat, dm_ols_p = None, None
        if horizon in DM_OLS_HORIZONS:
            safe_name_ols = target.replace("/", "-").replace(" ", "_")
            ols_csv = f"{OLS_RESULTS}/holdout_preds_{safe_name_ols}.csv"
            if os.path.exists(ols_csv):
                ols_df  = pd.read_csv(ols_csv, parse_dates=["Date"])
                merged  = pd.DataFrame({
                    "Date":   hold_data["Date"].values,
                    "Pred":   hold_preds,
                    "Actual": hold_actuals,
                })
                merged = merged.merge(
                    ols_df[["Date", "Predicted"]].rename(columns={"Predicted": "OLS_Pred"}),
                    on="Date", how="inner")
                if len(merged) >= 10:
                    dm_ols_stat, dm_ols_p = metrics.diebold_mariano(
                        merged["Actual"].values, merged["Pred"].values,
                        horizon=max(purge_wks, 1),
                        benchmark_pred=merged["OLS_Pred"].values)

        # SHAP values via CatBoost's native TreeSHAP (Lundberg & Lee, 2017)
        hold_pool = Pool(data=hold_data[ALL_FEAT].values, feature_names=ALL_FEAT)
        shap_raw  = final_model.get_feature_importance(data=hold_pool, type='ShapValues')
        # shap_raw: (n_holdout, n_features + 1) — last column is the bias term
        shap_vals = shap_raw[:, :-1]
        mean_shap = pd.Series(np.abs(shap_vals).mean(axis=0), index=ALL_FEAT).sort_values(ascending=False)

        # Print holdout results
        print(f"  Holdout  RMSE={hold_rmse:.4f}  MAE={hold_mae:.4f}  RW_RMSE={hold_rw_rmse:.4f}  "
              f"R²={hold_r2:.4f}  Hit={hold_hitrate:.1%}  "
              f"DM={dm_stat:.2f}  p={dm_p:.3f}"
              + (f"  DM_OLS={dm_ols_stat:.2f}  p={dm_ols_p:.3f}" if dm_ols_stat is not None else ""))

        # Save holdout predictions for model comparison

        safe_name = target.replace("/", "-").replace(" ", "_")
        pd.DataFrame({
            "Date": hold_data["Date"].values,
            "Actual": hold_actuals,
            "Predicted": hold_preds,
        }).to_csv(f"{RESULTS_DIR}/holdout_preds_{safe_name}.csv", index=False)

        # Save SHAP values — raw (signed, per observation) and mean |SHAP| importance
        pd.DataFrame(shap_vals, columns=ALL_FEAT, index=hold_data["Date"].values)\
            .to_csv(f"{RESULTS_DIR}/shap_over_time_{safe_name}.csv")

        # Self-contained beeswarm data: one row per (Date, Feature) with the SHAP value
        # (dot position) and the raw feature value (dot colour) — reproduces the beeswarm
        _bee_shap = pd.DataFrame(shap_vals, columns=ALL_FEAT, index=hold_data["Date"].values)\
                      .rename_axis("Date").reset_index().melt(id_vars="Date", var_name="Feature", value_name="SHAP")
        _bee_val  = pd.DataFrame(hold_data[ALL_FEAT].values, columns=ALL_FEAT, index=hold_data["Date"].values)\
                      .rename_axis("Date").reset_index().melt(id_vars="Date", var_name="Feature", value_name="Value")
        _bee_shap.merge(_bee_val, on=["Date", "Feature"])\
                 .to_csv(f"{RESULTS_DIR}/shap_beeswarm_{safe_name}.csv", index=False)

        shap_imp_df = pd.DataFrame({"Feature": mean_shap.index, "Importance": mean_shap.values})
        shap_imp_df.insert(0, "Horizon", horizon)
        shap_imp_df.to_csv(f"{RESULTS_DIR}/feature_importance_{safe_name}.csv", index=False)

        # Summary of results per horizon
        summary.append({
            "Y":             target,
            "Horizon":       horizon,
            "Nowcast":       is_nowcast,
            "Loss":          loss_fn,
            "CV RMSE":       cv_rmse,
            "CV RW RMSE":    cv_rw_rmse,
            "CV MAE":        cv_mae,
            "CV RW MAE":     cv_rw_mae,
            "CV R2":         cv_r2,
            "CV RW R2":      cv_rw_r2,
            "CV Hit":        cv_hitrate,
            "Hold RMSE":     hold_rmse,
            "Hold RW RMSE":  hold_rw_rmse,
            "Hold MAE":      hold_mae,
            "Hold RW MAE":   hold_rw_mae,
            "Hold R2":       hold_r2,
            "Hold RW R2":    hold_rw_r2,
            "Hold Hit":      hold_hitrate,
            "Hold DM":       dm_stat,
            "Hold DM p":     dm_p,
            "Hold DM OLS":   dm_ols_stat,
            "Hold DM OLS p": dm_ols_p,
            "Depth":         depth,
            "n_train":       len(cv_data),
            "n_holdout":     len(hold_data),
        })

        print(f"  Top 10 features by mean |SHAP|:")
        for feat, score in mean_shap.head(10).items():
            print(f"    {score:.4f}  {feat}")

        # Prediction plot — CV and holdout panels (SHAP beeswarm saved separately below)
        n_axes = 2 if len(cv_preds) > 0 else 1
        fig, axes = plt.subplots(n_axes, 1, figsize=(14, 5 * n_axes), facecolor="white")
        if n_axes == 1:
            axes = [axes]
        fig.suptitle(f"CatBoost [{loss_fn}]  —  {target}{'  [NOWCAST]' if is_nowcast else ''}",
                     fontsize=13, fontweight="bold")

        ax_idx = 0
        if len(cv_preds) > 0:
            axes[ax_idx].plot(cv_dates, cv_actuals, label="Actual",    color=_DARK, lw=1.5, alpha=0.85)
            axes[ax_idx].plot(cv_dates, cv_preds,   label="Predicted", color=_BLUE, lw=1.2, alpha=0.85)
            axes[ax_idx].axhline(0, color=_GREY, lw=0.8, ls="--")
            axes[ax_idx].set_title(
                f"Purged CV — RMSE={cv_rmse:.4f}  MAE={cv_mae:.4f}  |  RW={cv_rw_rmse:.4f}  |  R²={cv_r2:.4f}",
                fontsize=10)
            axes[ax_idx].legend(frameon=False, fontsize=9)
            _style_ax(axes[ax_idx], ylabel="∆ Price (NOK/kg)")
            ax_idx += 1

        _hold_title = (f"Holdout 2022–2025 — RMSE={hold_rmse:.4f}  MAE={hold_mae:.4f}  |  "
                       f"R²={hold_r2:.4f}  |  DM p={dm_p:.3f}")
        if dm_ols_stat is not None:
            _hold_title += f"  |  DM_OLS p={dm_ols_p:.3f}"
        axes[ax_idx].plot(hold_data["Date"].values, hold_actuals, label="Actual",    color=_DARK, lw=1.5, alpha=0.85)
        axes[ax_idx].plot(hold_data["Date"].values, hold_preds,   label="Predicted", color=_BLUE, lw=1.2, alpha=0.85)
        axes[ax_idx].axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
        axes[ax_idx].set_title(_hold_title, fontsize=10)
        axes[ax_idx].legend(frameon=False, fontsize=9)
        _style_ax(axes[ax_idx], ylabel="∆ Price (NOK/kg)")

        plt.tight_layout()
        plt.savefig(f"{RESULTS_DIR}/{safe_name}.pdf", format="pdf", bbox_inches="tight")
        plt.close()

        # SHAP beeswarm — separate PDF (Lundberg & Lee, 2017)
        # Each dot = one holdout observation; x-axis = SHAP value (push on prediction);
        # colour = feature value (red = high, blue = low)
        shap.summary_plot(
            shap_vals,
            pd.DataFrame(hold_data[ALL_FEAT].values, columns=ALL_FEAT),
            plot_type   = "dot",
            max_display = 20,
            show        = False,
        )
        plt.gcf().set_size_inches(10, 8)
        plt.title(
            f"SHAP Beeswarm  —  CatBoost [{loss_fn}]  |  {target}{'  [NOWCAST]' if is_nowcast else ''}\n"
            f"Holdout 2022–2025  |  Top 20 features  |  depth={depth}",
            fontsize=10, fontweight="bold", pad=12,
        )
        plt.tight_layout()
        plt.savefig(f"{RESULTS_DIR}/{safe_name}_shap.pdf", format="pdf", bbox_inches="tight")
        plt.close()

        # SHAP over time — Panel 1: signed SHAP values, Panel 2: rolling mean |SHAP|
        TOP_N    = 20
        roll_wks = 8
        top_feats = mean_shap.head(TOP_N).index.tolist()
        shap_df   = pd.DataFrame(shap_vals, columns=ALL_FEAT,
                                 index=pd.to_datetime(hold_data["Date"].values))
        shap_top  = shap_df[top_feats]

        fig, axes = plt.subplots(2, 1, figsize=(14, 10), facecolor="white")
        fig.suptitle(
            f"SHAP Over Time  —  CatBoost [{loss_fn}]  |  {target}"
            f"{'  [NOWCAST]' if is_nowcast else ''}\n"
            f"Top {TOP_N} features by mean |SHAP|  |  Holdout 2022–2025",
            fontsize=11, fontweight="bold",
        )

        ax = axes[0]
        for feat in top_feats:
            ax.plot(shap_top.index, shap_top[feat], lw=1.0, alpha=0.8, label=feat)
        ax.axhline(0, color=_GREY, lw=0.8, ls="--")
        ax.set_title("Signed SHAP values over time  (positive = pushes prediction up)", fontsize=10)
        ax.legend(frameon=False, fontsize=7, loc="upper left",
                  bbox_to_anchor=(1.01, 1), borderaxespad=0)
        _style_ax(ax, ylabel="SHAP value")

        ax = axes[1]
        for feat in top_feats:
            rolling = shap_top[feat].abs().rolling(roll_wks, min_periods=1).mean()
            ax.plot(shap_top.index, rolling, lw=1.0, alpha=0.8, label=feat)
        ax.set_title(f"{roll_wks}-week rolling mean |SHAP|  (feature importance over time)", fontsize=10)
        ax.legend(frameon=False, fontsize=7, loc="upper left",
                  bbox_to_anchor=(1.01, 1), borderaxespad=0)
        _style_ax(ax, ylabel="|SHAP| rolling mean")

        plt.tight_layout()
        plt.savefig(f"{RESULTS_DIR}/{safe_name}_shap_over_time.pdf", format="pdf", bbox_inches="tight")
        plt.close()

    #Summary table
    print(f"\n Summary of CatBoost [{loss_fn}] results:")
    summary_df = pd.DataFrame(summary).set_index("Y")
    print(summary_df.to_string())
    summary_df.to_csv(f"{RESULTS_DIR}/catboost_summary.csv")

    # PDF results table
    disp = pd.DataFrame({
        "Horizon":    [r["Horizon"] for r in summary],
        "CV RMSE":    [metrics.fmt(r["CV RMSE"]) for r in summary],
        "CV MAE":     [metrics.fmt(r["CV MAE"]) for r in summary],
        "CV R²":      [metrics.fmt(r["CV R2"], ".3f") for r in summary],
        "CV Hit":     [f'{r["CV Hit"]:.1%}' if r["CV Hit"] else "—" for r in summary],
        "Hold RMSE":  [metrics.fmt(r["Hold RMSE"]) for r in summary],
        "Hold MAE":   [metrics.fmt(r["Hold MAE"]) for r in summary],
        "Hold R²":    [metrics.fmt(r["Hold R2"], ".3f") for r in summary],
        "Hold Hit":   [f'{r["Hold Hit"]:.1%}' if r["Hold Hit"] else "—" for r in summary],
        "RW RMSE":    [metrics.fmt(r["Hold RW RMSE"]) for r in summary],
        "Skill%":     [f'{(1 - r["Hold RMSE"]/r["Hold RW RMSE"])*100:+.1f}%' for r in summary],
        "MAE Skill%": [f'{(1 - r["Hold MAE"]/r["Hold RW MAE"])*100:+.1f}%' for r in summary],
        "DM":         [metrics.fmt(r["Hold DM"], ".2f") for r in summary],
        "p(DM)":      [f'{r["Hold DM p"]:.4f}' if r["Hold DM p"] >= 0.001
                       else "< 0.001" for r in summary],
        "DM OLS":     [metrics.fmt(r["Hold DM OLS"], ".2f") for r in summary],
        "p(DM OLS)":  [f'{r["Hold DM OLS p"]:.4f}' if r["Hold DM OLS p"] is not None and r["Hold DM OLS p"] >= 0.001
                       else ("< 0.001" if r["Hold DM OLS p"] is not None else "—") for r in summary],
        "Depth":      [r["Depth"] for r in summary],
    })

    Plotter().results_table(
        disp,
        f"CatBoost [{loss_fn}] — Results Summary\n"
        f"Holdout: 2022–2026 | Purged CV | Depth CV'd over {{2,...,9}} | "
        f"DM = Harvey et al. (1997) vs RW | DM OLS = Diebold-Mariano vs OLS",
        f"{RESULTS_DIR}/catboost_results.pdf",
        width=24,
    )
