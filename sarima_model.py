##
#  SARIMA model — all Y horizons
#  Purged walk-forward CV; parameters selected by BIC via auto_arima
#  Includes: random walk baseline, Diebold-Mariano test
#  Final holdout: 2022–2025
#
#  SARIMA is univariate: input = weekly log-return r_t (Y 0w column).
#  Parameters are estimated ONCE per fold on the training period.
#  At each test date, the Kalman filter is re-run on ACTUAL observed
#  history via statsmodels `apply(refit=False)` — this conditions
#  predictions on real data without re-estimating parameters.
#
#  Note: Y 0w is a nowcast (S_t and F_t are Wednesday closes, simultaneously
#  determined). Results for Y 0w should be labelled nowcast, not forecast.
##

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import re, os, time
from pmdarima import auto_arima
from sklearn.metrics import mean_squared_error, r2_score
from scipy import stats
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt

## ── Config ───────────────────────────────────────────────────────────────────
HOLDOUT_START  = "2022-01-01"
WEEKLY_RET_COL = "Y 0w ∆ Salmon (NOK/KG)"   # r_t = ln(S_t / S_{t-1})

RESULTS_DIR = "Results/SARIMA"
os.makedirs(RESULTS_DIR, exist_ok=True)

## ── Load data ────────────────────────────────────────────────────────────────
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
df = df.sort_values("Date").reset_index(drop=True)

Y_COLS = [c for c in df.columns if c.startswith("Y ")]

## ── Horizon config ───────────────────────────────────────────────────────────
#  steps = number of consecutive weekly log-returns summed in the target
HORIZON_CONFIG = {
    "0w":  {"purge_weeks":  0, "n_folds": 10, "steps":  1},
    "1w":  {"purge_weeks":  1, "n_folds": 10, "steps":  2},
    "2w":  {"purge_weeks":  2, "n_folds": 10, "steps":  3},
    "1m":  {"purge_weeks":  4, "n_folds": 10, "steps":  5},
    "3m":  {"purge_weeks": 13, "n_folds":  8, "steps": 14},
    "6m":  {"purge_weeks": 26, "n_folds":  6, "steps": 27},
    "12m": {"purge_weeks": 52, "n_folds":  4, "steps": 53},
}

## ── SARIMA settings ──────────────────────────────────────────────────────────
#  m=52: weekly data, annual seasonal cycle.
#  d=0, D=0: log-returns are already stationary.
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

def parse_horizon(target):
    m = re.search(r"Y (\d+(?:w|m)) ", target)
    return m.group(1) if m else "0w"

## ── Diebold-Mariano test ─────────────────────────────────────────────────────
def diebold_mariano(actual, pred_model):
    e_model = actual - pred_model
    e_rw    = actual
    d       = e_model**2 - e_rw**2
    n       = len(d)
    dm_stat = np.mean(d) / (np.std(d, ddof=1) / np.sqrt(n))
    p_value = 2 * stats.norm.sf(np.abs(dm_stat))
    return dm_stat, p_value

## ── Purged walk-forward CV ───────────────────────────────────────────────────
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

            print(f"      fold {i}/{n_folds-1}  {order}  ({time.time()-t0:.0f}s)")

        except Exception as e:
            print(f"      fold {i} FAILED: {e}")
            continue

        results.append({
            "dates":   test["Date"].values,
            "actuals": test[target].values,
            "preds":   preds,
            "fold":    i,
            "order":   order,
        })

    return results

## ── Run per Y horizon ────────────────────────────────────────────────────────
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

    cv_data   = df[df["Date"] < HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)
    hold_data = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    label = f"{target}  [NOWCAST]" if is_nowcast else target
    print(f"\n{label}  (horizon={horizon}, purge={purge_wks}w, folds={n_folds}, steps={steps})")

    ## ── Purged CV ────────────────────────────────────────────────────────────
    print(f"  Running CV (m=52, seasonal) ...")
    t0 = time.time()
    fold_results = cv_sarima(df, cv_data, target, WEEKLY_RET_COL,
                              purge_wks, n_folds, steps)

    if fold_results:
        cv_preds   = np.concatenate([f["preds"]   for f in fold_results])
        cv_actuals = np.concatenate([f["actuals"] for f in fold_results])
        cv_dates   = np.concatenate([f["dates"]   for f in fold_results])

        cv_rmse    = np.sqrt(mean_squared_error(cv_actuals, cv_preds))
        cv_rw_rmse = np.sqrt(np.mean(cv_actuals**2))
        cv_r2      = r2_score(cv_actuals, cv_preds)
        cv_hitrate = np.mean(np.sign(cv_preds) == np.sign(cv_actuals))
        print(f"  CV       RMSE={cv_rmse:.4f}  RW_RMSE={cv_rw_rmse:.4f}  "
              f"R²={cv_r2:.4f}  Hit={cv_hitrate:.1%}  "
              f"(n_obs={len(cv_actuals)}, folds={len(fold_results)}, {time.time()-t0:.0f}s)")
    else:
        cv_rmse = cv_rw_rmse = cv_r2 = cv_hitrate = None
        cv_preds = cv_actuals = cv_dates = []
        print("  CV       insufficient data")

    ## ── Final model — trained on all pre-holdout weekly returns ──────────────
    print(f"  Running holdout ...")
    t0 = time.time()

    weekly_ret_full = (
        df[df["Date"] < HOLDOUT_START][WEEKLY_RET_COL]
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

        hold_actuals  = hold_data[target].values
        hold_rmse     = np.sqrt(mean_squared_error(hold_actuals, hold_preds))
        hold_rw_rmse  = np.sqrt(np.mean(hold_actuals**2))
        hold_r2       = r2_score(hold_actuals, hold_preds)
        hold_hitrate  = np.mean(np.sign(hold_preds) == np.sign(hold_actuals))
        dm_stat, dm_p = diebold_mariano(hold_actuals, hold_preds)

        print(f"  Holdout  RMSE={hold_rmse:.4f}  RW_RMSE={hold_rw_rmse:.4f}  "
              f"R²={hold_r2:.4f}  Hit={hold_hitrate:.1%}  "
              f"DM={dm_stat:.2f}  p={dm_p:.3f}  ({hold_order}, {time.time()-t0:.0f}s)")

    except Exception as e:
        hold_rmse = hold_rw_rmse = hold_r2 = hold_hitrate = None
        dm_stat = dm_p = None
        hold_actuals = hold_data[target].values
        print(f"  Holdout  FAILED: {e}")

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

    ## ── Plot ─────────────────────────────────────────────────────────────────
    n_axes = 2 if len(cv_preds) > 0 else 1
    fig, axes = plt.subplots(n_axes, 1, figsize=(14, 5 * n_axes))
    if n_axes == 1:
        axes = [axes]
    fig.suptitle(f"SARIMA  —  {target}{' [NOWCAST]' if is_nowcast else ''}", fontsize=13)

    ax_idx = 0
    if len(cv_preds) > 0:
        axes[ax_idx].plot(cv_dates, cv_actuals, label="Actual",    color="black", alpha=0.8, lw=1.5)
        axes[ax_idx].plot(cv_dates, cv_preds,   label="SARIMA",    alpha=0.75, lw=1.2)
        axes[ax_idx].axhline(0, color="grey", lw=0.8, ls="--", label="Random Walk (0)")
        axes[ax_idx].set_title(f"Purged CV  (RMSE={cv_rmse:.4f}, R²={cv_r2:.4f})")
        axes[ax_idx].legend(); axes[ax_idx].grid(True, alpha=0.3)
        ax_idx += 1

    axes[ax_idx].plot(hold_data["Date"].values, hold_actuals, label="Actual",
                      color="black", alpha=0.8, lw=1.5)
    if hold_preds is not None:
        axes[ax_idx].plot(hold_data["Date"].values, hold_preds, label="SARIMA", alpha=0.75, lw=1.2)
    axes[ax_idx].axhline(0, color="grey", lw=0.8, ls="--", label="Random Walk (0)")
    _title = f"Holdout 2022–2025  ({hold_order})"
    if hold_r2 is not None:
        _title += f"  R²={hold_r2:.4f}"
    axes[ax_idx].set_title(_title)
    axes[ax_idx].legend(); axes[ax_idx].grid(True, alpha=0.3)

    plt.tight_layout()
    safe_name = target.replace("/", "-").replace(" ", "_").replace("∆", "d")
    plt.savefig(f"{RESULTS_DIR}/sarima_{safe_name}.pdf", format="pdf", bbox_inches="tight")
    plt.close()

## ── Summary table ────────────────────────────────────────────────────────────
print("\n── Summary ──────────────────────────────────────────────────────────")
summary_df = pd.DataFrame(summary).set_index("Y")
print(summary_df.to_string())
summary_df.to_csv(f"{RESULTS_DIR}/sarima_summary.csv")

## ── PDF results table ────────────────────────────────────────────────────────
def _fmt(x, fmt=".4f"):
    return f"{x:{fmt}}" if pd.notna(x) and x is not None else "—"

disp = pd.DataFrame({
    "Horizon":   [r["Horizon"] for r in summary],
    "CV RMSE":   [_fmt(r["CV RMSE"]) for r in summary],
    "CV R²":     [_fmt(r["CV R2"], ".3f") for r in summary],
    "CV Hit":    [f'{r["CV Hit"]:.1%}' if r["CV Hit"] else "—" for r in summary],
    "Hold RMSE": [_fmt(r["Hold RMSE"]) for r in summary],
    "Hold R²":   [_fmt(r["Hold R2"], ".3f") for r in summary],
    "Hold Hit":  [f'{r["Hold Hit"]:.1%}' if r["Hold Hit"] else "—" for r in summary],
    "RW RMSE":   [_fmt(r["Hold RW RMSE"]) for r in summary],
    "Skill %":   [f'{(1 - r["Hold RMSE"]/r["Hold RW RMSE"])*100:+.1f}%'
                  if r["Hold RMSE"] and r["Hold RW RMSE"] else "—" for r in summary],
    "DM":        [_fmt(r["Hold DM"], ".2f") for r in summary],
    "p-value":   [f'{r["Hold DM p"]:.4f}' if r["Hold DM p"] is not None and r["Hold DM p"] >= 0.001
                  else ("< 0.001" if r["Hold DM p"] is not None else "—") for r in summary],
    "Order":     [r["Order"] for r in summary],
})

fig, ax = plt.subplots(figsize=(18, len(disp) * 0.55 + 2.5))
ax.axis("off")
ax.set_title("SARIMA — Results Summary\nHoldout: 2022–2025  |  Purged Walk-Forward CV  |  BIC Selection (m=52)",
             fontsize=13, fontweight="bold", pad=20)

table = ax.table(cellText=disp.values, colLabels=disp.columns,
                 loc="center", cellLoc="center")
table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1, 1.8)

for j in range(len(disp.columns)):
    table[0, j].set_facecolor("#2c3e50")
    table[0, j].set_text_props(color="white", fontweight="bold")
for i in range(1, len(disp) + 1):
    color = "#f0f4f8" if i % 2 == 0 else "white"
    for j in range(len(disp.columns)):
        table[i, j].set_facecolor(color)

plt.savefig(f"{RESULTS_DIR}/sarima_results.pdf", format="pdf", bbox_inches="tight", dpi=150)
plt.close()
print(f"PDF saved → {RESULTS_DIR}/sarima_results.pdf")
