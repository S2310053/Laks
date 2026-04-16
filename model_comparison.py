##
#  Model comparison: CatBoost vs SARIMA vs Random Walk
#  Horizons: Y 1m, Y 3m, Y 6m, Y 12m
#  Purged walk-forward CV (same structure as catboost_model.py)
#  Holdout: 2022-2025
#
#  Metrics: RMSE, MAE, R², Hit Rate (log-return space)
#
##

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import re, os, time
from catboost import CatBoostRegressor
from pmdarima import auto_arima
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt

## ── Config ───────────────────────────────────────────────────────────────────
HOLDOUT_START  = "2022-01-01"
WEEKLY_RET_COL = "Y 0w ∆ Salmon (NOK/KG)"   # r_t = ln(S_t / S_{t-1})

os.makedirs("Results", exist_ok=True)

## ── Load data ────────────────────────────────────────────────────────────────
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
df = df.sort_values("Date").reset_index(drop=True)

Y_COLS   = [c for c in df.columns if c.startswith("Y ")]
NON_FEAT = {"Date", "Spot (NOK/KG)"} | set(Y_COLS)
ALL_FEAT = [c for c in df.columns if c not in NON_FEAT]

## ── Horizon config ───────────────────────────────────────────────────────────
#  steps       = number of consecutive weekly log-returns summed in the target
#  purge_weeks = rows removed between train and test to prevent label overlap
#
#  Target construction (feature_engineer.py):
#    Y 0w  = _dln_spot                    → 1 return,  shift=0
#    Y 1w  = rolling(2).sum().shift(-1)   → 2 returns, shift=1
#    Y 2w  = rolling(3).sum().shift(-2)   → 3 returns, shift=2
#    Y 1m  = rolling(5).sum().shift(-4)   → 5 returns, shift=4
#    Y 3m  = rolling(14).sum().shift(-13) → 14 returns, shift=13
#    Y 6m  = rolling(27).sum().shift(-26) → 27 returns, shift=26
#    Y 12m = rolling(53).sum().shift(-52) → 53 returns, shift=52
HORIZONS = {
    "Y 0w ∆ Salmon (NOK/KG)":  {"purge_weeks":  0, "n_folds": 10, "steps":  1},
    "Y 1w ∆ Salmon (NOK/KG)":  {"purge_weeks":  1, "n_folds": 10, "steps":  2},
    "Y 2w ∆ Salmon (NOK/KG)":  {"purge_weeks":  2, "n_folds": 10, "steps":  3},
    "Y 1m ∆ Salmon (NOK/KG)":  {"purge_weeks":  4, "n_folds": 10, "steps":  5},
    "Y 3m ∆ Salmon (NOK/KG)":  {"purge_weeks": 13, "n_folds":  8, "steps": 14},
    "Y 6m ∆ Salmon (NOK/KG)":  {"purge_weeks": 26, "n_folds":  6, "steps": 27},
    "Y 12m ∆ Salmon (NOK/KG)": {"purge_weeks": 52, "n_folds":  4, "steps": 53},
}

## ── CatBoost params (identical to catboost_model.py) ─────────────────────────
CB_PARAMS = dict(
    iterations=500, learning_rate=0.05, depth=6,
    loss_function="RMSE", random_seed=42,
    verbose=False, allow_writing_files=False,
)

## ── SARIMA params ────────────────────────────────────────────────────────────
#  m=52: weekly data, annual seasonal cycle (52 weeks per year).
#  d=0, D=0: log-returns are already stationary — no differencing needed.
#  Orders selected by AIC via stepwise search.
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

## ── Metrics ──────────────────────────────────────────────────────────────────
#  R² is valid here because targets are log-returns (stationary).
#  R² > 0 means the model beats predicting the sample mean.
#  R² < 0 means the model is worse than the sample mean.
def rmse(a, b): return np.sqrt(mean_squared_error(a, b))
def mae(a, b):  return np.mean(np.abs(a - b))
def r2(a, b):   return r2_score(a, b)
def hit(a, b):  return np.mean(np.sign(a) == np.sign(b))

## ── Purged walk-forward CV: CatBoost ─────────────────────────────────────────
def cv_catboost(cv_data, target, features, purge_weeks, n_folds):
    n         = len(cv_data)
    fold_size = n // n_folds
    results   = []

    for i in range(1, n_folds):
        test_start = i * fold_size
        test_end   = (i + 1) * fold_size if i < n_folds - 1 else n
        train_end  = test_start - purge_weeks
        if train_end < fold_size:
            continue

        train = cv_data.iloc[:train_end].dropna(subset=[target])
        test  = cv_data.iloc[test_start:test_end].dropna(subset=[target])

        if len(train) < 50 or len(test) == 0:
            continue

        model = CatBoostRegressor(**CB_PARAMS)
        model.fit(train[features], train[target])

        results.append({
            "dates":   test["Date"].values,
            "actuals": test[target].values,
            "preds":   model.predict(test[features]),
        })

    return results

## ── Purged walk-forward CV: SARIMA ───────────────────────────────────────────
#
#  SARIMA is univariate: input = weekly log-return r_t (Y_0w column).
#
#  Parameters are estimated ONCE per fold on weekly returns up to the purged
#  train_cutoff_date. For each test observation at date t:
#    (1) Apply fitted parameters to the ACTUAL observed weekly-return history
#        r_1, ..., r_t  (no parameter re-estimation — `apply(refit=False)`)
#    (2) Forecast r_{t+1}, ..., r_{t+steps}
#    (3) Sum → prediction of Y_h at date t
#
#  This is analogous to how CatBoost gets fresh feature values at each test
#  date: the model's parameters are locked to pre-test data, but predictions
#  condition on actual observations available at time t.
#
def cv_sarima(df_all, cv_data, target, weekly_ret_col, purge_weeks, n_folds, steps):
    n         = len(cv_data)
    fold_size = n // n_folds
    results   = []

    # All weekly returns (with dates) — used for conditioning at each test date
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

        # Date of last row used for training (drives SARIMA cutoff)
        train_cutoff_date = cv_data.iloc[train_end - 1]["Date"]

        # Training weekly returns: up to purged cutoff (parameters see only this)
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
                # Target at t is Σ r_t, r_{t+1}, ..., r_{t+steps-1}
                # Condition on r_1, ..., r_{t-1} (strictly before t)
                # Forecast r_t, ..., r_{t+steps-1}, sum
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
        })

    return results

## ── Flatten fold results ─────────────────────────────────────────────────────
def _concat(folds, key):
    return np.concatenate([f[key] for f in folds])

## ── Main loop ────────────────────────────────────────────────────────────────
summary = []

for target, cfg in HORIZONS.items():
    purge_wks = cfg["purge_weeks"]
    n_folds   = cfg["n_folds"]
    steps     = cfg["steps"]

    cv_data   = (df[df["Date"] < HOLDOUT_START]
                 .dropna(subset=[target])
                 .copy()
                 .reset_index(drop=True))
    hold_data = (df[df["Date"] >= HOLDOUT_START]
                 .dropna(subset=[target])
                 .copy()
                 .reset_index(drop=True))

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    print(f"\n{'='*65}")
    print(f"TARGET: {target}")
    print(f"  purge={purge_wks}w  folds={n_folds}  steps={steps}"
          f"  cv_n={len(cv_data)}  hold_n={len(hold_data)}")

    # ── Random Walk baseline (predicts Δln = 0) ───────────────────────────────
    #  RW RMSE = std(Y_h).  RW R² = r2_score(y, zeros) — typically near 0
    #  since mean(log-return) ≈ 0.
    rw_cv_a     = cv_data[target].values
    rw_hld_a    = hold_data[target].values
    rw_cv_rmse  = float(np.std(rw_cv_a))
    rw_hld_rmse = float(np.std(rw_hld_a))
    rw_cv_mae   = float(np.mean(np.abs(rw_cv_a)))
    rw_hld_mae  = float(np.mean(np.abs(rw_hld_a)))
    rw_cv_r2    = r2(rw_cv_a,  np.zeros(len(rw_cv_a)))
    rw_hld_r2   = r2(rw_hld_a, np.zeros(len(rw_hld_a)))
    print(f"\n  [RW]  CV  RMSE={rw_cv_rmse:.4f}  MAE={rw_cv_mae:.4f}  R²={rw_cv_r2:.4f}  Hit=50.0%")
    print(f"  [RW]  Hld RMSE={rw_hld_rmse:.4f}  MAE={rw_hld_mae:.4f}  R²={rw_hld_r2:.4f}  Hit=50.0%")

    # ── CatBoost CV ──────────────────────────────────────────────────────────
    print(f"\n  [CB]  running CV ...")
    t0 = time.time()
    cb_folds = cv_catboost(cv_data, target, ALL_FEAT, purge_wks, n_folds)

    if cb_folds:
        cb_cv_a    = _concat(cb_folds, "actuals")
        cb_cv_p    = _concat(cb_folds, "preds")
        cb_cv_rmse = rmse(cb_cv_a, cb_cv_p)
        cb_cv_mae  = mae(cb_cv_a, cb_cv_p)
        cb_cv_r2   = r2(cb_cv_a, cb_cv_p)
        cb_cv_hit  = hit(cb_cv_p, cb_cv_a)
        print(f"  [CB]  CV  RMSE={cb_cv_rmse:.4f}  MAE={cb_cv_mae:.4f}"
              f"  R²={cb_cv_r2:.4f}  Hit={cb_cv_hit:.1%}  (n={len(cb_cv_a)}, {time.time()-t0:.0f}s)")
    else:
        cb_cv_rmse = cb_cv_mae = cb_cv_r2 = cb_cv_hit = None
        print("  [CB]  CV  insufficient data")

    # ── CatBoost holdout ─────────────────────────────────────────────────────
    cb_model = CatBoostRegressor(**CB_PARAMS)
    cb_model.fit(cv_data[ALL_FEAT], cv_data[target])
    cb_h_p   = cb_model.predict(hold_data[ALL_FEAT])
    cb_h_a   = hold_data[target].values

    cb_h_rmse = rmse(cb_h_a, cb_h_p)
    cb_h_mae  = mae(cb_h_a, cb_h_p)
    cb_h_r2   = r2(cb_h_a, cb_h_p)
    cb_h_hit  = hit(cb_h_p, cb_h_a)
    print(f"  [CB]  Hld RMSE={cb_h_rmse:.4f}  MAE={cb_h_mae:.4f}"
          f"  R²={cb_h_r2:.4f}  Hit={cb_h_hit:.1%}  (n={len(cb_h_a)})")

    # ── SARIMA CV ─────────────────────────────────────────────────────────────
    print(f"\n  [SA]  running CV (m=52, seasonal) ...")
    t0 = time.time()
    sa_folds = cv_sarima(df, cv_data, target, WEEKLY_RET_COL,
                         purge_wks, n_folds, steps)

    if sa_folds:
        sa_cv_a    = _concat(sa_folds, "actuals")
        sa_cv_p    = _concat(sa_folds, "preds")
        sa_cv_rmse = rmse(sa_cv_a, sa_cv_p)
        sa_cv_mae  = mae(sa_cv_a, sa_cv_p)
        sa_cv_r2   = r2(sa_cv_a, sa_cv_p)
        sa_cv_hit  = hit(sa_cv_p, sa_cv_a)
        print(f"  [SA]  CV  RMSE={sa_cv_rmse:.4f}  MAE={sa_cv_mae:.4f}"
              f"  R²={sa_cv_r2:.4f}  Hit={sa_cv_hit:.1%}  (n={len(sa_cv_a)}, {time.time()-t0:.0f}s)")
    else:
        sa_cv_rmse = sa_cv_mae = sa_cv_r2 = sa_cv_hit = None
        print("  [SA]  CV  insufficient data")

    # ── SARIMA holdout ────────────────────────────────────────────────────────
    #  Parameters fit ONCE on all weekly returns strictly before HOLDOUT_START.
    #  For each holdout date t, condition on the ACTUAL observed weekly-return
    #  history r_1, ..., r_t (via statsmodels `apply(refit=False)`), then
    #  forecast r_{t+1}, ..., r_{t+steps} and sum.
    #
    #  This matches what CatBoost does: locked parameters from the training
    #  period, but fresh conditioning information at each prediction date.
    print(f"  [SA]  running holdout ...")
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

    sa_h_p = None
    try:
        sa_hld_model = _fit_sarima(weekly_ret_full.values)
        order        = f"SARIMA{sa_hld_model.order}x{sa_hld_model.seasonal_order}"
        sm_res       = sa_hld_model.arima_res_

        sa_h_p = np.zeros(len(hold_data))
        for j, t_date in enumerate(hold_data["Date"].values):
            # Target at t is Σ r_t, ..., r_{t+steps-1}
            # Condition on r_1, ..., r_{t-1} (strictly before t), forecast steps ahead
            mask_t  = ret_series_full["Date"] < t_date
            history = ret_series_full.loc[mask_t, WEEKLY_RET_COL].values
            applied = sm_res.apply(history, refit=False)
            fc      = applied.forecast(steps=steps)
            sa_h_p[j] = float(np.sum(fc))

        sa_h_a    = hold_data[target].values
        sa_h_rmse = rmse(sa_h_a, sa_h_p)
        sa_h_mae  = mae(sa_h_a, sa_h_p)
        sa_h_r2   = r2(sa_h_a, sa_h_p)
        sa_h_hit  = hit(sa_h_p, sa_h_a)
        print(f"  [SA]  Hld RMSE={sa_h_rmse:.4f}  MAE={sa_h_mae:.4f}"
              f"  R²={sa_h_r2:.4f}  Hit={sa_h_hit:.1%}  (n={len(sa_h_a)}, {order}, {time.time()-t0:.0f}s)")

    except Exception as e:
        sa_h_rmse = sa_h_mae = sa_h_r2 = sa_h_hit = None
        print(f"  [SA]  Hld FAILED: {e}")

    # ── Summary row ───────────────────────────────────────────────────────────
    def _r(x): return round(x, 4) if x is not None else None
    summary.append({
        "Target":       target,
        # Random walk baseline
        "RW CV RMSE":   _r(rw_cv_rmse),
        "RW CV R2":     _r(rw_cv_r2),
        "RW Hld RMSE":  _r(rw_hld_rmse),
        "RW Hld R2":    _r(rw_hld_r2),
        # CatBoost
        "CB CV RMSE":   _r(cb_cv_rmse),
        "CB CV MAE":    _r(cb_cv_mae),
        "CB CV R2":     _r(cb_cv_r2),
        "CB CV Hit":    _r(cb_cv_hit),
        "CB Hld RMSE":  _r(cb_h_rmse),
        "CB Hld MAE":   _r(cb_h_mae),
        "CB Hld R2":    _r(cb_h_r2),
        "CB Hld Hit":   _r(cb_h_hit),
        # SARIMA
        "SA CV RMSE":   _r(sa_cv_rmse),
        "SA CV MAE":    _r(sa_cv_mae),
        "SA CV R2":     _r(sa_cv_r2),
        "SA CV Hit":    _r(sa_cv_hit),
        "SA Hld RMSE":  _r(sa_h_rmse),
        "SA Hld MAE":   _r(sa_h_mae),
        "SA Hld R2":    _r(sa_h_r2),
        "SA Hld Hit":   _r(sa_h_hit),
    })

    # ── Plot holdout comparison ───────────────────────────────────────────────
    dates  = hold_data["Date"].values
    fig, axes = plt.subplots(2, 1, figsize=(14, 8))
    fig.suptitle(f"{target}  —  Holdout 2022–2025", fontsize=12)

    # Panel 1: predictions vs actuals
    ax = axes[0]
    ax.plot(dates, cb_h_a, label="Actual",      color="black", alpha=0.85, lw=1.5)
    ax.plot(dates, cb_h_p, label="CatBoost",    alpha=0.75, lw=1.2)
    if sa_h_p is not None:
        ax.plot(dates, sa_h_p, label="SARIMA",  alpha=0.75, lw=1.2)
    ax.axhline(0, color="grey", lw=0.8, linestyle="--", label="Random Walk (0)")
    ax.set_title("Predicted vs Actual  (log-return space)")
    ax.legend(); ax.grid(True, alpha=0.3)

    # Panel 2: cumulative prediction error
    ax = axes[1]
    ax.plot(dates, np.cumsum((cb_h_p - cb_h_a)**2), label="CatBoost SSE", alpha=0.8)
    if sa_h_p is not None:
        ax.plot(dates, np.cumsum((sa_h_p - sa_h_a)**2), label="SARIMA SSE",  alpha=0.8)
    ax.plot(dates, np.cumsum(cb_h_a**2), label="Random Walk SSE", linestyle="--",
            color="grey", alpha=0.8)
    ax.set_title("Cumulative Squared Error (lower = better)")
    ax.legend(); ax.grid(True, alpha=0.3)

    plt.tight_layout()
    safe = target.replace("/", "-").replace(" ", "_").replace("∆", "d")
    plt.savefig(f"Results/comparison_{safe}.png", dpi=120)
    plt.close()

## ── Summary table ────────────────────────────────────────────────────────────
print(f"\n{'='*65}")
print("SUMMARY  (log-return space — lower RMSE/MAE = better, higher R²/Hit = better)")
print(f"{'='*65}")
summary_df = pd.DataFrame(summary).set_index("Target")
print(summary_df.to_string())
summary_df.to_csv("Results/model_comparison_summary.csv")

## ── Skill scores vs Random Walk (holdout) ────────────────────────────────────
print(f"\n{'='*65}")
print("SKILL SCORES vs Random Walk  (holdout, RMSE reduction %)")
print(f"{'='*65}")
for row in summary:
    rw   = row["RW Hld RMSE"]
    cb   = row["CB Hld RMSE"]
    sa   = row.get("SA Hld RMSE")
    cb_s = (1 - cb / rw) * 100 if cb and rw else None
    sa_s = (1 - sa / rw) * 100 if sa and rw else None
    cb_str = f"{cb_s:+.1f}%" if cb_s is not None else "N/A"
    sa_str = f"{sa_s:+.1f}%" if sa_s is not None else "N/A"
    print(f"  {row['Target']:<40}  CB={cb_str}  SA={sa_str}")
