##
#  Forward price benchmark — OLS with β=1 test (Expectations Hypothesis)
#
#  For each horizon h, fits: ΔlnS = α + β·FWD_h + ε
#  Tests H0: β=1 (unbiased predictor, standard term structure literature test).
#  Uses same purged walk-forward CV structure as main model.
#  Holdout: 2022–2025.
##

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.metrics import mean_squared_error, r2_score
import statsmodels.api as sm
import os

## ── Load data ────────────────────────────────────────────────────────────────
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
df = df.sort_values("Date").reset_index(drop=True)

HOLDOUT_START = "2022-01-01"

os.makedirs("Results", exist_ok=True)

## ── Horizon config ───────────────────────────────────────────────────────────
HORIZONS = {
    "Y 1m ∆ Salmon (NOK/KG)":  {"purge_weeks":  4, "n_folds": 10, "fwd": "FWD 1m"},
    "Y 3m ∆ Salmon (NOK/KG)":  {"purge_weeks": 13, "n_folds":  8, "fwd": "FWD 3m"},
    "Y 6m ∆ Salmon (NOK/KG)":  {"purge_weeks": 26, "n_folds":  6, "fwd": "FWD 6m"},
    "Y 12m ∆ Salmon (NOK/KG)": {"purge_weeks": 52, "n_folds":  4, "fwd": "FWD 12m"},
}

## ── Diebold-Mariano test vs random walk ──────────────────────────────────────
def diebold_mariano(actual, pred_model):
    e_model = actual - pred_model
    e_rw    = actual
    d       = e_model**2 - e_rw**2
    n       = len(d)
    dm_stat = np.mean(d) / (np.std(d, ddof=1) / np.sqrt(n))
    p_value = 2 * stats.norm.sf(np.abs(dm_stat))
    return dm_stat, p_value

## ── Purged walk-forward CV (OLS) ─────────────────────────────────────────────
def purged_wf_cv_ols(data, target, fwd_feat, purge_weeks, n_folds):
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
        ols     = sm.OLS(train[target].values, X_train).fit()
        X_test  = sm.add_constant(test[fwd_feat].values, has_constant="add")
        preds   = ols.predict(X_test)

        results.append({
            "dates":   test["Date"].values,
            "actuals": test[target].values,
            "preds":   preds,
        })

    return results

## ── Run benchmark ────────────────────────────────────────────────────────────
summary = []

print("── OLS Forward Benchmark (Expectations Hypothesis) ─────────────────")

for target, cfg in HORIZONS.items():
    fwd_feat  = cfg["fwd"]
    purge_wks = cfg["purge_weeks"]
    n_folds   = cfg["n_folds"]

    if fwd_feat not in df.columns:
        print(f"[SKIP] {target} — {fwd_feat} not in data")
        continue

    cv_data   = df[df["Date"] < HOLDOUT_START].dropna(subset=[target, fwd_feat]).copy().reset_index(drop=True)
    hold_data = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target, fwd_feat]).copy().reset_index(drop=True)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    print(f"\n{target}  (OLS, fwd={fwd_feat}, purge={purge_wks}w, folds={n_folds})")

    ## ── Purged CV ────────────────────────────────────────────────────────────
    fold_results = purged_wf_cv_ols(cv_data, target, fwd_feat, purge_wks, n_folds)

    if fold_results:
        cv_preds   = np.concatenate([f["preds"]   for f in fold_results])
        cv_actuals = np.concatenate([f["actuals"] for f in fold_results])
        cv_rmse    = np.sqrt(mean_squared_error(cv_actuals, cv_preds))
        cv_rw_rmse = np.sqrt(np.mean(cv_actuals**2))
        cv_r2      = r2_score(cv_actuals, cv_preds)
        cv_hit     = np.mean(np.sign(cv_preds) == np.sign(cv_actuals))
        print(f"  CV       RMSE={cv_rmse:.4f}  RW_RMSE={cv_rw_rmse:.4f}  "
              f"R²={cv_r2:.4f}  Hit={cv_hit:.1%}  (n={len(cv_actuals)})")
    else:
        cv_rmse, cv_rw_rmse, cv_r2, cv_hit = None, None, None, None
        print("  CV       insufficient data")

    ## ── Final OLS on full CV period ──────────────────────────────────────────
    X_cv   = sm.add_constant(cv_data[fwd_feat].values)
    ols    = sm.OLS(cv_data[target].values, X_cv).fit()

    alpha_hat = ols.params[0]
    beta_hat  = ols.params[1]
    beta_se   = ols.bse[1]

    # β=1 t-test (Expectations Hypothesis: forward is unbiased predictor)
    t_beta1 = (beta_hat - 1) / beta_se
    p_beta1 = 2 * stats.t.sf(np.abs(t_beta1), df=ols.df_resid)

    print(f"  OLS      α={alpha_hat:.4f}  β={beta_hat:.4f}  SE(β)={beta_se:.4f}  "
          f"t(β=1)={t_beta1:.2f}  p(β=1)={p_beta1:.3f}")

    ## ── Holdout evaluation ───────────────────────────────────────────────────
    X_hold     = sm.add_constant(hold_data[fwd_feat].values, has_constant="add")
    hold_preds = ols.predict(X_hold)

    hold_actuals = hold_data[target].values
    hold_rmse    = np.sqrt(mean_squared_error(hold_actuals, hold_preds))
    hold_rw_rmse = np.sqrt(np.mean(hold_actuals**2))
    hold_r2      = r2_score(hold_actuals, hold_preds)
    hold_hit     = np.mean(np.sign(hold_preds) == np.sign(hold_actuals))
    dm_stat, dm_p = diebold_mariano(hold_actuals, hold_preds)

    print(f"  Holdout  RMSE={hold_rmse:.4f}  RW_RMSE={hold_rw_rmse:.4f}  "
          f"R²={hold_r2:.4f}  Hit={hold_hit:.1%}  DM={dm_stat:.2f}  p={dm_p:.3f}")

    summary.append({
        "Y":          target,
        "FWD":        fwd_feat,
        "α":          alpha_hat,
        "β":          beta_hat,
        "p(β=1)":     p_beta1,
        "CV RMSE":    cv_rmse,
        "CV RW RMSE": cv_rw_rmse,
        "CV R2":      cv_r2,
        "CV Hit":     cv_hit,
        "Hold RMSE":  hold_rmse,
        "Hold RW RMSE": hold_rw_rmse,
        "Hold R2":    hold_r2,
        "Hold Hit":   hold_hit,
        "Hold DM":    dm_stat,
        "Hold DM p":  dm_p,
    })

## ── Summary ──────────────────────────────────────────────────────────────────
print("\n── Summary ──────────────────────────────────────────────────────────")
summary_df = pd.DataFrame(summary).set_index("Y")
print(summary_df.to_string())
summary_df.to_csv("Results/fwd_benchmark_summary.csv")
