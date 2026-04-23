##
#  Lasso model — all Y horizons
#  Purged walk-forward CV; alpha selected by inner time-series CV (LassoCV)
#  Includes: random walk baseline, Diebold-Mariano test
#  Final holdout: 2022–2025
#
#  Features are standardised inside each fold (StandardScaler fit on train only)
#  to make the L1 penalty scale-invariant across heterogeneous regressors.
#
#  Note: Y 0w is a nowcast (S_t and F_t are Wednesday closes, simultaneously
#  determined). Results for Y 0w should be labelled nowcast, not forecast.
##

import pandas as pd
import numpy as np
import re, os
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, r2_score
from scipy import stats
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt

## ── Load data ────────────────────────────────────────────────────────────────
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
df = df.sort_values("Date").reset_index(drop=True)

HOLDOUT_START = "2022-01-01"

Y_COLS   = [c for c in df.columns if c.startswith("Y ")]
NON_FEAT = {"Date"} | set(Y_COLS)
ALL_FEAT = [c for c in df.columns if c not in NON_FEAT]

RESULTS_DIR = "Results/Lasso"
os.makedirs(RESULTS_DIR, exist_ok=True)

## ── Horizon config ───────────────────────────────────────────────────────────
HORIZON_CONFIG = {
    "0w":  {"purge_weeks":  0, "n_folds": 10},
    "1w":  {"purge_weeks":  1, "n_folds": 10},
    "2w":  {"purge_weeks":  2, "n_folds": 10},
    "1m":  {"purge_weeks":  4, "n_folds": 10},
    "3m":  {"purge_weeks": 13, "n_folds":  8},
    "6m":  {"purge_weeks": 26, "n_folds":  6},
    "12m": {"purge_weeks": 52, "n_folds":  4},
}

## ── LassoCV settings ─────────────────────────────────────────────────────────
#  Alpha grid: 100 log-spaced values from 1e-4 to 1e1.
#  TimeSeriesSplit(5) respects temporal order within the training window so
#  future training observations are never used to select alpha.
ALPHAS      = np.logspace(-4, 1, 100)
INNER_CV    = TimeSeriesSplit(n_splits=5)
MAX_ITER    = 10_000

def parse_horizon(target):
    m = re.search(r"Y (\d+(?:w|m)) ", target)
    return m.group(1) if m else "0w"

## ── Diebold-Mariano test (vs random walk: predict 0) ─────────────────────────
def diebold_mariano(actual, pred_model):
    e_model = actual - pred_model
    e_rw    = actual                   # random walk predicts 0 → error = actual
    d       = e_model**2 - e_rw**2
    n       = len(d)
    dm_stat = np.mean(d) / (np.std(d, ddof=1) / np.sqrt(n))
    p_value = 2 * stats.norm.sf(np.abs(dm_stat))
    return dm_stat, p_value

## ── Purged walk-forward CV ───────────────────────────────────────────────────
def purged_wf_cv(data, target, features, purge_weeks, n_folds):
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

        X_tr = train[features].values
        y_tr = train[target].values
        X_te = test[features].values

        # Drop columns that are all-NaN in this train window
        # Impute remaining NaN with column training mean so that after StandardScaler
        # missing values map to 0 (the scaled mean) — neutral, no spurious signal
        valid_cols  = ~np.all(np.isnan(X_tr), axis=0)
        X_tr        = X_tr[:, valid_cols]
        X_te        = X_te[:, valid_cols]
        col_means   = np.nanmean(X_tr, axis=0)
        X_tr        = np.where(np.isnan(X_tr), col_means, X_tr)
        X_te        = np.where(np.isnan(X_te), col_means, X_te)

        scaler = StandardScaler()
        X_tr   = scaler.fit_transform(X_tr)
        X_te   = scaler.transform(X_te)

        model  = LassoCV(alphas=ALPHAS, cv=INNER_CV, max_iter=MAX_ITER)
        model.fit(X_tr, y_tr)
        preds = model.predict(X_te)

        results.append({
            "dates":      test["Date"].values,
            "actuals":    test[target].values,
            "preds":      preds,
            "fold":       i,
            "best_alpha": model.alpha_,
            "n_nonzero":  int(np.sum(model.coef_ != 0)),
        })

    return results

## ── Run per Y horizon ────────────────────────────────────────────────────────
summary = []

for target in Y_COLS:
    horizon    = parse_horizon(target)
    cfg        = HORIZON_CONFIG.get(horizon, {"purge_weeks": 0, "n_folds": 10})
    purge_wks  = cfg["purge_weeks"]
    n_folds    = cfg["n_folds"]
    is_nowcast = (horizon == "0w")

    cv_data   = df[df["Date"] < HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)
    hold_data = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    label = f"{target}  [NOWCAST]" if is_nowcast else target
    print(f"\n{label}  (horizon={horizon}, purge={purge_wks}w, folds={n_folds})")

    ## ── Purged CV ────────────────────────────────────────────────────────────
    fold_results = purged_wf_cv(cv_data, target, ALL_FEAT, purge_wks, n_folds)

    if fold_results:
        cv_preds    = np.concatenate([f["preds"]   for f in fold_results])
        cv_actuals  = np.concatenate([f["actuals"] for f in fold_results])
        cv_dates    = np.concatenate([f["dates"]   for f in fold_results])
        cv_alphas   = [f["best_alpha"] for f in fold_results]
        cv_nonzeros = [f["n_nonzero"]  for f in fold_results]

        cv_rmse    = np.sqrt(mean_squared_error(cv_actuals, cv_preds))
        cv_rw_rmse = np.sqrt(np.mean(cv_actuals**2))
        cv_r2      = r2_score(cv_actuals, cv_preds)
        cv_hitrate = np.mean(np.sign(cv_preds) == np.sign(cv_actuals))

        print(f"  CV       RMSE={cv_rmse:.4f}  RW_RMSE={cv_rw_rmse:.4f}  "
              f"R²={cv_r2:.4f}  Hit={cv_hitrate:.1%}  "
              f"(n_obs={len(cv_actuals)}, folds={len(fold_results)})")
        print(f"           alpha range [{min(cv_alphas):.5f}, {max(cv_alphas):.5f}]  "
              f"nonzero coefs {min(cv_nonzeros)}–{max(cv_nonzeros)}")
    else:
        cv_rmse = cv_rw_rmse = cv_r2 = cv_hitrate = None
        cv_preds = cv_actuals = cv_dates = []
        cv_alphas = cv_nonzeros = []
        print("  CV       insufficient data for all folds")

    ## ── Final model — trained on full CV period ──────────────────────────────
    X_cv = cv_data[ALL_FEAT].values
    y_cv = cv_data[target].values

    valid_cols_final  = ~np.all(np.isnan(X_cv), axis=0)
    X_cv_v            = X_cv[:, valid_cols_final]
    col_means_final   = np.nanmean(X_cv_v, axis=0)
    X_cv_clean        = np.where(np.isnan(X_cv_v), col_means_final, X_cv_v)

    X_hold     = hold_data[ALL_FEAT].values
    X_hold_v   = X_hold[:, valid_cols_final]
    X_hold_clean = np.where(np.isnan(X_hold_v), col_means_final, X_hold_v)

    scaler_final = StandardScaler()
    X_cv_sc      = scaler_final.fit_transform(X_cv_clean)
    X_hold_sc    = scaler_final.transform(X_hold_clean)

    final_model = LassoCV(alphas=ALPHAS, cv=INNER_CV, max_iter=MAX_ITER)
    final_model.fit(X_cv_sc, y_cv)

    hold_preds   = final_model.predict(X_hold_sc)
    hold_actuals = hold_data[target].values
    hold_rmse    = np.sqrt(mean_squared_error(hold_actuals, hold_preds))
    hold_rw_rmse = np.sqrt(np.mean(hold_actuals**2))
    hold_r2      = r2_score(hold_actuals, hold_preds)
    hold_hitrate = np.mean(np.sign(hold_preds) == np.sign(hold_actuals))
    dm_stat, dm_p = diebold_mariano(hold_actuals, hold_preds)

    n_nonzero_final = int(np.sum(final_model.coef_ != 0))
    best_alpha      = final_model.alpha_

    print(f"  Holdout  RMSE={hold_rmse:.4f}  RW_RMSE={hold_rw_rmse:.4f}  "
          f"R²={hold_r2:.4f}  Hit={hold_hitrate:.1%}  "
          f"DM={dm_stat:.2f}  p={dm_p:.3f}")
    print(f"           alpha={best_alpha:.5f}  nonzero={n_nonzero_final}/{len(final_model.coef_)}")

    summary.append({
        "Y":              target,
        "Horizon":        horizon,
        "Nowcast":        is_nowcast,
        "CV RMSE":        cv_rmse,
        "CV RW RMSE":     cv_rw_rmse,
        "CV R2":          cv_r2,
        "CV Hit":         cv_hitrate,
        "Hold RMSE":      hold_rmse,
        "Hold RW RMSE":   hold_rw_rmse,
        "Hold R2":        hold_r2,
        "Hold Hit":       hold_hitrate,
        "Hold DM":        dm_stat,
        "Hold DM p":      dm_p,
        "Best Alpha":     best_alpha,
        "Nonzero Coefs":  n_nonzero_final,
        "Total Features": len(final_model.coef_),
        "n_train":        len(cv_data),
        "n_holdout":      len(hold_data),
    })

    ## ── Coefficient plot (non-zero only) ─────────────────────────────────────
    feat_names   = np.array(ALL_FEAT)[valid_cols_final]
    coef_series  = pd.Series(final_model.coef_, index=feat_names)
    nonzero_coef = coef_series[coef_series != 0].sort_values()

    n_axes = 3 if len(cv_preds) > 0 else 2
    fig, axes = plt.subplots(n_axes, 1, figsize=(14, 5 * n_axes))
    fig.suptitle(f"{target}{' [NOWCAST]' if is_nowcast else ''}", fontsize=13)

    ax_idx = 0
    if len(cv_preds) > 0:
        axes[ax_idx].plot(cv_dates, cv_actuals, label="Actual",    alpha=0.8)
        axes[ax_idx].plot(cv_dates, cv_preds,   label="Predicted", alpha=0.8)
        axes[ax_idx].set_title(
            f"Purged CV  (RMSE={cv_rmse:.4f}, RW={cv_rw_rmse:.4f}, R²={cv_r2:.4f})")
        axes[ax_idx].legend(); axes[ax_idx].grid(True, alpha=0.3)
        ax_idx += 1

    axes[ax_idx].plot(hold_data["Date"].values, hold_actuals, label="Actual",    alpha=0.8)
    axes[ax_idx].plot(hold_data["Date"].values, hold_preds,   label="Predicted", alpha=0.8)
    axes[ax_idx].set_title(
        f"Holdout 2022–2025  (RMSE={hold_rmse:.4f}, RW={hold_rw_rmse:.4f}, "
        f"R²={hold_r2:.4f}, DM p={dm_p:.3f}, α={best_alpha:.5f})")
    axes[ax_idx].legend(); axes[ax_idx].grid(True, alpha=0.3)
    ax_idx += 1

    if len(nonzero_coef) > 0:
        nonzero_coef.tail(30).plot(kind="barh", ax=axes[ax_idx])  # top 30 by magnitude
        axes[ax_idx].set_title(
            f"Non-zero Lasso coefficients  ({n_nonzero_final} selected, α={best_alpha:.5f})")
        axes[ax_idx].axvline(0, color="black", lw=0.8)
        axes[ax_idx].grid(True, alpha=0.3)
    else:
        axes[ax_idx].text(0.5, 0.5, "All coefficients zeroed out",
                          ha="center", va="center", transform=axes[ax_idx].transAxes)

    plt.tight_layout()
    safe_name = target.replace("/", "-").replace(" ", "_").replace("∆", "d")
    plt.savefig(f"{RESULTS_DIR}/lasso_{safe_name}.pdf", format="pdf", bbox_inches="tight")
    plt.close()

## ── Summary table ────────────────────────────────────────────────────────────
print("\n── Summary ──────────────────────────────────────────────────────────")
summary_df = pd.DataFrame(summary).set_index("Y")
print(summary_df.to_string())
summary_df.to_csv(f"{RESULTS_DIR}/lasso_summary.csv")

## ── PDF results table ────────────────────────────────────────────────────────
def _fmt(x, fmt=".4f"):
    return f"{x:{fmt}}" if pd.notna(x) else "—"

disp = pd.DataFrame({
    "Horizon":    [r["Horizon"] for r in summary],
    "CV RMSE":    [_fmt(r["CV RMSE"]) for r in summary],
    "CV R²":      [_fmt(r["CV R2"], ".3f") for r in summary],
    "CV Hit":     [f'{r["CV Hit"]:.1%}' if r["CV Hit"] else "—" for r in summary],
    "Hold RMSE":  [_fmt(r["Hold RMSE"]) for r in summary],
    "Hold R²":    [_fmt(r["Hold R2"], ".3f") for r in summary],
    "Hold Hit":   [f'{r["Hold Hit"]:.1%}' if r["Hold Hit"] else "—" for r in summary],
    "RW RMSE":    [_fmt(r["Hold RW RMSE"]) for r in summary],
    "Skill %":    [f'{(1 - r["Hold RMSE"]/r["Hold RW RMSE"])*100:+.1f}%' for r in summary],
    "DM":         [_fmt(r["Hold DM"], ".2f") for r in summary],
    "p-value":    [f'{r["Hold DM p"]:.4f}' if r["Hold DM p"] >= 0.001
                   else "< 0.001" for r in summary],
    "α":          [f'{r["Best Alpha"]:.5f}' for r in summary],
    "Nonzero":    [f'{r["Nonzero Coefs"]}/{r["Total Features"]}' for r in summary],
})

fig, ax = plt.subplots(figsize=(18, len(disp) * 0.55 + 2.5))
ax.axis("off")
ax.set_title("Lasso — Results Summary\nHoldout: 2022–2025  |  Purged Walk-Forward CV  |  LassoCV (TimeSeriesSplit)",
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

plt.savefig(f"{RESULTS_DIR}/lasso_results.pdf", format="pdf", bbox_inches="tight", dpi=150)
plt.close()
print(f"PDF saved → {RESULTS_DIR}/lasso_results.pdf")
