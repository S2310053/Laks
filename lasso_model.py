# This module constructs the LASSO regression model

##
# We construct the Lasso model for all horizons. We considered using normal OLS, where we'd choose feature based on economic theory
# In favour of this, and as a important part of our thesis (where we want to analyse how good machine algoritms can be at forecasting salmon prices),
# we decided to use the Lasso regression, which can perform automatic feature selection by shrinking coefficients to zero
# We believe that if we had done this instead, it would both be time-consuming and include subjectivity in selection process, which can lead to biases in results
# This way the data speak for itself, and performance is based on how good the algoritims can select relevant features
# We use standard L1 penalty, and select the regularisation strength (alpha) using nested cross-validation within the training set to avoid look-ahead bias
##

import pandas as pd
import numpy as np
import re, os
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit
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
_RED  = "#C4654A"
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
from metrics import Metrics
metrics = Metrics()
from plotter import Plotter

# Load data
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])

HOLDOUT_START = "2022-01-01"

# Identify Y columns and features
Y_COLS   = [c for c in df.columns if c.startswith("Y ")]
NON_FEAT = {"Date"} | set(Y_COLS)
ALL_FEAT = [c for c in df.columns if c not in NON_FEAT]

# Create folder to store results
RESULTS_DIR = "Results/Lasso"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Same structure as OLS to get results comparison with CatBoost for CV splits, as well as final holdout period
HORIZON_CONFIG = {
    "0w":  {"purge_weeks":  0, "n_folds": 10},
    "1w":  {"purge_weeks":  1, "n_folds": 10},
    "2w":  {"purge_weeks":  2, "n_folds": 10},
    "1m":  {"purge_weeks":  4, "n_folds": 10},
    "3m":  {"purge_weeks": 13, "n_folds":  8},
    "6m":  {"purge_weeks": 26, "n_folds":  6},
    "12m": {"purge_weeks": 52, "n_folds":  4},
}

# Lasso settings 
# TimeSeriesSplit(5) respects temporal order within the training window so
# future training observations are never used to select alpha
# Note: Setings can be adjusted if seen fit
ALPHAS      = np.logspace(-4, 1, 100)
INNER_CV    = TimeSeriesSplit(n_splits=5)
MAX_ITER    = 10_000

def parse_horizon(target):
    m = re.search(r"Y (\d+(?:w|m)) ", target)
    return m.group(1) if m else "0w"

# Purged k-fold CV
def purged_cv(data, target, features, purge_weeks, n_folds):
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

#Run model for all horizons
summary = []

for target in Y_COLS:
    horizon    = parse_horizon(target)
    cfg        = HORIZON_CONFIG.get(horizon)
    if cfg is None:
        continue
    purge_wks  = cfg["purge_weeks"]
    n_folds    = cfg["n_folds"]
    is_nowcast = (horizon == "0w")

    cv_data   = df[df["Date"] < HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)
    hold_data = df[df["Date"] >= HOLDOUT_START].dropna(subset=[target]).copy().reset_index(drop=True)

    # Embargo: final model training ends purge_wks before holdout so no training target overlaps holdout returns
    embargo_date = pd.Timestamp(HOLDOUT_START) - pd.Timedelta(weeks=purge_wks)

    if len(cv_data) < 100 or len(hold_data) == 0:
        print(f"[SKIP] {target}")
        continue

    label = f"{target}  [NOWCAST]" if is_nowcast else target
    print(f"\n{label}  (horizon={horizon}, purge={purge_wks}w, folds={n_folds})")

    # Performance evaluation on Purged CV splits
    fold_results = purged_cv(cv_data, target, ALL_FEAT, purge_wks, n_folds)

    if fold_results:
        cv_preds    = np.concatenate([f["preds"]   for f in fold_results])
        cv_actuals  = np.concatenate([f["actuals"] for f in fold_results])
        cv_dates    = np.concatenate([f["dates"]   for f in fold_results])
        cv_alphas   = [f["best_alpha"] for f in fold_results]
        cv_nonzeros = [f["n_nonzero"]  for f in fold_results]

        cv_rmse    = metrics.rmse(cv_actuals, cv_preds)
        cv_rw_rmse = metrics.rw_rmse(cv_actuals)
        cv_r2      = metrics.r2(cv_actuals, cv_preds)
        cv_hitrate = metrics.hit_rate(cv_actuals, cv_preds)

        print(f"CV RMSE={cv_rmse:.4f}  RW_RMSE={cv_rw_rmse:.4f}  "
              f"R²={cv_r2:.4f}  Hit={cv_hitrate:.1%}  "
              f"(n_obs={len(cv_actuals)}, folds={len(fold_results)})")
        print(f"alpha range [{min(cv_alphas):.5f}, {max(cv_alphas):.5f}]  "
              f"nonzero coefs {min(cv_nonzeros)}–{max(cv_nonzeros)}")
    else:
        cv_rmse = cv_rw_rmse = cv_r2 = cv_hitrate = None
        cv_preds = cv_actuals = cv_dates = []
        cv_alphas = cv_nonzeros = []
        print("CV: insufficient data for all folds")

    # Final lasso on embargoed training set (no target overlap with holdout)
    train_final = cv_data[cv_data["Date"] < embargo_date].copy()
    X_cv = train_final[ALL_FEAT].values
    y_cv = train_final[target].values

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

    # Holdout period performance
    hold_preds   = final_model.predict(X_hold_sc)
    hold_actuals = hold_data[target].values
    hold_rmse    = metrics.rmse(hold_actuals, hold_preds)
    hold_rw_rmse = metrics.rw_rmse(hold_actuals)
    hold_r2      = metrics.r2(hold_actuals, hold_preds)
    hold_hitrate = metrics.hit_rate(hold_actuals, hold_preds)
    dm_stat, dm_p = metrics.diebold_mariano(hold_actuals, hold_preds, horizon=max(purge_wks, 1))

    n_nonzero_final = int(np.sum(final_model.coef_ != 0))
    best_alpha      = final_model.alpha_

    print(f"Holdout RMSE={hold_rmse:.4f}  RW_RMSE={hold_rw_rmse:.4f}  "
          f"R²={hold_r2:.4f}  Hit={hold_hitrate:.1%}  "
          f"DM={dm_stat:.2f}  p={dm_p:.3f}")
    print(f"alpha={best_alpha:.5f}  nonzero={n_nonzero_final}/{len(final_model.coef_)}")

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

    # Plot (non-zero only)
    feat_names   = np.array(ALL_FEAT)[valid_cols_final]
    coef_series  = pd.Series(final_model.coef_, index=feat_names)
    nonzero_coef = coef_series[coef_series != 0].sort_values()

    n_axes = 3 if len(cv_preds) > 0 else 2
    fig, axes = plt.subplots(n_axes, 1, figsize=(14, 5 * n_axes), facecolor="white")
    fig.suptitle(f"{target}{'[NOWCAST]' if is_nowcast else ''}",
                 fontsize=13, fontweight="bold")

    ax_idx = 0
    if len(cv_preds) > 0:
        axes[ax_idx].plot(cv_dates, cv_actuals, label="Actual",    color=_DARK, lw=1.5, alpha=0.85)
        axes[ax_idx].plot(cv_dates, cv_preds,   label="Predicted", color=_BLUE, lw=1.2, alpha=0.85)
        axes[ax_idx].axhline(0, color=_GREY, lw=0.8, ls="--")
        axes[ax_idx].set_title(f"Purged CV — RMSE={cv_rmse:.4f} | RW={cv_rw_rmse:.4f} | R²={cv_r2:.4f}",
                               fontsize=10)
        axes[ax_idx].legend(frameon=False, fontsize=9)
        _style_ax(axes[ax_idx], ylabel="∆ Price (NOK/kg)")
        ax_idx += 1

    axes[ax_idx].plot(hold_data["Date"].values, hold_actuals, label="Actual",    color=_DARK, lw=1.5, alpha=0.85)
    axes[ax_idx].plot(hold_data["Date"].values, hold_preds,   label="Predicted", color=_BLUE, lw=1.2, alpha=0.85)
    axes[ax_idx].axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk")
    axes[ax_idx].set_title(f"Holdout 2022–2025 — RMSE={hold_rmse:.4f}  |  RW={hold_rw_rmse:.4f}  |  "
                           f"R²={hold_r2:.4f} | DM p={dm_p:.3f} | α={best_alpha:.5f}", fontsize=10)
    axes[ax_idx].legend(frameon=False, fontsize=9)
    _style_ax(axes[ax_idx], ylabel="∆ Price (NOK/kg)")
    ax_idx += 1

    if len(nonzero_coef) > 0:
        coef30 = nonzero_coef.tail(30)
        bar_colors = [_BLUE if v > 0 else _RED for v in coef30.values]
        axes[ax_idx].barh(coef30.index, coef30.values, color=bar_colors, alpha=0.85)
        axes[ax_idx].axvline(0, color=_GREY, lw=0.8)
        axes[ax_idx].set_title(
            f"Non-zero Lasso Coefficients  ({n_nonzero_final} selected,  α={best_alpha:.5f})",
            fontsize=10)
        axes[ax_idx].set_xlabel("Coefficient", fontweight="bold")
    else:
        axes[ax_idx].text(0.5, 0.5, "All coefficients zeroed out",
                          ha="center", va="center", transform=axes[ax_idx].transAxes)
    axes[ax_idx].spines["top"].set_visible(False)
    axes[ax_idx].spines["right"].set_visible(False)
    axes[ax_idx].grid(True, linestyle="--", alpha=0.1)

    plt.tight_layout()
    safe_name = target.replace("/", "-").replace(" ", "_").replace("∆", "d")
    plt.savefig(f"{RESULTS_DIR}/lasso_{safe_name}.pdf", format="pdf", bbox_inches="tight")
    plt.close()

# Summary table
print("\n Summary of Lasso results")
summary_df = pd.DataFrame(summary).set_index("Y")
print(summary_df.to_string())
summary_df.to_csv(f"{RESULTS_DIR}/lasso_summary.csv")

# PDF results table
disp = pd.DataFrame({
    "Horizon":    [r["Horizon"] for r in summary],
    "CV RMSE":    [metrics.fmt(r["CV RMSE"]) for r in summary],
    "CV R²":      [metrics.fmt(r["CV R2"], ".3f") for r in summary],
    "CV Hit":     [f'{r["CV Hit"]:.1%}' if r["CV Hit"] else "—" for r in summary],
    "Hold RMSE":  [metrics.fmt(r["Hold RMSE"]) for r in summary],
    "Hold R²":    [metrics.fmt(r["Hold R2"], ".3f") for r in summary],
    "Hold Hit":   [f'{r["Hold Hit"]:.1%}' if r["Hold Hit"] else "—" for r in summary],
    "RW RMSE":    [metrics.fmt(r["Hold RW RMSE"]) for r in summary],
    "Skill %":    [f'{(1 - r["Hold RMSE"]/r["Hold RW RMSE"])*100:+.1f}%' for r in summary],
    "DM":         [metrics.fmt(r["Hold DM"], ".2f") for r in summary],
    "p-value":    [f'{r["Hold DM p"]:.4f}' if r["Hold DM p"] >= 0.001
                   else "< 0.001" for r in summary],
    "α":          [f'{r["Best Alpha"]:.5f}' for r in summary],
    "Nonzero":    [f'{r["Nonzero Coefs"]}/{r["Total Features"]}' for r in summary],
})

Plotter().results_table(
    disp,
    "Lasso — Results Summary\nHoldout: 2022–2025 | Purged CV | LassoCV",
    f"{RESULTS_DIR}/lasso_results.pdf",
    width=18,
)
