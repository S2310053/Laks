# This module is made for model comparison 

##
# All other scripts must be run first to generate the necessary results files in the Results/ folder
# This script only reads those saved results and compares them — it does not re-estimate any models
# That way models can be run independently, and this comparison can be run quickly after all models have been estimated
# Because some of the models can take a bit of time to run
##

import pandas as pd
import numpy as np
import os
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from metrics import Metrics
metrics = Metrics()
from plotter import Plotter

# Create folder to store results
RESULTS_DIR = "Results/Comparison"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Load saved results (each model needs to be run first to generate these)
MODELS = {
    "CatBoost": "Results/CatBoost/catboost_summary.csv",
    "XGBoost":  "Results/XGBoost/xgboost_summary.csv",
    "HTBoost":  "Results/HTBoost/htboost_summary.csv",
    "Lasso":    "Results/Lasso/lasso_summary.csv",
    "SARIMA":   "Results/SARIMA/sarima_summary.csv",
    "OLS":      "Results/OLS/ols_summary.csv",
}

loaded = {}
for name, path in MODELS.items():
    if os.path.exists(path):
        df = pd.read_csv(path)
        if "Horizon" not in df.columns and "Y" in df.columns:
            import re
            df["Horizon"] = df["Y"].apply(
                lambda x: re.search(r"Y (\d+\w)", x).group(1) if re.search(r"Y (\d+\w)", x) else "?")
        loaded[name] = df
        print(f"  ✓ {name:<10} loaded ({len(df)} horizons) ← {path}")
    else:
        print(f"  ✗ {name:<10} NOT FOUND — run {name.lower()}_model.py first")

if len(loaded) == 0:
    print("\nNo results found. Run individual model scripts first")
    exit()

# Define horizon order 
HORIZON_ORDER = ["0w", "1w", "2w", "1m", "3m", "6m", "12m"]

# Build comparison table
# For each horizon of each model it extracts: CV R², Hold R², Hold Hit, DM p
rows = []
for hz in HORIZON_ORDER:
    row = {"Horizon": hz}
    for name, df in loaded.items():
        mask = df["Horizon"] == hz
        if mask.sum() == 0:
            row[f"{name} CV R²"]  = None
            row[f"{name} R²"]     = None
            row[f"{name} Hit"]    = None
            row[f"{name} DM p"]   = None
            continue
        r = df[mask].iloc[0]
        cv_r2     = r.get("CV R2", r.get("CV R²", None))
        hold_r2   = r.get("Hold R2", r.get("Hold R²", None))
        hold_hit  = r.get("Hold Hit", None)
        dm_p      = r.get("Hold DM p", None)

        row[f"{name} CV R²"]  = cv_r2
        row[f"{name} R²"]     = hold_r2
        row[f"{name} Hit"]    = hold_hit
        row[f"{name} DM p"]   = dm_p
    rows.append(row)

comp_df = pd.DataFrame(rows)
print("\nComparison Table (Holdout period)")
print(comp_df.to_string(index=False))
comp_df.to_csv(f"{RESULTS_DIR}/comparison_summary.csv", index=False)

# PDF with combined results table 
def _fmt_pct(x):
    if pd.isna(x) or x is None:
        return "—"
    return f"{x:.1%}"

model_names = list(loaded.keys())

# Build display table: CV R² and Hold R² side by side per model, plus Hit
disp_data = {"Horizon": [r["Horizon"] for r in rows]}

for name in model_names:
    disp_data[f"{name} CV R²"]   = [metrics.fmt(r.get(f"{name} CV R²"), ".3f") for r in rows]
    disp_data[f"{name} Hold R²"] = [metrics.fmt(r.get(f"{name} R²"), ".3f") for r in rows]
    disp_data[f"{name} Hit"]     = [_fmt_pct(r.get(f"{name} Hit")) for r in rows]

disp = pd.DataFrame(disp_data)

Plotter().results_table(
    disp,
    "Model Comparison — CV vs Holdout 2022–2025\nR² and Hit Rate",
    f"{RESULTS_DIR}/comparison_table.pdf",
    width=max(18, len(disp.columns) * 1.5),
)
print()

# Bar chart: Holdout R² by horizon 
fig, axes = plt.subplots(3, 1, figsize=(14, 16))
fig.suptitle("Model Comparison — Holdout 2022–2025", fontsize=14, fontweight="bold")

COLORS = {
    "CatBoost": "#1f77b4",
    "XGBoost":  "#17becf",
    "HTBoost":  "#9467bd",
    "Lasso":    "#ff7f0e",
    "SARIMA":   "#2ca02c",
    "OLS":      "#d62728",
}

# Panel 1: R²
ax = axes[0]
x = np.arange(len(HORIZON_ORDER))
width = 0.8 / max(len(model_names), 1)
for k, name in enumerate(model_names):
    vals = []
    for hz in HORIZON_ORDER:
        match = [r for r in rows if r["Horizon"] == hz]
        v = match[0].get(f"{name} R²") if match else None
        vals.append(v if pd.notna(v) else 0)
    ax.bar(x + k * width, vals, width, label=name, color=COLORS.get(name, f"C{k}"), alpha=0.85)
ax.set_xticks(x + width * (len(model_names) - 1) / 2)
ax.set_xticklabels(HORIZON_ORDER)
ax.set_ylabel("R²")
ax.set_title("Holdout R²  (higher = better)")
ax.axhline(0, color="black", lw=0.8)
ax.legend()
ax.grid(True, alpha=0.3, axis="y")

# Panel 2: Hit Rate
ax = axes[1]
for k, name in enumerate(model_names):
    vals = []
    for hz in HORIZON_ORDER:
        match = [r for r in rows if r["Horizon"] == hz]
        v = match[0].get(f"{name} Hit") if match else None
        vals.append(v * 100 if pd.notna(v) and v is not None else 0)
    ax.bar(x + k * width, vals, width, label=name, color=COLORS.get(name, f"C{k}"), alpha=0.85)
ax.axhline(50, color="grey", lw=1, ls="--", label="Random Walk (50%)")
ax.set_xticks(x + width * (len(model_names) - 1) / 2)
ax.set_xticklabels(HORIZON_ORDER)
ax.set_ylabel("Hit Rate (%)")
ax.set_title("Holdout Hit Rate  (higher = better, RW = 50%)")
ax.legend()
ax.grid(True, alpha=0.3, axis="y")

# Panel 3: Skill vs RW
ax = axes[2]
for k, name in enumerate(model_names):
    vals = []
    for hz in HORIZON_ORDER:
        match = [r for r in rows if r["Horizon"] == hz]
        v = match[0].get(f"{name} Skill") if match else None
        vals.append(v if pd.notna(v) and v is not None else 0)
    ax.bar(x + k * width, vals, width, label=name, color=COLORS.get(name, f"C{k}"), alpha=0.85)
ax.axhline(0, color="black", lw=0.8)
ax.set_xticks(x + width * (len(model_names) - 1) / 2)
ax.set_xticklabels(HORIZON_ORDER)
ax.set_ylabel("Skill (%)")
ax.set_title("RMSE Skill vs Random Walk  (higher = better, 0% = RW)")
ax.legend()
ax.grid(True, alpha=0.3, axis="y")

plt.tight_layout()
plt.savefig(f"{RESULTS_DIR}/comparison_charts.pdf", format="pdf", bbox_inches="tight")
plt.close()
print(f"PDF saved → {RESULTS_DIR}/comparison_charts.pdf")

# Detailed table: Best model per horizon
print("\n── Best Model per Horizon (Holdout R²)")
for hz in HORIZON_ORDER:
    best_name = "—"
    best_r2   = -np.inf
    for name in model_names:
        match = [r for r in rows if r["Horizon"] == hz]
        if not match:
            continue
        v = match[0].get(f"{name} R²")
        if pd.notna(v) and v is not None and v > best_r2:
            best_r2   = v
            best_name = name
    if best_r2 > -np.inf:
        print(f"  {hz:>4}:  {best_name:<10}  R²={best_r2:.3f}")
    else:
        print(f"  {hz:>4}:  no data")

# Time-series comparison plots per horizon
# Loads holdout predictions saved by each model script
# Panel 1: Predicted vs Actual (all models + RW)
# Panel 2: Cumulative Squared Error (all models + RW)

import matplotlib.dates as mdates
import re

plt.rcParams["font.family"]      = "Times New Roman"
plt.rcParams["mathtext.fontset"] = "custom"
plt.rcParams["mathtext.rm"]      = "Times New Roman"
plt.rcParams["mathtext.it"]      = "Times New Roman:italic"
plt.rcParams["mathtext.bf"]      = "Times New Roman:bold"

_DARK = "#0D3B5E"
_GREY = "dimgrey"

def _style_ax(ax, ylabel=""):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, linestyle="--", alpha=0.1)
    if ylabel:
        ax.set_ylabel(ylabel, fontweight="bold")
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))

# Map model names to their prediction file directories
PRED_DIRS = {
    "CatBoost": "Results/CatBoost",
    "XGBoost":  "Results/XGBoost",
    "HTBoost":  "Results/HTBoost",
    "Lasso":    "Results/Lasso",
    "SARIMA":   "Results/SARIMA",
    "OLS":      "Results/OLS",
}

# Identify all Y targets from the loaded summaries
all_targets = set()
for name, df_model in loaded.items():
    all_targets.update(df_model["Y"].values)

for target in sorted(all_targets):
    safe_name = target.replace("/", "-").replace(" ", "_")
    hz_match  = re.search(r"Y (\d+\w)", target)
    hz_label  = hz_match.group(1) if hz_match else "?"

    # Load holdout predictions from each model
    model_preds = {}
    actuals     = None
    dates       = None

    for model_name, pred_dir in PRED_DIRS.items():
        pred_file = f"{pred_dir}/holdout_preds_{safe_name}.csv"
        if os.path.exists(pred_file):
            pdf = pd.read_csv(pred_file, parse_dates=["Date"])
            model_preds[model_name] = pdf["Predicted"].values
            if actuals is None:
                actuals = pdf["Actual"].values
                dates   = pdf["Date"].values

    if actuals is None or len(model_preds) == 0:
        continue

    print(f"  Comparison plot: {target} ({len(model_preds)} models)")

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), facecolor="white")
    fig.suptitle(f"{target}  —  Holdout 2022–2025",
                 fontsize=13, fontweight="bold")

    # Panel 1: Predicted vs Actual
    ax = axes[0]
    ax.plot(dates, actuals, label="Actual", color=_DARK, lw=1.8, alpha=0.9)
    for model_name, preds in model_preds.items():
        ax.plot(dates, preds, label=model_name,
                color=COLORS.get(model_name, "C0"), lw=1.2, alpha=0.85)
    ax.axhline(0, color=_GREY, lw=0.8, ls="--", label="Random Walk (0)")
    ax.set_title("Predicted vs Actual  (log-return space)", fontsize=10)
    ax.legend(frameon=False, fontsize=9)
    _style_ax(ax, ylabel="∆ Price (NOK/kg)")

    # Panel 2: Cumulative Squared Error
    ax = axes[1]
    rw_sse = np.cumsum(actuals ** 2)
    ax.plot(dates, rw_sse, label="Random Walk SSE",
            color=_GREY, lw=1.2, ls="--", alpha=0.8)
    for model_name, preds in model_preds.items():
        sse = np.cumsum((actuals - preds) ** 2)
        ax.plot(dates, sse, label=f"{model_name} SSE",
                color=COLORS.get(model_name, "C0"), lw=1.2, alpha=0.85)
    ax.set_title("Cumulative Squared Error  (lower = better)", fontsize=10)
    ax.legend(frameon=False, fontsize=9)
    _style_ax(ax, ylabel="Cumulative SE")

    plt.tight_layout()
    plt.savefig(f"{RESULTS_DIR}/comparison_{safe_name}.pdf",
                format="pdf", bbox_inches="tight")
    plt.close()

print(f"\nAll outputs saved to {RESULTS_DIR}/")
