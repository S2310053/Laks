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
from plotter import Plotter

# Create folder to store results
RESULTS_DIR = "Results/Comparison"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Load saved results (each model needs to be run first to generate these)
MODELS = {
    "CatBoost": "Results/CatBoost/catboost_summary.csv",
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
# For each horizon of each model it extracts the following: Hold R², Hold Hit, Skill vs RW, DM p
rows = []
for hz in HORIZON_ORDER:
    row = {"Horizon": hz}
    for name, df in loaded.items():
        mask = df["Horizon"] == hz
        if mask.sum() == 0:
            row[f"{name} R²"]    = None
            row[f"{name} Hit"]   = None
            row[f"{name} Skill"] = None
            row[f"{name} DM p"]  = None
            continue
        r = df[mask].iloc[0]
        hold_r2   = r.get("Hold R2", r.get("Hold R²", None))
        hold_hit  = r.get("Hold Hit", None)
        hold_rmse = r.get("Hold RMSE", None)
        rw_rmse   = r.get("Hold RW RMSE", None)
        dm_p      = r.get("Hold DM p", None)

        skill = Metrics.skill_score(hold_rmse, rw_rmse) if pd.notna(hold_rmse) and pd.notna(rw_rmse) else None

        row[f"{name} R²"]    = hold_r2
        row[f"{name} Hit"]   = hold_hit
        row[f"{name} Skill"] = skill
        row[f"{name} DM p"]  = dm_p
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

def _fmt_skill(x):
    if pd.isna(x) or x is None:
        return "—"
    return f"{x:+.1f}%"

model_names = list(loaded.keys())

# Build display table for R² and Hit for each model
disp_cols = ["Horizon"]
disp_data = {"Horizon": [r["Horizon"] for r in rows]}

for name in model_names:
    disp_data[f"{name} R²"]    = [Metrics.fmt(r.get(f"{name} R²"), ".3f") for r in rows]
    disp_data[f"{name} Hit"]   = [_fmt_pct(r.get(f"{name} Hit")) for r in rows]
    disp_data[f"{name} Skill"] = [_fmt_skill(r.get(f"{name} Skill")) for r in rows]

disp = pd.DataFrame(disp_data)

Plotter().results_table(
    disp,
    "Model Comparison — Holdout 2022–2025\nR², Hit Rate, and Skill vs Random Walk",
    f"{RESULTS_DIR}/comparison_table.pdf",
    width=max(16, len(disp.columns) * 1.5),
)
print()

# Bar chart: Holdout R² by horizon 
fig, axes = plt.subplots(3, 1, figsize=(14, 16))
fig.suptitle("Model Comparison — Holdout 2022–2025", fontsize=14, fontweight="bold")

COLORS = {
    "CatBoost": "#1f77b4",
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

print(f"\nAll outputs saved to {RESULTS_DIR}/")
