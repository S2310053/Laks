# This module is made for model comparison

##
# All other scripts must be run first to generate the necessary results files in the Results/ folder
# This script only reads those saved results and compares them — it does not re-estimate any models
# That way models can be run independently, and this comparison can be run quickly after all models have been estimated
# Because some of the models can take a bit of time to run
# CatBoost and HTBoost produce results for both RMSE and MAE loss functions in separate subdirectories.
# LOSS_VARIANT below selects which to use for the primary comparison (default: RMSE).
##

import pandas as pd
import numpy as np
import os
import re
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from metrics import Metrics
metrics = Metrics()
from plotter import Plotter

# Create folder to store results
RESULTS_DIR  = "Results/Comparison"
LOSS_VARIANT = "RMSE"      # "RMSE" or "MAE" — selects CatBoost/HTBoost variant
os.makedirs(RESULTS_DIR, exist_ok=True)

# Plot style
plt.rcParams["font.family"]      = "Times New Roman"
plt.rcParams["mathtext.fontset"] = "custom"
plt.rcParams["mathtext.rm"]      = "Times New Roman"
plt.rcParams["mathtext.it"]      = "Times New Roman:italic"
plt.rcParams["mathtext.bf"]      = "Times New Roman:bold"

_DARK = "#0D3B5E"
_GREY = "dimgrey"

COLORS = {
    "CatBoost": "#1A6B8A",
    "HTBoost":  "#9467bd",
    "Lasso":    "#ff7f0e",
    "SARIMA":   "#2ca02c",
    "OLS":      "#d62728",
}

# Load saved results (each model needs to be run first to generate these)
MODELS = {
    "CatBoost": f"Results/CatBoost/{LOSS_VARIANT}/catboost_summary.csv",
    "HTBoost":  f"Results/HTBoost/{LOSS_VARIANT}/htboost_summary.csv",
    "Lasso":    "Results/Lasso/lasso_summary.csv",
    "SARIMA":   "Results/SARIMA/sarima_summary.csv",
    "OLS":      "Results/OLS/ols_summary.csv",
}

loaded = {}
for name, path in MODELS.items():
    if os.path.exists(path):
        df_m = pd.read_csv(path)
        if "Horizon" not in df_m.columns and "Y" in df_m.columns:
            df_m["Horizon"] = df_m["Y"].apply(
                lambda x: re.search(r"Y (\d+\w)", x).group(1)
                          if re.search(r"Y (\d+\w)", x) else "?")
        loaded[name] = df_m
        print(f"  ✓ {name:<10} loaded ({len(df_m)} horizons) ← {path}")
    else:
        print(f"  ✗ {name:<10} NOT FOUND — run {name.lower()}_model.py first")

if not loaded:
    print("\nNo results found. Run individual model scripts first.")
    raise SystemExit

# Define horizon order
HORIZON_ORDER = ["0w", "1w", "2w", "1m", "3m", "6m", "12m"]
model_names   = list(loaded.keys())

# Build comparison table
# For each horizon of each model it extracts the following: Hold R², Hold Hit, Skill vs RW, DM p, CW p
def _get(r, *keys):
    for k in keys:
        v = r.get(k)
        if v is not None and not (isinstance(v, float) and np.isnan(v)):
            return v
    return None

rows = []
for hz in HORIZON_ORDER:
    row = {"Horizon": hz}
    for name, df_m in loaded.items():
        mask = df_m["Horizon"] == hz
        if mask.sum() == 0:
            for key in ("CV R²", "R²", "RMSE", "MAE", "RW RMSE", "Skill",
                        "MAE Skill", "Hit", "DM p", "CW p"):
                row[f"{name} {key}"] = None
            continue
        r = df_m[mask].iloc[0]

        hold_rmse = _get(r, "Hold RMSE")
        hold_mae  = _get(r, "Hold MAE")
        rw_rmse   = _get(r, "Hold RW RMSE")
        rw_mae    = _get(r, "Hold RW MAE")

        row[f"{name} CV R²"]    = _get(r, "CV R2",   "CV R²")
        row[f"{name} R²"]       = _get(r, "Hold R2", "Hold R²")
        row[f"{name} RMSE"]     = hold_rmse
        row[f"{name} MAE"]      = hold_mae
        row[f"{name} RW RMSE"]  = rw_rmse
        row[f"{name} Skill"]    = metrics.skill_score(hold_rmse, rw_rmse)
        row[f"{name} MAE Skill"]= metrics.skill_score(hold_mae,  rw_mae)
        row[f"{name} Hit"]      = _get(r, "Hold Hit")
        row[f"{name} DM p"]     = _get(r, "Hold DM p")
        row[f"{name} CW p"]     = _get(r, "Hold CW p")   # None for OLS/SARIMA
    rows.append(row)

comp_df = pd.DataFrame(rows)
print("\nComparison Table (Holdout period)")
print(comp_df.to_string(index=False))
comp_df.to_csv(f"{RESULTS_DIR}/comparison_summary.csv", index=False)

# PDF with combined results table
def _fmt_pct(x, signed=False):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    fmt = f"{x:+.1f}%" if signed else f"{x:.1%}"
    return fmt

def _fmt_p(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    if x < 0.001:
        return "<.001"
    return f"{x:.3f}"

disp_data = {"Horizon": [r["Horizon"] for r in rows]}
for name in model_names:
    disp_data[f"{name} RMSE"]     = [metrics.fmt(r.get(f"{name} RMSE"),     ".4f") for r in rows]
    disp_data[f"{name} MAE"]      = [metrics.fmt(r.get(f"{name} MAE"),      ".4f") for r in rows]
    disp_data[f"{name} Skill%"]   = [_fmt_pct(r.get(f"{name} Skill"),   signed=True) for r in rows]
    disp_data[f"{name} R²"]       = [metrics.fmt(r.get(f"{name} R²"),       ".3f") for r in rows]
    disp_data[f"{name} Hit"]      = [_fmt_pct(r.get(f"{name} Hit"))          for r in rows]
    disp_data[f"{name} p(DM)"]    = [_fmt_p(r.get(f"{name} DM p"))           for r in rows]
    disp_data[f"{name} p(CW)"]    = [_fmt_p(r.get(f"{name} CW p"))           for r in rows]

disp = pd.DataFrame(disp_data)
Plotter().results_table(
    disp,
    f"Model Comparison — Holdout 2022–2025  [{LOSS_VARIANT} loss variant]\n"
    "RMSE, MAE, Skill vs RW, R², Hit Rate | DM = Harvey et al. (1997) vs RW | CW = Clark-West (2007) vs OLS",
    f"{RESULTS_DIR}/comparison_table.pdf",
    width=max(22, len(disp.columns) * 1.8),
)

# Bar chart: Holdout R² by horizon
fig, axes = plt.subplots(4, 1, figsize=(14, 20), facecolor="white")
fig.suptitle(f"Model Comparison — Holdout 2022–2025  [{LOSS_VARIANT}]",
             fontsize=14, fontweight="bold")

x     = np.arange(len(HORIZON_ORDER))
width = 0.8 / max(len(model_names), 1)

def _bar_vals(metric_key, default=0):
    out = {}
    for name in model_names:
        vals = []
        for hz in HORIZON_ORDER:
            match = [r for r in rows if r["Horizon"] == hz]
            v = match[0].get(f"{name} {metric_key}") if match else None
            vals.append(v if v is not None and pd.notna(v) else default)
        out[name] = vals
    return out

# Panel 1: R²
ax = axes[0]
r2_vals = _bar_vals("R²")
for k, name in enumerate(model_names):
    ax.bar(x + k * width, r2_vals[name], width, label=name,
           color=COLORS.get(name, f"C{k}"), alpha=0.85)
ax.set_xticks(x + width * (len(model_names) - 1) / 2)
ax.set_xticklabels(HORIZON_ORDER)
ax.set_ylabel("R²", fontweight="bold")
ax.set_title("Holdout R²  (higher = better)", fontsize=11)
ax.axhline(0, color="black", lw=0.8)
ax.legend(frameon=False, fontsize=9)
ax.grid(True, alpha=0.2, axis="y")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

# Panel 2: Hit Rate
ax = axes[1]
hit_vals = _bar_vals("Hit")
for k, name in enumerate(model_names):
    ax.bar(x + k * width, [v * 100 for v in hit_vals[name]], width, label=name,
           color=COLORS.get(name, f"C{k}"), alpha=0.85)
ax.axhline(50, color=_GREY, lw=1, ls="--", label="Random Walk (50%)")
ax.set_xticks(x + width * (len(model_names) - 1) / 2)
ax.set_xticklabels(HORIZON_ORDER)
ax.set_ylabel("Hit Rate (%)", fontweight="bold")
ax.set_title("Holdout Hit Rate  (higher = better, RW = 50%)", fontsize=11)
ax.legend(frameon=False, fontsize=9)
ax.grid(True, alpha=0.2, axis="y")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

# Panel 3: Skill vs RW
ax = axes[2]
skill_vals = _bar_vals("Skill")
for k, name in enumerate(model_names):
    ax.bar(x + k * width, skill_vals[name], width, label=name,
           color=COLORS.get(name, f"C{k}"), alpha=0.85)
ax.axhline(0, color="black", lw=0.8)
ax.set_xticks(x + width * (len(model_names) - 1) / 2)
ax.set_xticklabels(HORIZON_ORDER)
ax.set_ylabel("Skill (%)", fontweight="bold")
ax.set_title("RMSE Skill vs Random Walk  (positive = better than RW)", fontsize=11)
ax.legend(frameon=False, fontsize=9)
ax.grid(True, alpha=0.2, axis="y")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

# Panel 4: DM p-value (Harvey et al. 1997) vs RW — lower = stronger evidence model beats RW
ax = axes[3]
dm_vals = _bar_vals("DM p", default=1.0)
for k, name in enumerate(model_names):
    ax.bar(x + k * width, dm_vals[name], width, label=name,
           color=COLORS.get(name, f"C{k}"), alpha=0.85)
ax.axhline(0.10, color=_GREY, lw=1, ls=":",  label="10% significance")
ax.axhline(0.05, color=_GREY, lw=1, ls="--", label="5% significance")
ax.axhline(0.01, color=_GREY, lw=1.2, ls="-", label="1% significance")
ax.set_xticks(x + width * (len(model_names) - 1) / 2)
ax.set_xticklabels(HORIZON_ORDER)
ax.set_ylabel("p-value", fontweight="bold")
ax.set_ylim(0, 1.05)
ax.set_title("DM Test p-value vs Random Walk — Harvey et al. (1997)  (lower = model beats RW)",
             fontsize=11)
ax.legend(frameon=False, fontsize=9)
ax.grid(True, alpha=0.2, axis="y")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

plt.tight_layout()
plt.savefig(f"{RESULTS_DIR}/comparison_charts.pdf", format="pdf", bbox_inches="tight")
plt.close()
print(f"Bar charts saved → {RESULTS_DIR}/comparison_charts.pdf")

# Detailed table: Best model per horizon
print("\n── Best Model per Horizon ──────────────────────────────────")
print(f"  {'Horizon':>7}  {'By R²':<12}  {'R²':>6}  {'By RMSE Skill':<14}  {'Skill':>6}")
for hz in HORIZON_ORDER:
    best_r2_name = best_skill_name = "—"
    best_r2 = best_skill = -np.inf
    for name in model_names:
        match = [r for r in rows if r["Horizon"] == hz]
        if not match:
            continue
        v_r2    = match[0].get(f"{name} R²")
        v_skill = match[0].get(f"{name} Skill")
        if v_r2 is not None and pd.notna(v_r2) and v_r2 > best_r2:
            best_r2 = v_r2; best_r2_name = name
        if v_skill is not None and pd.notna(v_skill) and v_skill > best_skill:
            best_skill = v_skill; best_skill_name = name
    r2_str    = f"{best_r2:.3f}"    if best_r2    > -np.inf else "—"
    skill_str = f"{best_skill:+.1f}%" if best_skill > -np.inf else "—"
    print(f"  {hz:>7}  {best_r2_name:<12}  {r2_str:>6}  {best_skill_name:<14}  {skill_str:>6}")

# Overall ranking by average holdout RMSE across all horizons
print("\n── Overall Ranking by Avg Holdout RMSE ─────────────────────")
avg_rmse = {}
avg_skill = {}
for name in model_names:
    rmse_vals  = [r.get(f"{name} RMSE")  for r in rows if r.get(f"{name} RMSE")  is not None and pd.notna(r.get(f"{name} RMSE"))]
    skill_vals = [r.get(f"{name} Skill") for r in rows if r.get(f"{name} Skill") is not None and pd.notna(r.get(f"{name} Skill"))]
    avg_rmse[name]  = np.mean(rmse_vals)  if rmse_vals  else np.nan
    avg_skill[name] = np.mean(skill_vals) if skill_vals else np.nan

ranking = sorted(avg_rmse.items(), key=lambda x: x[1] if pd.notna(x[1]) else np.inf)
print(f"  {'Rank':>4}  {'Model':<12}  {'Avg RMSE':>9}  {'Avg Skill vs RW':>15}")
for rank, (name, rmse) in enumerate(ranking, 1):
    skill_str = f"{avg_skill[name]:+.2f}%" if pd.notna(avg_skill.get(name, np.nan)) else "—"
    rmse_str  = f"{rmse:.4f}"              if pd.notna(rmse)                          else "—"
    print(f"  {rank:>4}  {name:<12}  {rmse_str:>9}  {skill_str:>15}")

# Add ranking row to PDF table
rank_row_rmse  = {"Horizon": "Avg RMSE"}
rank_row_rank  = {"Horizon": "Rank"}
for name in model_names:
    rank_row_rmse[f"{name} RMSE"]    = metrics.fmt(avg_rmse.get(name), ".4f")
    rank_row_rmse[f"{name} MAE"]     = "—"
    rank_row_rmse[f"{name} Skill%"]  = f"{avg_skill.get(name, np.nan):+.1f}%" if pd.notna(avg_skill.get(name, np.nan)) else "—"
    rank_row_rmse[f"{name} R²"]      = "—"
    rank_row_rmse[f"{name} Hit"]     = "—"
    rank_row_rmse[f"{name} p(DM)"]   = "—"
    rank_row_rmse[f"{name} p(CW)"]   = "—"
    rank_row_rank[f"{name} RMSE"]    = str(next(i+1 for i, (n, _) in enumerate(ranking) if n == name))
    rank_row_rank[f"{name} MAE"]     = "—"
    rank_row_rank[f"{name} Skill%"]  = "—"
    rank_row_rank[f"{name} R²"]      = "—"
    rank_row_rank[f"{name} Hit"]     = "—"
    rank_row_rank[f"{name} p(DM)"]   = "—"
    rank_row_rank[f"{name} p(CW)"]   = "—"

disp = pd.concat([disp, pd.DataFrame([rank_row_rmse, rank_row_rank])], ignore_index=True)

# Time-series comparison plots per horizon
# Panel 1: Predicted vs Actual (all models + RW)
# Panel 2: Cumulative Squared Error (lower = better)

# CatBoost and HTBoost predictions come from the selected LOSS_VARIANT subdirectory
PRED_DIRS = {
    "CatBoost": f"Results/CatBoost/{LOSS_VARIANT}",
    "HTBoost":  f"Results/HTBoost/{LOSS_VARIANT}",
    "Lasso":    "Results/Lasso",
    "SARIMA":   "Results/SARIMA",
    "OLS":      "Results/OLS",
}

def _style_ax(ax, ylabel=""):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, linestyle="--", alpha=0.1)
    if ylabel:
        ax.set_ylabel(ylabel, fontweight="bold")
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))

# Collect all target names from loaded summaries
all_targets = set()
for df_m in loaded.values():
    all_targets.update(df_m["Y"].values)

print("\nGenerating time-series comparison plots...")
for target in sorted(all_targets):
    safe_name = target.replace("/", "-").replace(" ", "_")
    hz_match  = re.search(r"Y (\d+\w)", target)
    hz_label  = hz_match.group(1) if hz_match else "?"

    # Load and merge holdout predictions from each model
    merged = None
    for model_name, pred_dir in PRED_DIRS.items():
        pred_file = f"{pred_dir}/holdout_preds_{safe_name}.csv"
        if not os.path.exists(pred_file):
            continue
        pdf = pd.read_csv(pred_file, parse_dates=["Date"]).rename(
            columns={"Predicted": model_name})
        if merged is None:
            merged = pdf
        else:
            merged = merged.merge(pdf[["Date", model_name]], on="Date", how="outer")

    if merged is None or len(merged) == 0:
        continue

    merged      = merged.sort_values("Date").reset_index(drop=True)
    dates       = merged["Date"].values
    actuals     = merged["Actual"].values
    model_preds = {n: merged[n].values for n in PRED_DIRS if n in merged.columns}

    print(f"  {target}  ({len(model_preds)} models, {len(dates)} dates)")

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), facecolor="white")
    fig.suptitle(f"{target}  —  Holdout 2022–2025  [{LOSS_VARIANT}]",
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

    # Panel 2: Cumulative Squared Error (lower = better)
    ax = axes[1]
    for model_name, preds in model_preds.items():
        mask = ~(np.isnan(actuals) | np.isnan(preds))
        if mask.sum() == 0:
            continue
        sse = np.cumsum((actuals[mask] - preds[mask]) ** 2)
        ax.plot(dates[mask], sse, label=f"{model_name}",
                color=COLORS.get(model_name, "C0"), lw=1.2, alpha=0.85)
    mask_rw = ~np.isnan(actuals)
    rw_sse  = np.cumsum(actuals[mask_rw] ** 2)
    ax.plot(dates[mask_rw], rw_sse, label="Random Walk",
            color=_GREY, lw=1.2, ls="--", alpha=0.8)
    ax.set_title("Cumulative Squared Error  (lower = better)", fontsize=10)
    ax.legend(frameon=False, fontsize=9)
    _style_ax(ax, ylabel="Cumulative SE")

    plt.tight_layout()
    plt.savefig(f"{RESULTS_DIR}/comparison_{safe_name}.pdf",
                format="pdf", bbox_inches="tight")
    plt.close()

print(f"\nAll outputs saved to {RESULTS_DIR}/")
