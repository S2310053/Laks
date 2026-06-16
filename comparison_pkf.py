# This module compares all models under the purged k-fold method (appendix)
#
##
# Additive companion to model_comparison.py. Reads the per-model PurgedKFold outputs and builds
# the cross-model appendix material:
#   1. Headline pooled table  — each model on its own maximal rows (its own OOS coverage).
#   2. Common-sample table     — every model restricted to the dates ALL models share at a
#                                horizon, RMSE recomputed → the fair head-to-head (matters because
#                                Lasso drops NA rows and so is tested on fewer/later dates).
#   3. Per-fold table          — each model's per-fold RMSE side by side (the four folds).
#   4. Figures                 — skill-vs-RW bars and per-horizon cumulative-SE overlays.
# Boosters use their RMSE loss variant here (matching model_comparison.py); MAE variants live in
# their own PurgedKFold dirs. SARIMA is intentionally excluded (recursive — see notes).
##

import os
import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from metrics import Metrics
from plotter import Plotter
matplotlib.use("Agg", force=True)

metrics = Metrics()
plt.rcParams["font.family"] = "Times New Roman"

DF = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
HZ = ["0w", "1w", "2w", "1m", "3m", "6m", "12m"]
TARGET = {hz: next((c for c in DF.columns if c.startswith(f"Y {hz} ")), None) for hz in HZ}

OUT = "Results/Comparison/PurgedKFold"
os.makedirs(OUT, exist_ok=True)

# Model name -> PurgedKFold dir (boosters: RMSE loss variant)
MODELS = {
    "OLS":      "Results/OLS/PurgedKFold",
    "Lasso":    "Results/Lasso/PurgedKFold",
    "CatBoost": "Results/CatBoost/RMSE/PurgedKFold",
    "HTBoost":  "Results/HTBoost/RMSE/PurgedKFold",
}
COLORS = {"OLS": "#d62728", "Lasso": "#ff7f0e", "CatBoost": "#1A6B8A", "HTBoost": "#9467bd"}


def _safe(t):
    return t.replace("/", "-").replace(" ", "_")

def _h_weeks(hz):
    return {"0w": 0, "1w": 1, "2w": 2, "1m": 4, "3m": 13, "6m": 26, "12m": 52}[hz]

def _load_summary(model):
    p = f"{MODELS[model]}/pooled_summary.csv"
    return pd.read_csv(p).set_index("Horizon") if os.path.exists(p) else None

def _load_preds(model, hz):
    tgt = TARGET[hz]
    if tgt is None:
        return None
    p = f"{MODELS[model]}/pooled_preds_{_safe(tgt)}.csv"
    return pd.read_csv(p, parse_dates=["Date"]) if os.path.exists(p) else None

def _load_perfold(model):
    p = f"{MODELS[model]}/perfold_metrics_all.csv"
    return pd.read_csv(p) if os.path.exists(p) else None


present = [m for m in MODELS if os.path.exists(f"{MODELS[m]}/pooled_summary.csv")]
missing = [m for m in MODELS if m not in present]
print(f"Models present: {present}" + (f"  | MISSING (skipped): {missing}" if missing else ""))
summaries = {m: _load_summary(m) for m in present}


# ---- 1. Headline pooled table (each model on its own rows) ----
rows = []
for hz in HZ:
    row = {"Horizon": hz}
    for m in present:
        s = summaries[m]
        if s is not None and hz in s.index:
            r = s.loc[hz]
            row[f"{m} RMSE"]  = r["Pooled RMSE"]
            row[f"{m} Skill"] = r["Skill%"]
            row[f"{m} DMrw"]  = r.get("DM p (RW)", np.nan)
            row[f"{m} DMols"] = r.get("DM p (OLS)", np.nan)
            row[f"{m} n"]     = r["n_obs"]
        else:
            for k in ("RMSE", "Skill", "DMrw", "DMols", "n"):
                row[f"{m} {k}"] = np.nan
    rows.append(row)
headline = pd.DataFrame(rows)
headline.to_csv(f"{OUT}/comparison_pooled_summary.csv", index=False)
print("\n== Headline pooled (own-sample) RMSE ==")
print(headline[["Horizon"] + [f"{m} RMSE" for m in present]].to_string(index=False))


# ---- 2. Common-sample comparison (all models on shared dates per horizon) ----
def _fp(x):
    return "—" if x is None or (isinstance(x, float) and np.isnan(x)) else (f"{x:.4f}" if x >= 0.001 else "< 0.001")

common_rows = []
for hz in HZ:
    h = _h_weeks(hz)
    preds = {m: _load_preds(m, hz) for m in present}
    preds = {m: d for m, d in preds.items() if d is not None and len(d)}
    if len(preds) < 2:
        continue
    merged = None
    for m, d in preds.items():
        sub = d[["Date", "Actual", "Predicted"]].rename(columns={"Predicted": m})
        merged = sub if merged is None else merged.merge(
            sub.drop(columns="Actual"), on="Date", how="inner")
    if merged is None or len(merged) < 10:
        continue
    a = merged["Actual"].values
    row = {"Horizon": hz, "n_common": len(merged)}
    best_m, best_rmse = None, np.inf
    for m in preds:
        rmse = metrics.rmse(a, merged[m].values)
        row[f"{m} RMSE"] = rmse
        if rmse < best_rmse:
            best_rmse, best_m = rmse, m
    row["Best"] = best_m
    common_rows.append(row)
common = pd.DataFrame(common_rows)
common.to_csv(f"{OUT}/comparison_commonsample.csv", index=False)
print("\n== Common-sample RMSE (all models on shared dates) ==")
if len(common):
    print(common.to_string(index=False))

try:
    disp = pd.DataFrame({"Horizon": common["Horizon"], "n shared": common["n_common"]})
    for m in present:
        col = f"{m} RMSE"
        if col in common:
            disp[m] = [metrics.fmt(v) for v in common[col]]
    disp["Best"] = common["Best"]
    Plotter().results_table(
        disp,
        "Purged k-fold — Common-sample pooled RMSE (models on shared dates)\n"
        "Fair head-to-head: every model scored on the dates all models share at each horizon",
        f"{OUT}/comparison_commonsample.pdf", width=18)
except Exception as e:
    print(f"[warn] common-sample table failed: {type(e).__name__}: {e}")


# ---- 3. Per-fold cross-model RMSE table ----
pf = {m: _load_perfold(m) for m in present}
pf_rows = []
for hz in HZ:
    for fold in (1, 2, 3, 4):
        row = {"Horizon": hz, "Fold": fold}
        any_val = False
        for m in present:
            d = pf[m]
            if d is None:
                row[m] = np.nan; continue
            sel = d[(d["Horizon"] == hz) & (d["Fold"] == fold)]
            if len(sel) and pd.notna(sel.iloc[0]["RMSE"]):
                row[m] = float(sel.iloc[0]["RMSE"]); any_val = True
            else:
                row[m] = np.nan
        if any_val:
            pf_rows.append(row)
perfold = pd.DataFrame(pf_rows)
perfold.to_csv(f"{OUT}/comparison_perfold_rmse.csv", index=False)
print("\n== Per-fold RMSE (first rows) ==")
if len(perfold):
    print(perfold.head(8).to_string(index=False))


# ---- 4a. Skill-vs-RW bar chart (own-sample pooled) ----
try:
    x = np.arange(len(HZ))
    w = 0.8 / max(len(present), 1)
    fig, ax = plt.subplots(figsize=(13, 6), facecolor="white")
    for k, m in enumerate(present):
        vals = [headline.loc[headline["Horizon"] == hz, f"{m} Skill"].values[0] for hz in HZ]
        vals = [v if pd.notna(v) else 0 for v in vals]
        ax.bar(x + k * w, vals, w, label=m, color=COLORS.get(m, f"C{k}"), alpha=0.85)
    ax.axhline(0, color="black", lw=0.8)
    ax.set_xticks(x + w * (len(present) - 1) / 2)
    ax.set_xticklabels(HZ)
    ax.set_ylabel("RMSE Skill vs RW (%)", fontweight="bold")
    ax.set_title("Purged k-fold — Pooled RMSE Skill vs Random Walk", fontsize=12, fontweight="bold")
    ax.legend(frameon=False)
    ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
    ax.grid(True, alpha=0.2, axis="y")
    plt.tight_layout()
    plt.savefig(f"{OUT}/comparison_skill_chart.pdf", format="pdf", bbox_inches="tight")
    plt.close()
    print(f"\nSkill chart → {OUT}/comparison_skill_chart.pdf")
except Exception as e:
    print(f"[warn] skill chart failed: {type(e).__name__}: {e}")


# ---- 4b. Per-horizon cumulative-SE overlay (common dates) ----
for hz in HZ:
    try:
        preds = {m: _load_preds(m, hz) for m in present}
        preds = {m: d for m, d in preds.items() if d is not None and len(d)}
        if len(preds) < 2:
            continue
        merged = None
        for m, d in preds.items():
            sub = d[["Date", "Actual", "Predicted"]].rename(columns={"Predicted": m})
            merged = sub if merged is None else merged.merge(sub.drop(columns="Actual"), on="Date", how="inner")
        if merged is None or len(merged) < 10:
            continue
        merged = merged.sort_values("Date")
        dates, a = merged["Date"].values, merged["Actual"].values
        fig, ax = plt.subplots(figsize=(13, 5), facecolor="white")
        for m in preds:
            ax.plot(dates, np.cumsum((a - merged[m].values) ** 2), label=m, color=COLORS.get(m), lw=1.2)
        ax.plot(dates, np.cumsum(a ** 2), label="Random Walk", color="dimgrey", lw=1.2, ls="--")
        ax.set_title(f"Purged k-fold — Cumulative Squared Error ({hz}, {len(merged)} shared dates)",
                     fontsize=11, fontweight="bold")
        ax.legend(frameon=False, fontsize=9)
        ax.spines["top"].set_visible(False); ax.spines["right"].set_visible(False)
        ax.grid(True, ls="--", alpha=0.1)
        ax.xaxis.set_major_locator(mdates.YearLocator(2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        plt.tight_layout()
        plt.savefig(f"{OUT}/cumSE_{hz}.pdf", format="pdf", bbox_inches="tight")
        plt.close()
    except Exception as e:
        print(f"[warn] cumSE {hz} failed: {type(e).__name__}: {e}")

print(f"\nComparison outputs → {OUT}/")
