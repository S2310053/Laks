# Application — practical revenue-timing simulation (Heterogeneous Market Hypothesis)
#
# A simple, illustrative example (strong assumptions, kept deliberately simple).
# Three salmon participants make decisions at three frequencies per year:
#       Trader   52/yr (weekly)    using h = 1w, 2w, 1m
#       Exporter 12/yr (monthly)   using h = 3m, 6m
#       Farmer    4/yr (quarterly) using h = 12m
#
# This is a real business problem (revenue, the top line of the income statement = P*Q),
# NOT a portfolio problem. Quantity is fixed, so the decision affects only the PRICE realised.
# Salmon prices are forecast in LOG returns, so we work directly in returns (no price levels).
#
# Each decision: hold and realise the period log return if the forecast is up (pos=1),
# or sell now at spot and realise 0 if the forecast is down (pos=0).
#   Model revenue (log return) = pos * r_period
#   Benchmark = sell at spot every decision (no model) = r_period every period
# We report expected revenue (mean), volatility (SD), and Sharpe (reward for the risk taken),
# all PER DECISION in the participant's own period (weekly / monthly / quarterly) — the data
# is weekly, so nothing is annualised. No transaction costs.

import pandas as pd
import numpy as np
import os
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

plt.rcParams["font.family"]      = "Times New Roman"
plt.rcParams["mathtext.fontset"] = "custom"
plt.rcParams["mathtext.rm"]      = "Times New Roman"
plt.rcParams["mathtext.it"]      = "Times New Roman:italic"
plt.rcParams["mathtext.bf"]      = "Times New Roman:bold"

_BLUE = "#1A6B8A"
_DARK = "#0D3B5E"
_GREY = "dimgrey"

RESULTS_DIR = "Results/Application"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Weekly spot log returns — the realised return series all participants act on
df1w   = pd.read_csv("Results/HTBoost/RMSE/holdout_preds_Y_1w_∆_Salmon_(NOK-KG).csv",
                     parse_dates=["Date"]).dropna().sort_values("Date").reset_index(drop=True)
r1wRef = df1w.set_index("Date")["Actual"]

# Best model per horizon (lowest holdout RMSE)
HORIZONS = {
    "1w":  {"path": "Results/HTBoost/RMSE/holdout_preds_Y_1w_∆_Salmon_(NOK-KG).csv",  "model": "HTBoost", "label": "h = 1w"},
    "2w":  {"path": "Results/HTBoost/RMSE/holdout_preds_Y_2w_∆_Salmon_(NOK-KG).csv",  "model": "HTBoost", "label": "h = 2w"},
    "1m":  {"path": "Results/CatBoost/RMSE/holdout_preds_Y_1m_∆_Salmon_(NOK-KG).csv", "model": "CatBoost","label": "h = 1m"},
    "3m":  {"path": "Results/OLS/holdout_preds_Y_3m_∆_Salmon_(NOK-KG).csv",           "model": "OLS",     "label": "h = 3m"},
    "6m":  {"path": "Results/OLS/holdout_preds_Y_6m_∆_Salmon_(NOK-KG).csv",           "model": "OLS",     "label": "h = 6m"},
    "12m": {"path": "Results/CatBoost/RMSE/holdout_preds_Y_12m_∆_Salmon_(NOK-KG).csv","model": "CatBoost","label": "h = 12m"},
}

# Participant -> horizons used, decision frequency (freq key + decisions per year)
PARTICIPANTS = {
    "Trader":   {"horizons": ["1w", "2w", "1m"], "freq": "W", "ppy": 52, "figsize": (14, 13), "file": "trading_trader.pdf"},
    "Exporter": {"horizons": ["3m", "6m"],       "freq": "M", "ppy": 12, "figsize": (14, 9),  "file": "trading_exporter.pdf"},
    "Farmer":   {"horizons": ["12m"],            "freq": "Q", "ppy": 4,  "figsize": (14, 5),  "file": "trading_farmer.pdf"},
}

def loadPreds(h):
    df = pd.read_csv(HORIZONS[h]["path"], parse_dates=["Date"]).dropna(
             subset=["Predicted"]).sort_values("Date").reset_index(drop=True)
    return df[["Date", "Predicted"]].merge(
        r1wRef.reset_index().rename(columns={"Actual": "r1w"}), on="Date", how="inner"
    ).reset_index(drop=True)

def aggregate(df, freq):
    # Reduce weekly data to one decision per period: prediction at period start,
    # realised period return = sum of weekly log returns in the period (additive).
    if freq == "W":
        return df["Date"].values, df["Predicted"].values, df["r1w"].values
    d = df["Date"].dt
    if freq == "M":   key = d.year * 12 + d.month               # 12 / year (calendar month)
    else:             key = d.year * 4 + (d.month - 1) // 3     # 4 / year (calendar quarter)
    g = df.groupby(key, sort=True)
    return g["Date"].first().values, g["Predicted"].first().values, g["r1w"].sum().values

def perDecisionStats(r):
    # Per-decision moments (no annualisation): data is weekly, each period = one decision
    mean = r.mean()
    sd   = r.std()
    sr   = mean / sd if sd > 0 else 0.0
    return mean, sd, sr

def styleAx(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, linestyle="--", alpha=0.1)
    ax.set_ylabel("Cumulative Revenue (log return)", fontweight="bold")
    ax.xaxis.set_major_locator(mdates.YearLocator(1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

summaryRows = []

for participant, cfg in PARTICIPANTS.items():
    freq, ppy = cfg["freq"], cfg["ppy"]
    print(f"\n{participant}  ({ppy} decisions/year)")
    fig, axes = plt.subplots(len(cfg["horizons"]), 1, figsize=cfg["figsize"], facecolor="white")
    if len(cfg["horizons"]) == 1:
        axes = [axes]
    fig.suptitle(
        f"Revenue Timing — {participant}  ({ppy} decisions/year)\n"
        f"Model-timed vs sell-at-spot | log returns per decision | Holdout 2022–2025",
        fontsize=13, fontweight="bold"
    )

    for ax, h in zip(axes, cfg["horizons"]):
        dates, pred, ret = aggregate(loadPreds(h), freq)
        pos       = (pred > 0).astype(float)
        modelRet  = pos * ret      # hold if forecast up, sell now (0) if forecast down
        naiveRet  = ret            # sell at spot every decision (no model)

        mM, sM, srM = perDecisionStats(modelRet)
        mN, sN, srN = perDecisionStats(naiveRet)
        n = len(ret)

        print(f"  {HORIZONS[h]['label']:8s} | n={n:>3} | "
              f"Model:  mean={mM:+.3f} sd={sM:.3f} SR={srM:.2f} | "
              f"Naive:  mean={mN:+.3f} sd={sN:.3f} SR={srN:.2f}")

        summaryRows.append({
            "Participant": participant, "Horizon": h, "Model": HORIZONS[h]["model"],
            "Decisions_per_year": ppy, "n_periods": n,
            "Mean_Model": mM, "SD_Model": sM, "SR_Model": srM,
            "Mean_Naive": mN, "SD_Naive": sN, "SR_Naive": srN,
        })

        pd.DataFrame({"Date": dates, "Pred": pred, "Ret_period": ret,
                      "Position": pos, "Model_Ret": modelRet, "Naive_Ret": naiveRet,
                      "Cum_Model": np.cumsum(modelRet), "Cum_Naive": np.cumsum(naiveRet)}
                     ).to_csv(f"{RESULTS_DIR}/strategy_{h}.csv", index=False)

        ax.plot(dates, np.cumsum(modelRet), label=f"{HORIZONS[h]['model']} (model-timed)", color=_BLUE, lw=1.5)
        ax.plot(dates, np.cumsum(naiveRet), label="Sell at spot (no model)", color=_DARK, lw=1.2, ls="--", alpha=0.8)
        ax.axhline(0.0, color=_GREY, lw=0.8, ls=":")
        ax.set_title(f"{HORIZONS[h]['label']}  —  {HORIZONS[h]['model']}  |  "
                     f"SR {srM:.2f} vs {srN:.2f}  (n={n})", fontsize=10, fontweight="bold")
        ax.legend(frameon=False, fontsize=9)
        styleAx(ax)

    plt.tight_layout()
    plt.savefig(f"{RESULTS_DIR}/{cfg['file']}", format="pdf", bbox_inches="tight")
    plt.close()
    print(f"  Saved: {RESULTS_DIR}/{cfg['file']}")

summaryDf = pd.DataFrame(summaryRows)
summaryDf.to_csv(f"{RESULTS_DIR}/strategy_summary.csv", index=False)
print(f"\nSummary → {RESULTS_DIR}/strategy_summary.csv")
print(summaryDf.to_string(index=False))
