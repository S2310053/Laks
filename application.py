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

# Realised weekly return the decision can actually time: a choice at week t (knowing last
# week's published price P_{t-1}) is between selling now at P_t and holding to P_{t+1}, so it
# realises dln[t+1]. We take the clean non-overlapping weekly return Y 0w (= dln[t]) and shift
# it -1 so row t carries dln[t+1]. Signal stays at t → no look-ahead, no overlap.
df0w   = pd.read_csv("Results/HTBoost/RMSE/holdout_preds_Y_0w_∆_Salmon_(NOK-KG).csv",
                     parse_dates=["Date"]).dropna().sort_values("Date").reset_index(drop=True)
r1wRef = df0w.set_index("Date")["Actual"].shift(-1).dropna()  # drop last week (no next-week return)

# Best model per horizon selected on cross-validated (pre-holdout) forecast performance,
# so the model choice is independent of the 2022-2026 holdout used to evaluate the strategy
HORIZONS = {
    "1w":  {"path": "Results/HTBoost/RMSE/holdout_preds_Y_1w_∆_Salmon_(NOK-KG).csv",  "model": "HTBoost", "label": "h = 1w"},
    "2w":  {"path": "Results/HTBoost/RMSE/holdout_preds_Y_2w_∆_Salmon_(NOK-KG).csv",  "model": "HTBoost", "label": "h = 2w"},
    "1m":  {"path": "Results/OLS/holdout_preds_Y_1m_∆_Salmon_(NOK-KG).csv",           "model": "OLS",     "label": "h = 1m"},
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

def newey_west_t(d):
    # HAC (Bartlett-kernel) t-statistic for the mean of d against zero; lag by the standard rule
    n = len(d); m = d.mean(); e = d - m
    L = int(np.floor(4 * (n / 100) ** (2 / 9)))
    lrv = np.mean(e * e)
    for k in range(1, L + 1):
        lrv += 2 * (1 - k / (L + 1)) * np.mean(e[k:] * e[:-k])
    se = np.sqrt(lrv / n)
    return m / se if se > 0 else 0.0

# Model returns are reported net of a fixed one-way transaction cost charged on each position
# change (turnover); the always-hold benchmark holds a constant position, so pays none.
TC_BPS = 50

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
        f"Model-timed (net {{}}bp) vs always-hold | log returns per decision | Holdout 2022–2025".format(TC_BPS),
        fontsize=13, fontweight="bold"
    )

    for ax, h in zip(axes, cfg["horizons"]):
        dates, pred, ret = aggregate(loadPreds(h), freq)
        pos       = (pred > 0).astype(float)
        switches  = np.abs(np.diff(pos, prepend=0.0))             # 1 on every hold<->sell switch
        modelRet  = pos * ret - (TC_BPS / 10000.0) * switches     # net of transaction costs
        holdRet   = ret                                           # always hold every decision (no model)

        mM, sM, srM = perDecisionStats(modelRet)
        mH, sH, srH = perDecisionStats(holdRet)
        n    = len(ret)
        tHAC = newey_west_t(modelRet - holdRet)                   # HAC t-stat on the model-vs-hold spread

        print(f"  {HORIZONS[h]['label']:8s} | n={n:>3} | "
              f"Model(net {TC_BPS}bp): mean={mM:+.3f} sd={sM:.3f} SR={srM:.2f} | "
              f"Hold: SR={srH:.2f} | t={tHAC:.2f}")

        summaryRows.append({
            "Participant": participant, "Horizon": h, "Model": HORIZONS[h]["model"], "n": n,
            "Mean_Model": mM, "SD_Model": sM, "SR_Model": srM,
            "Mean_Hold": mH, "SD_Hold": sH, "SR_Hold": srH,
            "t_stat": tHAC,
        })

        pd.DataFrame({"Date": dates, "Pred": pred, "Ret_period": ret,
                      "Position": pos, "Model_Ret": modelRet, "Hold_Ret": holdRet,
                      "Cum_Model": np.cumsum(modelRet), "Cum_Hold": np.cumsum(holdRet)}
                     ).to_csv(f"{RESULTS_DIR}/strategy_{h}.csv", index=False)

        ax.plot(dates, np.cumsum(modelRet), label=f"{HORIZONS[h]['model']} (model-timed)", color=_BLUE, lw=1.5)
        ax.plot(dates, np.cumsum(holdRet), label="Always hold (no model)", color=_DARK, lw=1.2, ls="--", alpha=0.8)
        ax.axhline(0.0, color=_GREY, lw=0.8, ls=":")
        ax.set_title(f"{HORIZONS[h]['label']}  —  {HORIZONS[h]['model']}  |  "
                     f"SR {srM:.2f} vs {srH:.2f}  (n={n})", fontsize=10, fontweight="bold")
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
