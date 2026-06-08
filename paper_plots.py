import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap
from scipy import stats
from statsmodels.tsa.stattools    import acf as _acf, adfuller
from statsmodels.stats.diagnostic import acorr_ljungbox, het_arch

# ── paths ────────────────────────────────────────────────────────────────────
BASE   = os.path.dirname(os.path.abspath(__file__))
DATA   = os.path.join(BASE, "Data", "Factors.csv")
OUT    = os.path.join(BASE, "Results", "Paper Plots and Tables")
os.makedirs(OUT, exist_ok=True)

# ── data ─────────────────────────────────────────────────────────────────────
df = pd.read_csv(DATA, parse_dates=["Date"])

TARGETS = [
    "Y 0w ∆ Salmon (NOK/KG)",
    "Y 1w ∆ Salmon (NOK/KG)",
    "Y 2w ∆ Salmon (NOK/KG)",
    "Y 1m ∆ Salmon (NOK/KG)",
    "Y 3m ∆ Salmon (NOK/KG)",
    "Y 6m ∆ Salmon (NOK/KG)",
    "Y 12m ∆ Salmon (NOK/KG)",
]

LABELS = ["h = 0w", "h = 1w", "h = 2w", "h = 1m", "h = 3m", "h = 6m", "h = 12m"]

OUTLIER_Z = 3.0

# ── global rcParams (Journal of Finance style) ───────────────────────────────
plt.rcParams.update({
    "font.family":          "serif",
    "font.serif":           ["Times New Roman"],
    "font.size":            8,
    "axes.titlesize":       8,
    "axes.labelsize":       8,
    "xtick.labelsize":      7,
    "ytick.labelsize":      7,
    "axes.spines.top":      False,
    "axes.spines.right":    False,
    "axes.spines.left":     True,
    "axes.spines.bottom":   True,
    "axes.linewidth":       0.5,
    "xtick.major.width":    0.5,
    "ytick.major.width":    0.5,
    "xtick.major.size":     3,
    "ytick.major.size":     3,
    "axes.grid":            False,
    "figure.dpi":           300,
})

# ═══════════════════════════════════════════════════════════════════════════
# Figure 1 — Time Series Panel
# ═══════════════════════════════════════════════════════════════════════════
def plot_timeseries():
    # A4 text width with BI grading margin: usable ≈ 150 mm = 5.9 in
    fig, axes = plt.subplots(
        nrows=7, ncols=1,
        figsize=(5.9, 9.5),
        sharex=True,
    )

    for ax, col, label in zip(axes, TARGETS, LABELS):
        s = df[col].dropna()
        t = df.loc[s.index, "Date"]

        # outlier mask
        z      = (s - s.mean()) / s.std()
        is_out = z.abs() > OUTLIER_Z
        out_s  = s[is_out]
        t_out  = t[is_out]

        # continuous line
        ax.plot(t, s, color="black", linewidth=1.0, zorder=1)

        # outliers: open circles, small and unobtrusive
        ax.scatter(t_out, out_s,
                   facecolors="none", edgecolors="black",
                   s=10, linewidths=0.80, zorder=2, marker="o")

        # zero reference — very faint dashed
        ax.axhline(0, color="black", linewidth=0.3, linestyle=":", alpha=0.4)

        # panel label top-left, italicised
        ax.set_title(label, loc="left", pad=3)

        # y ticks: 3 levels, auto scale, no fixed format
        ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=3, prune="both"))
        ax.tick_params(axis="y", pad=2)

    axes[-1].set_xlabel("$t$", labelpad=4)

    # x-axis: pin limits and ticks on the shared axis (axes[0] is the sharex parent)
    tick_years = [2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024, 2026]
    tick_dates = [pd.Timestamp(f"{y}-01-01") for y in tick_years]
    axes[0].set_xlim(pd.Timestamp("2006-01-01"), pd.Timestamp("2026-12-31"))
    axes[0].xaxis.set_major_locator(
        ticker.FixedLocator(mdates.date2num(tick_dates))
    )
    axes[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    # JF-style legend: transparent box, thin black edge, placed on first panel
    _dot = mlines.Line2D(
        [], [], color="black", marker="o", markerfacecolor="none",
        markersize=4, linewidth=0, markeredgewidth=0.8,
        label=r"Outlier ($|z| > 3$)",
    )
    axes[0].legend(
        handles=[_dot],
        loc="lower right",
        fontsize=7,
        frameon=True,
        framealpha=0.0,
        edgecolor="black",
        handlelength=1.0,
        handletextpad=0.4,
        borderpad=0.4,
    )

    fig.tight_layout(h_pad=0.5)
    fig.subplots_adjust(left=0.07, top=0.955)   # wider panels; room for top label

    # y-axis variable name as a horizontal header centred above the top panel
    fig.text(0.5, 0.99, r"$\Delta \ln$ Salmon Spot Price",
             ha="center", va="top", fontsize=9)

    path = os.path.join(OUT, "fig1_timeseries.png")
    fig.savefig(path, bbox_inches="tight", dpi=300)
    plt.close(fig)
    print(f"Saved → {path}")


# ═══════════════════════════════════════════════════════════════════════════
# Figure 2 — Distribution Panel
# ═══════════════════════════════════════════════════════════════════════════
def plot_distributions():
    fig, axes = plt.subplots(
        nrows=7, ncols=1,
        figsize=(5.9, 9.5),
        sharex=False,
    )

    for ax, col, label in zip(axes, TARGETS, LABELS):
        s  = df[col].dropna()
        mu = s.mean()
        sg = s.std()
        sk = float(s.skew())
        ku = float(s.kurtosis())   # excess kurtosis (pandas subtracts 3)

        # filled histogram — light gray bars, white edges
        ax.hist(s, bins=35, density=True,
                color="#d0d0d0", edgecolor="white", linewidth=0.4)

        # symmetric x-axis so 0 aligns across all panels
        max_abs = max(abs(s.min()), abs(s.max()))
        ax.set_xlim(-max_abs, max_abs)

        # normal reference curve
        x = np.linspace(-max_abs, max_abs, 300)
        ax.plot(x, stats.norm.pdf(x, mu, sg),
                color="black", linewidth=0.8, linestyle="--")

        # panel title: horizon + four moments
        ax.set_title(
            f"{label}     "
            f"μ = {mu:.3f}   "
            f"σ = {sg:.3f}   "
            f"sk = {sk:.3f}   "
            f"κ = {ku:.3f}",
            loc="left", pad=3,
        )

        ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=3, prune="both"))
        ax.tick_params(axis="y", pad=2)
        ax.tick_params(axis="x", pad=2)

    axes[-1].set_xlabel(r"$\Delta \ln$ Salmon Spot Price", labelpad=4)

    # legend — normal curve, bottom-right of first panel
    _norm = mlines.Line2D([], [], color="black", linewidth=0.8, linestyle="--",
                          label="Normal")
    axes[0].legend(
        handles=[_norm],
        loc="lower right",
        fontsize=7,
        frameon=True,
        framealpha=0.0,
        edgecolor="black",
        handlelength=1.5,
        handletextpad=0.4,
        borderpad=0.4,
    )

    fig.tight_layout(h_pad=0.5)
    fig.subplots_adjust(left=0.07, top=0.955)   # wider panels; room for top label

    # y-axis variable name as a horizontal header centred above the top panel
    fig.text(0.5, 0.99, "Density", ha="center", va="top", fontsize=9)

    path = os.path.join(OUT, "fig2_distributions.png")
    fig.savefig(path, bbox_inches="tight", dpi=300)
    plt.close(fig)
    print(f"Saved → {path}")


# ═══════════════════════════════════════════════════════════════════════════
# Figure 3 — ACF Panel
# ═══════════════════════════════════════════════════════════════════════════
def plot_acf():
    fig, axes = plt.subplots(
        nrows=7, ncols=1,
        figsize=(5.9, 9.5),
        sharex=True,
    )

    for ax, col, label in zip(axes, TARGETS, LABELS):
        s     = df[col].dropna()
        n     = len(s)
        nlags = min(52, n // 2 - 1)

        vals  = _acf(s, nlags=nlags)[1:]   # drop lag 0 (always 1 by definition)
        lags  = np.arange(1, len(vals) + 1)
        ci    = 1.96 / np.sqrt(n)

        colors = ["#333333" if abs(v) > ci else "#d0d0d0" for v in vals]
        ax.bar(lags, vals, color=colors, width=0.7)

        ax.axhline( ci, color="black", linewidth=0.5, linestyle="--")
        ax.axhline(-ci, color="black", linewidth=0.5, linestyle="--")
        ax.axhline(0,   color="black", linewidth=0.4)

        ax.set_title(label, loc="left", pad=3)
        ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=3, prune="both"))
        ax.tick_params(axis="y", pad=2)

    axes[-1].set_xlabel("Lag (weeks)", labelpad=4)

    # legend — 95% CI, bottom-right of first panel
    _ci = mlines.Line2D([], [], color="black", linewidth=0.5, linestyle="--",
                        label="95% CI")
    axes[0].legend(
        handles=[_ci],
        loc="lower right",
        fontsize=7,
        frameon=True,
        framealpha=0.0,
        edgecolor="black",
        handlelength=1.5,
        handletextpad=0.4,
        borderpad=0.4,
    )

    fig.tight_layout(h_pad=0.5)
    fig.subplots_adjust(left=0.07, top=0.955)   # wider panels; room for top label

    # y-axis variable name as a horizontal header centred above the top panel
    fig.text(0.5, 0.99, "Autocorrelation", ha="center", va="top", fontsize=9)

    path = os.path.join(OUT, "fig3_acf.png")
    fig.savefig(path, bbox_inches="tight", dpi=300)
    plt.close(fig)
    print(f"Saved → {path}")


# ═══════════════════════════════════════════════════════════════════════════
# Figure 4 — HTB feature relevance (per horizon)
# ═══════════════════════════════════════════════════════════════════════════
# Muted, thesis-friendly palette keyed to the eight feature categories so the
# bars carry the category-by-category reading used in the text.
CAT_COLORS = {
    "Price Dynamics":                 "#333333",
    "Forward Basis":                  "#1f4e79",
    "Production Input Costs":         "#8c6d31",
    "Export Flows":                   "#2e7d32",
    "Biological Supply Fundamentals": "#4aa3a2",
    "Protein Substitutes":            "#c1581a",
    "Monetary & Currency Conditions": "#6a4c93",
    "Seasonality":                    "#b0b0b0",
}


def plot_feature_importance(horizon="0w", metric="RMSE", top_n=15):
    fname = f"feature_importance_Y_{horizon}_∆_Salmon_(NOK-KG).csv"
    src   = os.path.join(BASE, "Results", "HTBoost", metric, fname)
    d     = pd.read_csv(src)

    d = d.sort_values("Importance", ascending=False).head(top_n).copy()
    d["Category"] = d["Feature"].map(_feature_category)
    d = d.iloc[::-1].reset_index(drop=True)   # largest bar at top of barh

    fig, ax = plt.subplots(figsize=(5.9, 0.32 * top_n + 0.9))

    ax.barh(d["Feature"], d["Importance"],
            color=[CAT_COLORS.get(c, "#777777") for c in d["Category"]],
            edgecolor="white", linewidth=0.4, height=0.78, zorder=2)

    ax.set_xlabel("HTB Relevance", labelpad=4)
    ax.xaxis.set_major_locator(ticker.MaxNLocator(nbins=5))
    ax.tick_params(axis="y", length=0)        # labels only, no y ticks
    ax.margins(y=0.01)

    # category legend — only categories shown, ordered top-down
    seen = list(dict.fromkeys(d["Category"].iloc[::-1]))
    handles = [mpatches.Patch(facecolor=CAT_COLORS.get(c, "#777777"),
                              edgecolor="white", label=c) for c in seen]
    ax.legend(handles=handles, loc="lower right", fontsize=6.5,
              frameon=True, framealpha=0.0, edgecolor="black",
              handlelength=1.0, handletextpad=0.5, borderpad=0.5, labelspacing=0.35)

    fig.tight_layout()
    path = os.path.join(OUT, f"fig4_feature_importance_{horizon}.png")
    fig.savefig(path, bbox_inches="tight", dpi=300)
    plt.close(fig)
    print(f"Saved → {path}")


# ═══════════════════════════════════════════════════════════════════════════
# Figure 5 — SHAP beeswarm (CatBoost, per horizon)
# ═══════════════════════════════════════════════════════════════════════════
# SHAP's native diverging colormap (blue → red, no white midpoint), so median-
# valued points stay saturated and cannot be confused with gray missing points.
SHAP_CMAP = LinearSegmentedColormap.from_list("shap_red_blue", ["#1E88E5", "#FF0D57"])


def _beeswarm_offsets(x, height=0.8, nbins=80):
    """Symmetric vertical offsets so points at similar SHAP values pile up."""
    x = np.asarray(x, dtype=float)
    offs = np.zeros(len(x))
    if len(x) == 0 or not np.isfinite(x).any():
        return offs
    lo, hi = np.nanmin(x), np.nanmax(x)
    bins = np.linspace(lo, hi + 1e-12, nbins + 1)
    idx  = np.digitize(x, bins)
    for b in np.unique(idx):
        m = np.where(idx == b)[0]
        k = len(m)
        offs[m] = np.arange(k) - (k - 1) / 2.0
    maxo = np.max(np.abs(offs))
    if maxo > 0:
        offs = offs / maxo * (height / 2.0)
    return offs


def plot_shap_beeswarm(horizon="1m", metric="RMSE", top_n=10):
    fname = f"shap_beeswarm_Y_{horizon}_∆_Salmon_(NOK-KG).csv"
    src   = os.path.join(BASE, "Results", "CatBoost", metric, fname)
    d     = pd.read_csv(src)

    order = (d.groupby("Feature")["SHAP"].apply(lambda s: s.abs().mean())
               .sort_values(ascending=False).head(top_n).index.tolist())
    order = order[::-1]   # most important on top of the axis

    fig, ax = plt.subplots(figsize=(5.9, 0.46 * top_n + 1.0))
    cmap = SHAP_CMAP

    for yi, feat in enumerate(order):
        sub = d[d["Feature"] == feat]
        sh  = sub["SHAP"].to_numpy()
        val = sub["Value"].to_numpy()

        colors = np.tile([0.6, 0.6, 0.6, 1.0], (len(val), 1))   # neutral gray = missing
        fin = np.isfinite(val)
        if fin.sum() > 1:
            lo, hi = np.nanpercentile(val[fin], [5, 95])
            norm = (np.clip((val - lo) / (hi - lo), 0, 1) if hi > lo
                    else np.full_like(val, 0.5))
            colors[fin] = cmap(norm[fin])

        y = _beeswarm_offsets(sh, height=0.8) + yi
        ax.scatter(sh, y, c=colors, s=3.5, linewidths=0, alpha=0.85, zorder=2)

    ax.axvline(0, color="black", linewidth=0.5, zorder=1)
    ax.set_yticks(range(len(order)))
    ax.set_yticklabels(order)
    ax.tick_params(axis="y", length=0)
    ax.set_xlabel("SHAP value (impact on model output)", labelpad=4)
    ax.margins(y=0.02)

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(0, 1))
    sm.set_array([])
    cb = fig.colorbar(sm, ax=ax, ticks=[0, 1], pad=0.02, fraction=0.025, aspect=30)
    cb.ax.set_yticklabels(["Low", "High"])
    cb.set_label("Feature value", fontsize=7)
    cb.outline.set_linewidth(0.5)

    fig.tight_layout()
    path = os.path.join(OUT, f"fig5_shap_beeswarm_{horizon}.png")
    fig.savefig(path, bbox_inches="tight", dpi=300)
    plt.close(fig)
    print(f"Saved → {path}")


# ═══════════════════════════════════════════════════════════════════════════
# Table 1 — Summary Statistics
# ═══════════════════════════════════════════════════════════════════════════
def table_summary_stats():
    rows = []

    for col, label in zip(TARGETS, LABELS):
        s = df[col].dropna()
        n = len(s)

        # ── four moments ──────────────────────────────────────────────────
        mu   = s.mean()
        sig  = s.std()
        sk   = float(s.skew())
        ku   = float(s.kurtosis())   # excess kurtosis

        # ── normality: Jarque-Bera ────────────────────────────────────────
        _, jb_p = stats.jarque_bera(s)

        # ── stationarity: ADF ─────────────────────────────────────────────
        adf_stat, adf_p, _, _, _, _ = adfuller(s, autolag="AIC")
        i0 = "I(0)" if adf_p < 0.05 else "I(1)"

        # ── serial correlation: Ljung-Box Q(4) and Q(52) ─────────────────
        lb = acorr_ljungbox(s, lags=[4, 13, 26, 52], return_df=True)
        lb4_p  = float(lb.loc[4,  "lb_pvalue"])
        lb13_p = float(lb.loc[13, "lb_pvalue"])
        lb26_p = float(lb.loc[26, "lb_pvalue"])
        lb52_p = float(lb.loc[52, "lb_pvalue"])

        # ── volatility clustering: ARCH-LM(4) ────────────────────────────
        try:
            _, arch_p, _, _ = het_arch(s.values, nlags=4)
        except Exception:
            arch_p = float("nan")

        rows.append({
            "Horizon"    : label,
            "N"          : n,
            "Mean"       : round(mu,       4),
            "Std"        : round(sig,      4),
            "Min"        : round(s.min(),  4),
            "Max"        : round(s.max(),  4),
            "Skewness"   : round(sk,       4),
            "Ex. Kurt"   : round(ku,       4),
            "JB p"       : round(jb_p,     4),
            "ADF stat"   : round(adf_stat, 3),
            "ADF p"      : round(adf_p,    4),
            "I(0)/I(1)"  : i0,
            "LB Q(4) p"  : round(lb4_p,   4),
            "LB Q(13) p" : round(lb13_p,  4),
            "LB Q(26) p" : round(lb26_p,  4),
            "LB Q(52) p" : round(lb52_p,  4),
            "ARCH(4) p"  : round(arch_p,  4),
        })

    result = pd.DataFrame(rows)
    path   = os.path.join(OUT, "tab1_summary_stats.csv")
    result.to_csv(path, index=False)
    print(f"Saved → {path}")
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Table 1 — Summary Statistics (paper-ready)
# ═══════════════════════════════════════════════════════════════════════════
def _fmt_p(p):
    if pd.isna(p):
        return "—"
    return "<0.001" if p < 0.001 else f"{p:.3f}"

def _fmt(x, d=3):
    return "—" if pd.isna(x) else f"{x:.{d}f}"


def table_summary_stats_paper():
    raw = table_summary_stats()

    paper = pd.DataFrame({
        "Horizon"     : raw["Horizon"],
        "N"           : raw["N"],
        "Mean (%)"    : (raw["Mean"] * 100).map(lambda x: _fmt(x, 3)),
        "Std (%)"     : (raw["Std"]  * 100).map(lambda x: _fmt(x, 3)),
        "Skewness"    : raw["Skewness"].map(lambda x: _fmt(x, 3)),
        "Ex. Kurt"    : raw["Ex. Kurt"].map(lambda x: _fmt(x, 3)),
        "JB p"        : raw["JB p"].map(_fmt_p),
        "ADF p"       : raw["ADF p"].map(_fmt_p),
        "LB Q(4) p"   : raw["LB Q(4) p"].map(_fmt_p),
        "LB Q(13) p"  : raw["LB Q(13) p"].map(_fmt_p),
        "LB Q(26) p"  : raw["LB Q(26) p"].map(_fmt_p),
        "LB Q(52) p"  : raw["LB Q(52) p"].map(_fmt_p),
        "ARCH(4) p"   : raw["ARCH(4) p"].map(_fmt_p),
    })

    path = os.path.join(OUT, "tab1_summary_stats_paper.csv")
    paper.to_csv(path, index=False)
    print(f"Saved → {path}")
    return paper


# ═══════════════════════════════════════════════════════════════════════════
# Table A1 — Feature set (appendix)
# ═══════════════════════════════════════════════════════════════════════════
# Static documentation table. Definitions/transformations mirror feature_engineer.py.
# Notation: S = FishPool spot (NOK/kg), r = Δln S (weekly log return),
#           F^τ = salmon forward at maturity τ, C^τ = commodity forward,
#           MA_k = k-week moving average. Weekly features enter at lag ≥ 1w
#           (publication-aligned, no look-ahead).
def table_feature_appendix():
    rows = [
        # Category, Variable, Definition, Publication Lag, Transformation (with lags)
        ("Price Dynamics", "∆ Salmon (NOK/KG) 1w–12m", "Lagged spot log-return (momentum)",
         "1 week", "MA_k of weekly Δln spot, lag 1w; k∈{1,2,4,13,26,52}"),
        ("Price Dynamics", "Acc 1w", "Weekly return acceleration",
         "1 week", "r(t-1) − r(t-2)"),
        ("Price Dynamics", "Acc 1m", "Monthly return acceleration",
         "1 week", "MA4(r) ending t-1 − MA4(r) ending t-5"),
        ("Price Dynamics", "RVol 4w/13w/52w", "Realized volatility",
         "1 week", "Rolling std of weekly Δln spot over 4/13/52 weeks, lag 1w"),
        ("Price Dynamics", "Spread (FP − SSB)", "FishPool–SSB price spread",
         "1 week", "(FP spot − SSB spot), lag 1w"),
        ("Price Dynamics", "∆ Spread (FP − SSB)", "Weekly change in spread",
         "1 week", "First difference of the spread, lag 1w"),

        ("Biological Supply Fundamentals", "∆ Total Biomass Monthly",
         "Month-over-month change in national biomass",
         "~20 d after month-end", "MoM Δln of standing biomass (kg)"),
        ("Biological Supply Fundamentals", "∆YOY Total Biomass Monthly",
         "Year-over-year change in biomass",
         "~20 d after month-end", "ln(B_t) − ln(B_{t−52w})"),
        ("Biological Supply Fundamentals", "Avg Weight (KG) Monthly", "Mean fish weight",
         "~20 d after month-end", "Biomass (kg) ÷ fish stock"),
        ("Biological Supply Fundamentals", "YOY Avg Weight (KG) Monthly",
         "YoY change in mean weight",
         "~20 d after month-end", "YoY Δln of average weight"),
        ("Biological Supply Fundamentals", "Harvest Intensity 1m/3m Monthly",
         "Harvest relative to biomass",
         "~20 d after month-end", "Harvest (kg) ÷ biomass (kg); 3m = 13-week MA"),
        ("Biological Supply Fundamentals", "Loss Rate 1m/3m/6m Monthly",
         "Biological loss relative to stock", "~20 d after month-end",
         "(mortality+discard+escape+other loss, N) ÷ stock; 3m=13w MA, 6m=26w MA"),
        ("Biological Supply Fundamentals", "Smolt Release 2m–18m Monthly",
         "Smolt stocked m months earlier (supply pipeline)", "~20 d after month-end",
         "ln(smolt releases) shifted m months, m=2,…,18"),
        ("Biological Supply Fundamentals", "ISA Outbreak / 1m / 3m",
         "Active ISA-infected localities",
         "1 week", "Level; 1m = 4-week MA, 3m = 13-week MA"),
        ("Biological Supply Fundamentals", "Lice Outbreak / 1m / 3m",
         "Average adult female lice per fish",
         "1 week", "Level; 1m = 4-week MA, 3m = 13-week MA"),
        ("Biological Supply Fundamentals", "Sea Temp / 12m Avg", "Sea temperature",
         "1 week", "Level; 12m = 52-week MA"),

        ("Export Flows", "∆ Export Volume 1w/2w/1m", "Norwegian export-volume change",
         "1 week", "MA_k of weekly Δln export tons, lag 1w; k∈{1,2,4}"),
        ("Export Flows", "∆ Chilean Exports 1w/2w/1m/3m", "Chilean (competitor) export change",
         "1 week", "MA_k of weekly Δln Chilean volume, lag 1w; k∈{1,2,4,13}"),

        ("Protein Substitutes", "∆ Shrimp Price (Global) 1m–12m Monthly",
         "Shrimp substitute price change",
         "~4 weeks (month-end)", "Monthly Δln; 1m level, 3/6/12-month MA"),
        ("Protein Substitutes", "∆ Broiler Price (EU) 1w–12m", "Broiler substitute price change",
         "Same week (0)", "Weekly Δln (contemporaneous); 1w level, MA over 2/4/13/26/52 weeks"),
        ("Protein Substitutes", "∆ Pig Price (EU) /1w–12m", "Pork substitute price change",
         "Same week (0)", "Weekly Δln (contemporaneous); level, MA over 2/3/5/14/27/53 weeks"),
        ("Protein Substitutes", "Meat CPI (EU) Monthly", "EU meat consumer-price inflation",
         "~10 d after month-end", "EU meat HICP, YoY % (level)"),

        ("Monetary & Currency Conditions", "∆ EURNOK /1w–12m", "EUR/NOK exchange-rate change",
         "Same week (0)", "Weekly Δln; level + MA over 1/2/4/13/26/52 weeks, lag 1w"),
        ("Monetary & Currency Conditions", "CPI NO Monthly", "Norway consumer-price inflation",
         "~10 d after month-end", "Norway CPI, YoY % (level)"),
        ("Monetary & Currency Conditions", "NIBOR 3m", "3-month Norwegian interbank rate",
         "Same week (0)", "Level"),

        ("Seasonality", "Month_01 … Month_11", "Calendar-month dummies",
         "None (deterministic)", "Indicator = 1 if month = m, for Jan–Nov (December = reference)"),

        ("Forward Basis", "FWD 1m/3m/6m/12m", "Salmon forward basis (market expectations vs spot)",
         "Same week (0)", "ln(F_t^τ / S_{t−1}), τ∈{1,3,6,12} months"),
        ("Forward Basis", "∆ FWD 1m–12m", "Weekly change in forward basis",
         "Same week (0)", "First difference of each basis"),
        ("Forward Basis", "FWD Slope", "Salmon curve slope (tilting)",
         "Same week (0)", "ln(F^12) − ln(F^1)"),
        ("Forward Basis", "FWD Curvature", "Salmon curve curvature (bending)",
         "Same week (0)", "ln(F^1) − 2·ln(F^6) + ln(F^12)"),

        ("Production Input Costs", "Brent FWD Slope", "Energy curve slope (tilting)",
         "Same week (0)", "ln(C^12 / C^1)"),
        ("Production Input Costs", "Brent FWD Curvature", "Energy curve curvature (bending)",
         "Same week (0)", "ln(C^1) − 2·ln(C^6) + ln(C^12)"),
        ("Production Input Costs", "Soybean FWD Slope", "Feed (soybean meal) curve slope (tilting)",
         "Same week (0)", "ln(C^12 / C^1)"),
        ("Production Input Costs", "Soybean FWD Curvature", "Feed curve curvature (bending)",
         "Same week (0)", "ln(C^1) − 2·ln(C^6) + ln(C^12)"),
        ("Production Input Costs", "∆ Fishmeal 1m–12m Monthly", "Fishmeal (feed input) price change",
         "~4 weeks (month-end)", "Monthly Δln; 1m level, 3/6/12-month MA"),
    ]

    appendix = pd.DataFrame(
        rows,
        columns=["Category", "Variable", "Definition", "Publication Lag", "Transformation"],
    )
    path = os.path.join(OUT, "tabA1_feature_appendix.csv")
    appendix.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"Saved → {path}")
    return appendix


# ═══════════════════════════════════════════════════════════════════════════
# Table A2 — Feature-set summary statistics (appendix)
# ═══════════════════════════════════════════════════════════════════════════
# Descriptive statistics only (no stationarity / diagnostic tests).
# Weekly features are summarised on the weekly series; monthly features are
# collapsed back to their true monthly observations first, so N and the moments
# are not inflated by the weekly forward-fill in Factors.csv.
def _feature_category(col):
    if (col.startswith("∆ Salmon (NOK/KG)") or col.startswith("Acc")
            or col.startswith("RVol") or "Spread" in col):
        return "Price Dynamics"
    if (col.startswith("∆ FWD") or col in ("FWD Slope", "FWD Curvature")
            or any(col.startswith(f"FWD {m}") for m in ("1m", "3m", "6m", "12m"))):
        return "Forward Basis"
    if col.startswith("Brent") or col.startswith("Soybean") or col.startswith("∆ Fishmeal"):
        return "Production Input Costs"
    if col.startswith("∆ Export Volume") or col.startswith("∆ Chilean"):
        return "Export Flows"
    if (any(k in col for k in ("Biomass", "Avg Weight", "Harvest Intensity", "Loss Rate"))
            or col.startswith("Smolt Release") or col.startswith("ISA")
            or col.startswith("Lice") or col.startswith("Sea Temp")):
        return "Biological Supply Fundamentals"
    if (col.startswith("∆ Shrimp") or col.startswith("∆ Broiler")
            or col.startswith("∆ Pig") or col.startswith("Meat CPI")):
        return "Protein Substitutes"
    if col.startswith("∆ EURNOK") or col.startswith("CPI NO") or col.startswith("NIBOR"):
        return "Monetary & Currency Conditions"
    if col.startswith("Month_"):
        return "Seasonality"
    return "Other"


def table_feature_summary():
    d     = pd.read_csv(DATA, parse_dates=["Date"]).sort_values("Date")
    feats = [c for c in d.columns if c != "Date" and not c.startswith("Y ")]

    n_weeks    = len(d)
    months_idx = pd.period_range(d["Date"].min(), d["Date"].max(), freq="M")
    n_months   = len(months_idx)

    cat_order = ["Price Dynamics", "Biological Supply Fundamentals", "Export Flows",
                 "Protein Substitutes", "Monetary & Currency Conditions", "Seasonality",
                 "Forward Basis", "Production Input Costs"]

    rows = []
    for col in feats:
        is_monthly = col.endswith("Monthly")

        if is_monthly:
            # Collapse the weekly forward-fill back to one value per calendar month
            s     = d.set_index("Date")[col]
            s     = s.groupby(s.index.to_period("M")).last().reindex(months_idx)
            valid = s.dropna()
            denom = n_months
        else:
            valid = d[col].dropna()
            denom = n_weeks

        n       = len(valid)
        pct_nan = 100 * (denom - n) / denom if denom else float("nan")

        rows.append({
            "Category" : _feature_category(col),
            "Feature"  : col,
            "Freq"     : "Monthly" if is_monthly else "Weekly",
            "N"        : n,
            "% NaN"    : round(pct_nan, 1),
            "Mean"     : round(valid.mean(),  4) if n else float("nan"),
            "Std"      : round(valid.std(),   4) if n > 1 else float("nan"),
            "Min"      : round(valid.min(),   4) if n else float("nan"),
            "Max"      : round(valid.max(),   4) if n else float("nan"),
            "Skewness" : round(float(valid.skew()),     3) if n > 2 else float("nan"),
            "Ex. Kurt" : round(float(valid.kurtosis()), 3) if n > 3 else float("nan"),
        })

    res = pd.DataFrame(rows)
    res["__o"] = res["Category"].map({c: i for i, c in enumerate(cat_order)})
    res = (res.sort_values("__o", kind="stable")
              .drop(columns="__o")
              .reset_index(drop=True))

    path = os.path.join(OUT, "tabA2_feature_summary.csv")
    res.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"Saved → {path}")
    return res


# ═══════════════════════════════════════════════════════════════════════════
# Table 2 — Diebold-Mariano p-values vs the random walk
# ═══════════════════════════════════════════════════════════════════════════
# One-sided DM test (H1: model beats RW), read from the model-comparison output.
# Values formatted with _fmt_p ("<0.001" / 3 dp / "—" for models absent at a
# horizon, e.g. OLS at 0w–2w). Bold p<0.05 is applied manually when pasting.
def table_dm_pvalues():
    src = os.path.join(BASE, "Results", "Comparison", "comparison_summary.csv")
    d   = pd.read_csv(src)

    models = ["CatBoost", "HTBoost", "Lasso", "SARIMA", "OLS"]
    out = pd.DataFrame({"Horizon": d["Horizon"]})
    for m in models:
        col = f"{m} DM p"
        out[m] = d[col].map(_fmt_p) if col in d.columns else "—"

    path = os.path.join(OUT, "tab2_dm_pvalues.csv")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"Saved → {path}")
    return out


# ═══════════════════════════════════════════════════════════════════════════
# Table 3 — OLS forward-basis regression (Y_h = α + β·FWD_h)
# ═══════════════════════════════════════════════════════════════════════════
# Estimation sample (pre-holdout). p-value is for H0: β = 1 (Expectations
# Hypothesis), not β = 0 — flagged in the caption.
def table_ols_regression():
    src = os.path.join(BASE, "Results", "OLS", "ols_summary.csv")
    d   = pd.read_csv(src)

    out = pd.DataFrame({
        "Horizon"   : d["Horizon"],
        "Regressor" : d["FWD"],
        "α"         : d["α"].round(4),
        "β"         : d["β"].round(3),
        "SE(β)"     : d["SE(β)"].round(3),
        "p(β=1)"    : d["p(β=1)"].map(_fmt_p),
        "N"         : d["n_train"],
    })

    path = os.path.join(OUT, "tab3_ols_regression.csv")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"Saved → {path}")
    return out


# ═══════════════════════════════════════════════════════════════════════════
# Table 4 — Out-of-sample metrics for all models and horizons
# ═══════════════════════════════════════════════════════════════════════════
# Models ranked by holdout RMSE within each horizon. OLS exists only at 1m–12m.
def table_oos_metrics():
    src = os.path.join(BASE, "Results", "Comparison", "comparison_summary.csv")
    d   = pd.read_csv(src)
    models = ["CatBoost", "HTBoost", "Lasso", "SARIMA", "OLS"]

    rows = []
    for _, r in d.iterrows():
        recs = []
        for m in models:
            rmse = r.get(f"{m} RMSE")
            if pd.isna(rmse):
                continue
            recs.append({
                "Horizon"         : r["Horizon"],
                "Model"           : m,
                "RMSE"            : round(rmse, 4),
                "MAE"             : round(r[f"{m} MAE"], 4),
                "R²"              : round(r[f"{m} R²"], 3),
                "Skill vs RW (%)" : round(r[f"{m} Skill"], 1),
                "Hit (%)"         : round(r[f"{m} Hit"] * 100, 1),
                "DM p"            : _fmt_p(r[f"{m} DM p"]),
            })
        rows.extend(sorted(recs, key=lambda x: x["RMSE"]))   # rank by RMSE

    out  = pd.DataFrame(rows)
    path = os.path.join(OUT, "tab4_oos_metrics.csv")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"Saved → {path}")
    return out


# ═══════════════════════════════════════════════════════════════════════════
# Figure 6 — Out-of-sample R² by model and horizon
# ═══════════════════════════════════════════════════════════════════════════
MODEL_COLORS = {
    "CatBoost": "#1f4e79",
    "HTBoost":  "#2e7d32",
    "Lasso":    "#c1581a",
    "SARIMA":   "#b0b0b0",
    "OLS":      "#6a4c93",
}


def plot_r2_by_horizon():
    src = os.path.join(BASE, "Results", "Comparison", "comparison_summary.csv")
    d   = pd.read_csv(src)

    horizons = list(d["Horizon"])
    models   = ["CatBoost", "HTBoost", "Lasso", "SARIMA", "OLS"]
    x        = np.arange(len(horizons))
    n        = len(models)
    width    = 0.16

    fig, ax = plt.subplots(figsize=(5.9, 3.6))
    for i, m in enumerate(models):
        vals = d[f"{m} R²"].to_numpy()
        off  = x + (i - (n - 1) / 2.0) * width
        ax.bar(off, vals, width, label=m, color=MODEL_COLORS[m],
               edgecolor="white", linewidth=0.3, zorder=2)

    # random-walk reference (R² = 0)
    ax.axhline(0, color="black", linewidth=0.6, linestyle="--", zorder=1)

    ax.set_xticks(x)
    ax.set_xticklabels(horizons)
    ax.set_xlabel("Forecast horizon", labelpad=4)
    ax.set_ylabel(r"Out-of-sample $R^2$", labelpad=4)
    ax.legend(loc="upper left", fontsize=6.5, frameon=True, framealpha=0.0,
              edgecolor="black", ncol=2, handlelength=1.1, handletextpad=0.4,
              columnspacing=1.0, borderpad=0.4)

    fig.tight_layout()
    path = os.path.join(OUT, "fig6_r2_by_horizon.png")
    fig.savefig(path, bbox_inches="tight", dpi=300)
    plt.close(fig)
    print(f"Saved → {path}")


if __name__ == "__main__":
    plot_timeseries()
    plot_distributions()
    plot_acf()
    table_summary_stats_paper()
    table_feature_appendix()
    table_feature_summary()
    table_dm_pvalues()
    table_ols_regression()
    table_oos_metrics()
    plot_r2_by_horizon()
    plot_feature_importance("0w", "RMSE", 10)
    plot_feature_importance("1w", "RMSE", 10)
    plot_feature_importance("2w", "RMSE", 10)
    plot_shap_beeswarm("1m", "RMSE", 10)
    plot_shap_beeswarm("12m", "RMSE", 10)
