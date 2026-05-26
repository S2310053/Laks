import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
import matplotlib.lines as mlines
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

    # shared y-axis label centred on the figure
    fig.text(0.01, 0.5, r"$\Delta \ln$ Salmon Spot Price",
             va="center", ha="left", rotation="vertical", fontsize=9)

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
    fig.subplots_adjust(left=0.13)   # room for the shared y label

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

    fig.text(0.01, 0.5, "Density",
             va="center", ha="left", rotation="vertical", fontsize=9)

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
    fig.subplots_adjust(left=0.13)

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

    fig.text(0.01, 0.5, "Autocorrelation",
             va="center", ha="left", rotation="vertical", fontsize=9)

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
    fig.subplots_adjust(left=0.13)

    path = os.path.join(OUT, "fig3_acf.png")
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


if __name__ == "__main__":
    plot_timeseries()
    plot_distributions()
    plot_acf()
    table_summary_stats_paper()
