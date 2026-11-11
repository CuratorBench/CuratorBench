"""Two reviewable WWW figure proposals; leaves the manuscript unchanged."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from pathlib import Path
from statistics import mean, median

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/curatorbench-matplotlib")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Circle, FancyArrowPatch, FancyBboxPatch, Rectangle
from style import COL_IN, FONT_PT, INK, INK2, PREVIEW_DPI, ROOT

OUT = ROOT / "figures/drafts/redesign"
BLUE = "#256abf"
NAVY = "#184f95"
PALE = "#f1f6fc"
LINE = "#cedbec"
GREY = "#f6f6f5"
VAULTS = (
    "0x4ef53d2caa51c447fdfeeedee8f07fd1962c9ee6",
    "0xbeeff047c03714965a54b671a37c18bef6b96210",
)
START, END = "2026-01-23", "2026-10-01"
plt.rcParams.update({
    "font.family": "sans-serif", "font.sans-serif": ["Arial", "DejaVu Sans"],
    "font.size": FONT_PT, "pdf.fonttype": 42, "ps.fonttype": 42,
    "svg.fonttype": "none", "text.color": INK,
})


def canvas(width, height):
    fig = plt.figure(figsize=(width / 72, height / 72), facecolor="white")
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set(xlim=(0, width), ylim=(height, 0))
    ax.axis("off")
    return fig, ax


def text(ax, x, y, label, size=8, color=INK, bold=False, ha="left", **kw):
    return ax.text(x, y, label, fontsize=size, color=color, ha=ha,
                   va="center", weight="bold" if bold else "normal", **kw)


def box(ax, x, y, w, h, fill="white", edge=LINE, radius=3, lw=.65):
    p = FancyBboxPatch((x, y), w, h,
                      boxstyle=f"round,pad=0,rounding_size={radius}",
                      facecolor=fill, edgecolor=edge, linewidth=lw)
    ax.add_patch(p)
    return p


def line(ax, x1, y1, x2, y2, color=LINE, lw=.65, **kw):
    ax.plot([x1, x2], [y1, y2], color=color, lw=lw, **kw)


def arrow(ax, a, b, color=INK2, dashed=False, both=False):
    ax.add_patch(FancyArrowPatch(a, b, arrowstyle="<->" if both else "-|>",
                                mutation_scale=7, linewidth=.85, color=color,
                                linestyle=(0, (2, 2)) if dashed else "-",
                                shrinkA=0, shrinkB=0))


def human(ax, x, y, color=INK2, scale=1):
    ax.add_patch(Circle((x, y-5*scale), 3.3*scale, fill=False,
                        edgecolor=color, linewidth=.9))
    ax.plot([x-6*scale, x-5*scale, x, x+5*scale, x+6*scale],
            [y+7*scale, y+2*scale, y, y+2*scale, y+7*scale],
            color=color, lw=.9)


def robot(ax, x, y):
    box(ax, x-12, y-8, 24, 17, fill="white", edge=BLUE, radius=4, lw=1)
    line(ax, x, y-8, x, y-13, BLUE, .9)
    ax.add_patch(Circle((x, y-15), 1.5, facecolor=BLUE, edgecolor=BLUE))
    for dx in [-5, 5]:
        ax.add_patch(Circle((x+dx, y-1), 1.5, facecolor=BLUE, edgecolor=BLUE))
    line(ax, x-4, y+5, x+4, y+5, BLUE, .9)
    for dx in [-15, 15]:
        line(ax, x+dx, y-3, x+dx, y+4, BLUE, 1.2)


def save(fig, name, title):
    fig.savefig(OUT / f"{name}.pdf", metadata={
        "Title": title, "Author": "", "Creator": "", "Producer": "",
        "CreationDate": None, "ModDate": None})
    fig.savefig(OUT / f"{name}.svg")
    fig.savefig(OUT / f"{name}.png", dpi=PREVIEW_DPI)
    plt.close(fig)


def matched_pair():
    summary = ROOT / "data/metrics/e4/vault_scorecard.csv"
    daily = ROOT / "data/metrics/e4/vault_daily.csv"
    with summary.open() as f:
        registry = {r["vault"]: r for r in csv.DictReader(f)
                    if r["chain_id"] == "1" and r["vault"] in VAULTS}
    selected = {v: [] for v in VAULTS}
    with daily.open() as f:
        for r in csv.DictReader(f):
            if (r["chain_id"] == "1" and r["vault"] in selected
                    and START <= r["date"] <= END and r["active"] == "1"):
                selected[r["vault"]].append(r)
    dates = [{r["date"] for r in selected[v]} for v in VAULTS]
    assert dates[0] == dates[1] and len(dates[0]) == 252
    result = []
    for v in VAULTS:
        rows = selected[v]
        assert len(rows) == 252
        assert registry[v]["protocol"] == "morpho"
        assert registry[v]["asset_symbol"] == "USDC"
        assert all(r["apy_source"] == "morpho_api" for r in rows)
        returns = [float(r["ret_after_fee"]) for r in rows
                   if r["ret_in_yield"] == "1"]
        result.append({
            "vault": v, "name": registry[v]["name"],
            "curator": registry[v]["curator_name"],
            "days": len(rows),
            "median_displayed_apy": median(float(r["vault_apy"]) for r in rows),
            "mean_stress_tail_loss": mean(float(r["tl_headline"]) for r in rows),
            "withdrawable_p10": float(np.percentile([float(r["L"]) for r in rows], 10)),
            "annualized_net_yield": math.expm1(sum(math.log1p(r) for r in returns)
                                               * 365 / len(returns)),
        })
    metadata = {
        "start": START, "end": END, "protocol": "Morpho", "chain": "Ethereum",
        "loan_asset": "USDC", "vaults": result,
        "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in (summary, daily)},
        "selection": "Illustrative pair selected for similar APY and different stress loss "
                     "and liquidity among same-protocol, same-chain, same-asset vaults with "
                     "at least 180 active days and at least USD 10M mean net AUM in the "
                     "full scorecard; then recomputed on the common 252-day window.",
        "interpretation": "Scenario losses are not observed losses or failure probabilities. "
                          "Liquidity is the 10th percentile of the withdrawable share, not "
                          "a loss rate. This pair is illustrative, not representative.",
        "overview": "Recorded human positions and replayed automated positions receive "
                    "the same four metrics. LLMs are evaluated policies, not metric judges. "
                    "No model performance is asserted. Based on the paper text.",
    }
    (OUT / "teaser.pdf.meta.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return result


def teaser(pair):
    w, h = COL_IN * 72, 196
    fig, ax = canvas(w, h)
    text(ax, 2, 10, "Similar APY, different risks", 11, NAVY, True)
    text(ax, 2, 25, "Two Morpho USDC vaults on Ethereum", 8, INK2)
    centers = [120, 200]
    labels = [("KPK USDC", "Prime"), ("Waterline", "Reservoir USDC")]
    for x, label, d in zip(centers, labels, pair):
        box(ax, x-36, 38, 72, 61, PALE, edge=PALE)
        text(ax, x, 48, label[0], 7.5, NAVY, True, "center")
        text(ax, x, 59, label[1], 7.5, NAVY, True, "center")
        text(ax, x, 82, f"{100*d['median_displayed_apy']:.2f}%", 17, NAVY, True, "center")
    text(ax, 2, 77, "Displayed APY", 8, bold=True)
    text(ax, 2, 89, "median", 7.3, INK2)
    line(ax, 2, 106, w-2, 106)
    text(ax, 2, 117, "CURATORBENCH", 7.4, BLUE, True)
    for y, title, sub, key in [
            (137, "Stress loss", "scenario estimate", "mean_stress_tail_loss"),
            (168, "Withdrawable", "10th percentile", "withdrawable_p10")]:
        text(ax, 2, y-1, title, 8, bold=True)
        text(ax, 2, y+10, sub, 7, INK2)
        for x, d in zip(centers, pair):
            text(ax, x, y-2, f"{100*d[key]:.1f}%", 11.5, NAVY, True, "center")
            ax.add_patch(Rectangle((x-30, y+8), 60, 4.5, facecolor=LINE, edgecolor="none"))
            ax.add_patch(Rectangle((x-30, y+8), 60*d[key], 4.5, facecolor=BLUE, edgecolor="none"))
    text(ax, 2, 191, "Same 252 days: 23 Jan - 1 Oct 2026", 7.4, INK2)
    save(fig, "teaser", "Similar APY, different risks")


def overview():
    from fig_overview import draw
    draw()


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    pair = matched_pair()
    teaser(pair)
    overview()
    print(json.dumps({"output": str(OUT), "pair": pair}, indent=2))
