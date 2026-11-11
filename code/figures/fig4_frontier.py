# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.8", "numpy>=1.26"]
# ///
"""Fig: realized yield against tail loss (left) and against liquidity (right), two panels sharing the yield axis in one col."""
from __future__ import annotations

import csv
import json
import math

import numpy as np

import style

AUM_FLOOR = 10e6
AREA_PT2_PER_USD = 180.0 / 400e6
MIN_AREA_PT2 = 2.5
SIZE_KEYS_USD = [10e6, 100e6, 400e6]
Y_LO, Y_HI = -2.0, 12.0
X_TICKS = [0, 1, 2, 5, 10, 20, 30, 40, 50]


def num(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def area(usd):
    return max(MIN_AREA_PT2, AREA_PT2_PER_USD * usd)


def main(metrics_dir="metrics/e4", name="fig4_frontier", panel="both"):
    """panel: "both" (the paper figure), or "tail" or "liquidity" alone."""
    fig4 = style.DATA_OUT / metrics_dir / "fig4_data.csv"
    rank = style.DATA_OUT / metrics_dir / "rank_agreement.json"
    plt = style.setup()
    from matplotlib.lines import Line2D
    from matplotlib.scale import FuncScale

    with open(fig4, newline="") as fh:
        rows = list(csv.DictReader(fh))
    for r in rows:
        r["x"] = num(r["tl_headline"]) * 100
        r["y"] = num(r["yield_after_fee"]) * 100
        r["aum"] = num(r["aum_mean_usd"]) or 0.0
        r["L"] = (num(r["L_p10"]) or 0.0) * 100
    nd = json.loads(rank.read_text())["aum_floor_10m"]["non_dominated"]
    nd_keys = {(str(v["chain_id"]), v["vault"].lower()) for v in nd["vaults"]}
    for r in rows:
        r["nd"] = (r["chain_id"], r["vault"].lower()) in nd_keys
    shown = [r for r in rows if Y_LO <= r["y"] <= Y_HI]
    clipped = len(rows) - len(shown)
    floor = [r for r in rows if r["aum"] >= AUM_FLOOR]
    nd_rows = [r for r in rows if r["nd"]]
    nd_share = nd["aum_share"]

    fwd = lambda v: np.sqrt(np.clip(v, 0, None))
    inv = lambda v: np.square(v)
    kinds = ["tail", "liquidity"] if panel == "both" else [panel]
    k = len(kinds)
    W, H = style.COL_IN, (2.35 if k == 2 else 2.55)
    left, right, bottom, top, mtop, mright, gap, pgap = 0.40, 0.03, 0.74, 0.03, 0.14, 0.16, 0.04, 0.07
    aw = (W - left - right - mright - gap - pgap * (k - 1)) / k
    ah = H - bottom - top - mtop - gap
    fig = plt.figure(figsize=(W, H))
    axes, tops = [], []
    for i in range(k):
        x0 = left + i * (aw + pgap)
        a_ = fig.add_axes([x0 / W, bottom / H, aw / W, ah / H], sharey=axes[0] if axes else None)
        tops.append(fig.add_axes([x0 / W, (bottom + ah + gap) / H, aw / W, mtop / H], sharex=a_))
        axes.append(a_)
    axr = fig.add_axes([(left + k * aw + (k - 1) * pgap + gap) / W, bottom / H, mright / W, ah / H],
                       sharey=axes[0])

    keys = []
    for a_, kind in zip(axes, kinds):
        if kind == "tail":
            x_max = 5 * math.ceil(max(r["x"] for r in rows) / 5)
            ticks = X_TICKS if k == 1 else [0, 2, 5, 10, 20, 40]
            a_.set_xscale("function", functions=(fwd, inv))
            a_.set_xlim(0, x_max)
            a_.set_xticks([t for t in ticks if t <= x_max])
            a_.set_xticklabels([str(t) for t in ticks if t <= x_max])
            a_.set_xlabel("Tail loss (%, square-root scale)" if k == 1 else "Tail loss (%, sqrt scale)")
            keys.append("x")
        else:
            a_.set_xlim(-2, 102)
            a_.set_xticks([0, 25, 50, 75, 100] if k == 1 else [0, 50, 100])
            a_.set_xlabel("Liquidity (% withdrawable, 10th percentile)" if k == 1 else "Liquidity (%)")
            keys.append("L")
        a_.yaxis.grid(True, zorder=0)
        a_.xaxis.grid(True, zorder=0)
        a_.axhline(0, color=style.AXIS, lw=0.6, zorder=1)
    ax = axes[0]
    ax.set_ylim(Y_LO, Y_HI)
    ax.set_ylabel("Realized yield (%)")
    ax.set_yticks([0, 2.5, 5, 7.5, 10])
    ax.set_yticklabels(["0", "2.5", "5", "7.5", "10"])
    for a_ in axes[1:]:
        a_.tick_params(axis="y", labelleft=False)
    for a_, key in zip(axes, keys):
        for r in sorted(shown, key=lambda r: -r["aum"]):
            a_.scatter([r[key]], [r["y"]], s=area(r["aum"]) / k, marker="o", color=style.PROTOCOL_COLOR[r["protocol"]],
                       alpha=0.55, edgecolors=style.SURFACE, linewidths=0.3, zorder=3)
        for r in nd_rows:
            if Y_LO <= r["y"] <= Y_HI:
                a_.scatter([r[key]], [r["y"]], s=area(r["aum"]) / k + 5, marker="o", facecolors="none",
                           edgecolors=style.INK, linewidths=0.8, zorder=4)

    def box(axm, vals, vertical):
        q5, q25, q50, q75, q95 = np.percentile(vals, [5, 25, 50, 75, 95])
        kw = dict(color=style.INK2, lw=0.8, solid_capstyle="butt")
        if vertical:
            axm.plot([0.5, 0.5], [q5, q25], **kw); axm.plot([0.5, 0.5], [q75, q95], **kw)
            axm.add_patch(plt.Rectangle((0.2, q25), 0.6, q75 - q25, fc=style.GRID, ec=style.INK2, lw=0.8))
            axm.plot([0.2, 0.8], [q50, q50], color=style.INK, lw=1.2)
            axm.set_xlim(0, 1)
        else:
            axm.plot([q5, q25], [0.5, 0.5], **kw); axm.plot([q75, q95], [0.5, 0.5], **kw)
            axm.add_patch(plt.Rectangle((q25, 0.2), q75 - q25, 0.6, fc=style.GRID, ec=style.INK2, lw=0.8))
            axm.plot([q50, q50], [0.2, 0.8], color=style.INK, lw=1.2)
            axm.set_ylim(0, 1)
        axm.set_axis_off()
        return [float(q) for q in (q5, q25, q50, q75, q95)]
    qx = {key: box(t, [r[key] for r in rows], vertical=False) for t, key in zip(tops, keys)}
    qy = box(axr, [r["y"] for r in rows], vertical=True)

    handles = [Line2D([], [], ls="", marker="o", ms=5, mfc=style.PROTOCOL_COLOR[p], mec=style.SURFACE,
                      mew=0.4, alpha=0.7) for p in style.PROTOCOLS]
    labels = [style.PROTOCOL_LABEL[p] for p in style.PROTOCOLS]
    handles.append(Line2D([], [], ls="", marker="o", ms=5, mfc="none", mec=style.INK, mew=0.9))
    labels.append("Non-dominated")
    fig.legend(handles, labels, loc="lower left", ncol=5, bbox_to_anchor=(0.02 / W, 0.2 / H),
               handletextpad=0.1, columnspacing=0.55, borderaxespad=0, fontsize=6.5)
    fig.legend([Line2D([], [], ls="", marker="")]
               + [Line2D([], [], ls="", marker="o", ms=math.sqrt(area(u) / k), mfc=style.MUTED, mec=style.SURFACE,
                         alpha=0.55) for u in SIZE_KEYS_USD],
               ["Mark area, mean AUM:"] + [f"${u / 1e6:,.0f}M" for u in SIZE_KEYS_USD],
               loc="lower left", ncol=4, bbox_to_anchor=(0.02 / W, 0.0 / H), handlelength=2.4,
               handletextpad=0.3, columnspacing=1.1, borderaxespad=0, fontsize=6.5)

    variant = {} if name == "fig4_frontier" else {"generator": "code/figures/fig4_frontier.py",
                                                   "metrics_dir": metrics_dir}
    style.save(fig, name, [fig4, rank],
               f"Realized yield against {' and '.join('tail loss' if c == 'tail' else 'liquidity' for c in kinds)}, one mark per sample vault; "
               "non-dominated vaults ringed",
               {**variant, "vaults": len(rows), "vaults_clipped_y": clipped, "y_range_pct": [Y_LO, Y_HI],
                "aum_floor_usd": AUM_FLOOR, "vaults_at_or_above_floor": len(floor),
                "non_dominated_floor": len(nd_rows), "non_dominated_floor_aum_share": nd_share,
                "aum_floor_total_usd": sum(r["aum"] for r in floor),
                "margin_quantiles_5_25_50_75_95": {**{("tail_loss_pct" if key == "x" else "liquidity_pct"): q
                                                      for key, q in qx.items()},
                                                   "realized_yield_pct": qy}})
    plt.close(fig)
    print(f"fig: {len(rows)} vaults, {clipped} outside y range; non-dominated >= $10M, >= 90 days: {len(nd_rows)} of "
          f"{nd['of']}, {nd_share:.1%} of their AUM")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Realized yield against tail loss")
    ap.add_argument("--metrics-dir", default="metrics/e4", help="E4 output directory under data/")
    ap.add_argument("--name", default="fig4_frontier", help="output name in figures/")
    a = ap.parse_args()
    main(a.metrics_dir, a.name)
