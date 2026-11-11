# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.8", "numpy>=1.26"]
# ///
"""Teaser: human curators and LLM curators at a glance (one column, three radars in a row)."""
from __future__ import annotations

import csv
import gzip
import json
import math
import statistics as st
from pathlib import Path

import numpy as np

import style
import fig3_radar as R

RES = style.DATA_OUT / "autocurator/results"
RUNS = style.DATA_OUT / "autocurator/runs"
LOGOS = style.ROOT / "figures/assets/model-logos"
SPAN = "common"
TOL = 1e-6

RADAR = [("claude-opus-4-6-medium", "Claude Opus 4.6", "#D97757"),
         ("gpt-5.4-2026-03-05-medium", "GPT-5.4", "#10A37F"),
         ("gemini-2.5-flash-medium", "Gemini 2.5 Flash", "#8E6BD8"),
         ("deepseek-v3.1-thinking", "DeepSeek V3.1", "#4D6BFE")]
SCATTER = [("claude-opus-4-6-medium", "claude-color"), ("gpt-5.4-2026-03-05-medium", "openai"),
           ("gemini-2.5-flash-medium", "gemini-color"), ("deepseek-v3.1-thinking", "deepseek-color")]
NUDGE = {"claude-opus-4-6-medium": (-10, 5), "deepseek-v3.1-thinking": (3, 10), "gpt-5.4-2026-03-05-medium": (9, -6),
         "gemini-2.5-flash-medium": (8, 6)}
BASELINES = [("baseline-max_apy@common", "Max-APY"), ("baseline-rule_based@common", "Rule-based")]
TEASER_CURATORS = [(c, col) for c, col in zip(R.SHOWN, R.SERIES) if c != "telosc"]
H_EVENT_LOSS = None


def num(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def read(name):
    with open(RES / name) as fh:
        return list(csv.DictReader(r for r in fh if not r.startswith("#")))


def pctile(v, hs, better):
    hs = [h for h in hs if h is not None]
    if v is None or not hs:
        return None
    beat = sum(1 for h in hs if abs(v - h) > TOL and ((v < h) if better == "lower" else (v > h)))
    tie = sum(1 for h in hs if abs(v - h) <= TOL)
    return 100.0 * (beat + 0.5 * tie) / len(hs)


def aggressive(label):
    return label.replace("-medium", "-medium-aggressive") if label.endswith("-medium") else label + "-aggressive"


def radar_values(t6, hum):
    """Raw values on the axes of (b) and (c), in (a)'s order with cost in the place of displayed APY."""
    hmed = lambda col: st.median(st.median(v for h in hs if (v := num(h[col])) is not None) for hs in hum.values())
    out = {("human", None): [None, 0.0, 0.0, hmed("L_p10"), 100 * H_EVENT_LOSS]}
    for label, _n, _c in RADAR:
        for mandate, lab in (("default", label), ("high-yield", aggressive(label))):
            r = t6[lab]
            out[(label, mandate)] = [inference_per_year(lab), num(r["median_yield_gap_pp"]),
                                     -num(r["median_tail_loss_gap_pp"]), num(r["mean_vault_L_p10"]),
                                     100 * num(r["mean_loss_per_dollar"])]
    return out


BETTER = [-1, 1, 1, 1, -1]


FLOOR = 0.15
INNER = 12


def scaled(vals, runs):
    """Radii for one radar, centred on the human median (radius 50, dashed)."""
    out = {k: [None] * 5 for k in runs}
    for i in range(5):
        if i == 0:
            centre = st.median(math.log(vals[k][0]) for k in runs)
            d = {k: -(math.log(vals[k][0]) - centre) for k in runs}
        else:
            h = vals[("human", None)][i]
            d = {k: BETTER[i] * (vals[k][i] - h) for k in runs}
        pos = [x for x in d.values() if x > 0]
        neg = [-x for x in d.values() if x < 0]
        for k in runs:
            x = d[k]
            if x > 0:
                lo, hi = min(pos), max(pos)
                f = 1.0 if hi == lo else FLOOR + (1 - FLOOR) * (x - lo) / (hi - lo)
                out[k][i] = 50 + 50 * f
            elif x < 0:
                lo, hi = min(neg), max(neg)
                f = 1.0 if hi == lo else FLOOR + (1 - FLOOR) * (-x - lo) / (hi - lo)
                out[k][i] = 50 - (50 - INNER) * f
            else:
                out[k][i] = 50.0
    return out


def inference_per_year(label):
    """Inference at list price per vault-year, over all groups of the run (USD)."""
    cost = days = 0.0
    for p in (RUNS / label).glob("*__r1.json.gz"):
        m = json.load(gzip.open(p, "rt"))["meta"]
        cost += float(m.get("cost_usd") or 0)
        days += m["last_day"] - m["first_day"] + 1
    return cost / days * 365


def logo_image(name, size_pt, dpi=600):
    """The vendor's logo as an OffsetImage of `size_pt` points, downsampled once with Lanczos so it is smooth at print resolution."""
    from PIL import Image
    from matplotlib.offsetbox import OffsetImage
    px = max(8, int(round(size_pt / 72 * dpi)))
    im = Image.open(LOGOS / f"{name}.png").convert("RGBA").resize((px, px), Image.LANCZOS)
    return OffsetImage(np.asarray(im) / 255.0, zoom=72 / dpi, dpi_cor=True, resample=True,
                       interpolation="antialiased")


def human_cloud(hum):
    xs, ys = [], []
    for g, hs in hum.items():
        yv = [num(h["yield_before_fee"]) for h in hs if num(h["yield_before_fee"]) is not None]
        tv = [num(h["tl_headline"]) for h in hs if num(h["tl_headline"]) is not None]
        if not yv or not tv:
            continue
        my, mt = st.median(yv), st.median(tv)
        for h in hs:
            if num(h["yield_before_fee"]) is not None and num(h["tl_headline"]) is not None:
                ys.append(100 * (num(h["yield_before_fee"]) - my))
                xs.append(100 * (num(h["tl_headline"]) - mt))
    return np.array(xs), np.array(ys)


def rows_first(handles, ncol):
    """Reorder legend handles so that matplotlib, which fills columns first, shows them row by row."""
    nrow = -(-len(handles) // ncol)
    grid = [handles[r * ncol:(r + 1) * ncol] for r in range(nrow)]
    return [grid[r][c] for c in range(ncol) for r in range(nrow) if c < len(grid[r])]


def radar_axes(fig, rect, n):
    ax = fig.add_axes(rect, polar=True)
    ang = [(math.pi / 2 - 2 * math.pi * k / n) % (2 * math.pi) for k in range(n)]
    ax.set_ylim(0, 100)
    ax.set_yticks([25, 50, 75, 100])
    ax.set_yticklabels([])
    ax.set_xticks(ang)
    ax.set_xticklabels([])
    ax.grid(color="#D9D9D9", lw=0.4)
    ax.spines["polar"].set_color("#BDBDB0")
    ax.spines["polar"].set_linewidth(0.5)
    return ax, ang


def poly(ax, ang, vals, **kw):
    a = list(ang) + [ang[0]]
    v = list(vals) + [vals[0]]
    ax.plot(a, v, **kw)


def label_axes(ax, ang, names, fs, pad=118):
    for a, t in zip(ang, names):
        ha = "center" if abs(math.cos(a)) < 0.3 else ("left" if math.cos(a) > 0 else "right")
        va = "bottom" if math.sin(a) > 0.3 else ("top" if math.sin(a) < -0.3 else "center")
        ax.text(a, pad, t, ha=ha, va=va, fontsize=fs, linespacing=0.9, color=style.INK if hasattr(style, "INK") else "#222")


def main():
    plt = style.setup()
    from matplotlib.lines import Line2D
    from matplotlib.offsetbox import AnnotationBbox, OffsetImage
    from matplotlib.image import imread

    gs, hs_rows, ev = read("group_span.csv"), read("humans_span.csv"), read("outcomes_events.csv")
    groups = {r["group"] for r in gs if r["span"] == SPAN and r["label"] == "baseline-max_apy@common"}
    hum = {}
    for h in hs_rows:
        if h["span"] == SPAN and h["group"] in groups:
            hum.setdefault(h["group"], []).append(h)
    t6 = {r["label"]: r for r in read("table6.csv") if r["span"] == SPAN}
    global H_EVENT_LOSS
    ids = {e["event_id"] for e in ev if e["span"] == SPAN and e["label"] == "baseline-max_apy@common"}
    per = {}
    for h in read("humans_events.csv"):
        if h["event_id"] in ids:
            per.setdefault(h["event_id"], []).append(num(h["loss_per_dollar"]) or 0.0)
    H_EVENT_LOSS = st.mean(st.mean(per[e]) if e in per else 0.0 for e in ids)
    raw = radar_values(t6, hum)
    rv = {}
    for mandate in ("default", "high-yield"):
        rv.update(scaled(raw, [(label, mandate) for label, _n, _c in RADAR]))

    FS = 6.1
    W = style.COL_IN
    RW = 0.255
    KEY_IN, LEG_TOP_IN, TITLE_TOP_IN, RADAR_IN, TOP_IN = 0.01, 0.42, 0.55, 0.66, 0.14
    H = RADAR_IN + RW * W + TOP_IN
    fig = plt.figure(figsize=(W, H))
    RH = RW * W / H
    Y0 = RADAR_IN / H
    MEDIAN = dict(color="#7F7F7F", lw=0.75, ls=(0, (2.2, 1.4)))
    XS = (0.04, 0.3725, 0.705)

    cs = R.load()
    pools = {m: [R.num(r[m]) if m != "event_resilience" else r[m] for r in cs] for m, _l, _s in R.AXES}
    by_id = {r["curator_entity"]: r for r in cs}
    axa, anga = radar_axes(fig, [XS[0], Y0, RW, RH], len(R.AXES))
    med = []
    for m, _lab, sg in R.AXES:
        vals = sorted(v for v in pools[m] if v is not None)
        mv = vals[len(vals) // 2] if len(vals) % 2 else (vals[len(vals) // 2 - 1] + vals[len(vals) // 2]) / 2
        med.append(R.pct_of(mv, pools[m], sg))
    for cid, col in TEASER_CURATORS:
        r = by_id[cid]
        v = [R.pct_of(R.num(r[m]) if m != "event_resilience" else r[m], pools[m], sg) for m, _l, sg in R.AXES]
        poly(axa, anga, v, color=col, lw=0.7)
    poly(axa, anga, med, **MEDIAN)
    label_axes(axa, anga, ["A", "Y", "S", "L", "R"], FS, pad=113)

    for x, mandate in ((XS[1], "default"), (XS[2], "high-yield")):
        ax, an = radar_axes(fig, [x, Y0, RW, RH], 5)
        poly(ax, an, [50] * 5, **MEDIAN)
        for label, _n, col in RADAR[1:] + RADAR[:1]:
            poly(ax, an, rv[(label, mandate)], color=col, lw=0.8)
        label_axes(ax, an, ["C", "Y", "S", "L", "R"], FS, pad=113)

    for x, t in zip(XS, ("Human curators", "LLMs, default mode", "LLMs, high-yield mode")):
        fig.text(x + RW / 2, TITLE_TOP_IN / H, t, ha="center", va="top", fontsize=FS, weight="bold")

    hum_names = {"steakhouse-financial": "Steakhouse", "gauntlet": "Gauntlet", "sparkdao": "SparkDAO",
                 "sentora": "Sentora", "mev-capital": "MEV Capital", "telosc": "TelosC"}
    kw = dict(loc="upper center", fontsize=FS - 0.3, frameon=False, handlelength=1.0, handletextpad=0.3,
              labelspacing=0.12, borderaxespad=0, borderpad=0)
    la = [Line2D([], [], color=c, lw=0.9, label=hum_names[k]) for k, c in TEASER_CURATORS]
    fig.legend(handles=rows_first(la, 2), bbox_to_anchor=(XS[0] + RW / 2, LEG_TOP_IN / H), ncol=2,
               columnspacing=0.6, **kw)
    lb = [Line2D([], [], color=c, lw=0.9, label=n) for _l, n, c in RADAR]
    lb.append(Line2D([], [], label="Median human", **MEDIAN))
    fig.legend(handles=rows_first(lb, 2), bbox_to_anchor=((XS[1] + XS[2] + RW) / 2, LEG_TOP_IN / H), ncol=2,
               columnspacing=2.0, **kw)
    key = [("A", "displayed APY"), ("C", "cost efficiency"), ("Y", "realized yield"), ("S", "safety"),
           ("L", "liquidity"), ("R", "resilience")]
    rend = fig.canvas.get_renderer()
    parts = []
    for k, (letter, word) in enumerate(key):
        parts.append((letter + ":", "bold"))
        parts.append((" " + word + ("    " if k < len(key) - 1 else ""), "normal"))
    tmp = [fig.text(0, 0, t, fontsize=FS - 0.3, weight=w) for t, w in parts]
    widths = [t.get_window_extent(rend).width / fig.bbox.width for t in tmp]
    for t in tmp:
        t.remove()
    x = 0.5 - sum(widths) / 2
    for (t, w), dx in zip(parts, widths):
        fig.text(x, KEY_IN / H, t, ha="left", va="bottom", fontsize=FS - 0.3, weight=w, color="#333333")
        x += dx

    from matplotlib.lines import Line2D as _L
    for xs, y_lo in (((XS[0] + RW + XS[1]) / 2, (KEY_IN + 0.13) / H), ((XS[1] + RW + XS[2]) / 2, (LEG_TOP_IN + 0.03) / H)):
        fig.add_artist(_L([xs, xs], [y_lo, 1 - 0.02 / H], transform=fig.transFigure, color="#BDBDBD", lw=0.5,
                          ls=(0, (2, 2))))

    out = style.FIG_DIR / "fig0_teaser"
    for suffix in ("pdf", "png"):
        fig.savefig(f"{out}.{suffix}", dpi=600 if suffix == "png" else None, bbox_inches="tight", pad_inches=0.01)
    print(json.dumps({f"{k[0][:12]}|{k[1]}": [round(x) for x in v] for k, v in rv.items()}))
    print(f"{out}.pdf ({W * 72:.0f} x {H * 72:.0f} pt)")


if __name__ == "__main__":
    main()
