# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.8"]
# ///
"""Fig 3: curator profiles, one single-column radar with six curators overlaid."""
from __future__ import annotations

import csv
import math

import style

CURATORS = style.DATA_OUT / "metrics/e4/curator_scorecard.csv"
E5_SUMMARY = style.DATA_OUT / "metrics/e5/curator_summary.csv"
E4_FIG3 = style.DATA_OUT / "metrics/e4/fig3_data.csv"

N_SHOWN = 12
SHOWN = ["steakhouse-financial", "gauntlet", "sparkdao", "sentora", "mev-capital", "telosc"]
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
AXES = [("vault_apy", "Displayed APY", 1), ("yield_after_fee", "Realized yield", 1), ("tl_headline", "Safety", -1),
        ("L_p10", "Liquidity", 1), ("event_resilience", "Resilience", -1)]
ZERO = 1e-9


def num(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def pct_of(value, pool, sign):
    """Percentile of `value` among `pool` (non-missing), ties at average rank."""
    xs = [sign * v for v in pool if v is not None]
    if value is None or not xs:
        return None
    x = sign * value
    lt = sum(1 for v in xs if v < x)
    eq = sum(1 for v in xs if v == x)
    eq = max(eq, 1)
    return (lt + (eq + 1) / 2) / len(xs) * 100.0


def load():
    with open(CURATORS, newline="") as fh:
        cs = list(csv.DictReader(fh))
    cs.sort(key=lambda r: int(r["rank_by_aum"]))
    summ = {}
    with open(E5_SUMMARY, newline="") as fh:
        for r in csv.DictReader(fh):
            summ[r["curator"]] = r
    for r in cs:
        s = summ.get(r["curator_entity"])
        lpd = num(s["loss_per_dollar_value"]) if s else None
        tested = bool(s) and int(s["depeg_events_entered"] or 0) > 0
        r["event_resilience"] = (lpd if (tested and lpd is not None and lpd >= ZERO) else 0.0)
        r["_tested"] = tested
    return cs


def check_against_e4(cs, pct):
    """The four E4 axes must match E4's own fig3_data.csv percentiles."""
    ref = {}
    with open(E4_FIG3, newline="") as fh:
        for r in csv.DictReader(fh):
            ref[(r["curator_entity"], r["metric"])] = num(r["percentile"])
    worst = 0.0
    n = 0
    for r in cs[:N_SHOWN]:
        for m, _lab, _s in AXES[:4]:
            a, b = pct[(r["curator_entity"], m)], ref.get((r["curator_entity"], m))
            if a is not None and b is not None:
                worst = max(worst, abs(a - b))
                n += 1
    return n, worst


def main():
    plt = style.setup()
    from matplotlib.lines import Line2D

    cs = load()
    pools = {m: [num(r[m]) if m != "event_resilience" else r[m] for r in cs] for m, _l, _s in AXES}
    pct = {}
    for r in cs:
        for m, _lab, sg in AXES:
            v = num(r[m]) if m != "event_resilience" else r[m]
            pct[(r["curator_entity"], m)] = pct_of(v, pools[m], sg)
    med = []
    for m, _lab, sg in AXES:
        vals = sorted(v for v in pools[m] if v is not None)
        mv = vals[len(vals) // 2] if len(vals) % 2 else (vals[len(vals) // 2 - 1] + vals[len(vals) // 2]) / 2
        med.append(pct_of(mv, pools[m], sg))
    n_checked, worst = check_against_e4(cs, pct)
    assert worst < 1e-6, f"percentiles differ from E4's fig3_data.csv by {worst}"

    n_ax = len(AXES)
    ang = [(math.pi / 2 - 2 * math.pi * k / n_ax) % (2 * math.pi) for k in range(n_ax)]
    by_id = {r["curator_entity"]: r for r in cs}
    shown = [by_id[c] for c in SHOWN]

    W = style.COL_IN
    H = 0.62 * W
    fig = plt.figure(figsize=(W, H))
    D = 1.42
    cx, cy = 0.50 * W, H - 0.2 - D / 2
    ax = fig.add_axes([(cx - D / 2) / W, (cy - D / 2) / H, D / W, D / H], projection="polar")
    ax.set_ylim(0, 100)
    ax.set_yticks([25, 50, 75])
    ax.set_yticklabels([])
    ax.set_xticks(ang)
    ax.set_xticklabels([])
    ax.tick_params(length=0)
    ax.grid(color=style.GRID, lw=0.5)
    ax.spines["polar"].set_color(style.AXIS)
    ax.spines["polar"].set_linewidth(0.5)
    names = ["Displayed APY", "Realized\nyield", "Safety", "Liquidity", "Resilience"]
    for th, name in zip(ang, names):
        c, s_ = math.cos(th), math.sin(th)
        ax.annotate(name, (th, 100), xytext=(4 * c, 4 * s_), textcoords="offset points",
                    ha="center" if abs(c) < 0.2 else ("left" if c > 0 else "right"),
                    va="bottom" if s_ > 0.5 else ("top" if s_ < -0.5 else "center"),
                    fontsize=style.FONT_PT, color=style.INK, linespacing=1.0, annotation_clip=False)
    med_style = dict(color=style.MUTED, lw=0.8, ls=(0, (3, 1.5)))
    ax.plot(ang + ang[:1], med + med[:1], zorder=2, **med_style)
    handles = []
    panels = []
    for k, r in enumerate(shown):
        vals = [pct[(r["curator_entity"], m)] for m, _l, _s in AXES]
        vv = [v if v is not None else 0.0 for v in vals]
        ax.plot(ang + ang[:1], vv + vv[:1], color=SERIES[k], lw=1.1, marker="o", ms=2.6,
                mec=style.SURFACE, mew=0.4, zorder=3 + k)
        handles.append((Line2D([], [], color=SERIES[k], lw=1.1, marker="o", ms=2.6, mec=style.SURFACE,
                               mew=0.4), style.curator_display(r["curator_name"])))
        panels.append({"rank_by_aum": int(r["rank_by_aum"]), "curator": r["curator_name"],
                       "aum_avg_usd": float(r["aum_avg_usd"]),
                       "percentiles": {lab: (round(v, 2) if v is not None else None)
                                       for (_m, lab, _s), v in zip(AXES, vals)},
                       "values": {m: (num(r[m]) if m != "event_resilience" else r[m]) for m, _l, _s in AXES},
                       "depeg_exposure": r["_tested"]})
    handles.append((Line2D([], [], **med_style), "Median curator"))
    by_name = {t: h for h, t in handles}
    long_row = ["Steakhouse", "MEV Capital", "Median curator"]
    short_row = [t for _, t in handles if t not in long_row]
    for row, y in ((long_row, 0.15), (short_row, 0.0)):
        fig.legend([by_name[t] for t in row], row, loc="lower center", ncol=len(row),
                   bbox_to_anchor=(0.5, y / H), handlelength=1.5, handletextpad=0.3, columnspacing=0.8,
                   borderaxespad=0.1)
    style.save(fig, "fig3_radar", [CURATORS, E5_SUMMARY, E4_FIG3],
               "Fig 3: four curator profiles overlaid, percentile among curators on five axes",
               {"pool_curators": len(cs), "shown": SHOWN, "median_polygon": {lab: round(v, 2) for (_m, lab, _s), v in zip(AXES, med)},
                "panels": panels,
                "check_e4_fig3_data": {"values_compared": n_checked, "max_abs_diff": worst},
                "rules": {"percentile": "average rank of ties over curators with a value, x100",
                          "event_resilience": "E5 loss_per_dollar_value; no depeg-loss position = 0; "
                                              "|x| < 1e-9 = 0"}})
    plt.close(fig)
    print(f"fig3: {len(panels)} curators of {len(cs)}; checked {n_checked} E4 percentiles (max diff {worst:.1e})")
    for p in panels:
        print("  ", p["rank_by_aum"], p["curator"], p["percentiles"])

if __name__ == "__main__":
    main()
