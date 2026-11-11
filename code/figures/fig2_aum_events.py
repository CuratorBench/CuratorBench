# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.8"]
# ///
"""Fig 2: curated AUM by protocol (top) and the stress-event timeline (bottom)."""
from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from datetime import date, timedelta

import style

AUM = style.DATA_OUT / "aum/by_protocol_daily.csv"
EVENTS = style.DATA_OUT / "events/events.jsonl"
TABLE3_MAIN = style.DATA_OUT / "events/table3_main.csv"
FAMILIES = style.DATA_OUT / "events/collateral_families.jsonl"
EVENT_VAULT = style.DATA_OUT / "metrics/e5/event_vault.csv"

HEAD = date(2026, 10, 2)
X0, X1 = date(2024, 1, 1), date(2026, 10, 31)
AREA_PT2_PER_USD = 144.0 / 250e6
N_LABELS = 7
MIN_AREA_PT2 = 2.6 ** 2
SIZE_KEYS_USD = [10e6, 100e6, 250e6]
VERBOSE = False
PREFER_BELOW = {"E2-019", "E2-027"}


def d(s: str) -> date:
    return date.fromisoformat(s[:10])


def load_aum():
    by = defaultdict(dict)
    with open(AUM, newline="") as fh:
        for r in csv.DictReader(fh):
            if r["protocol"] in style.PROTOCOLS and d(r["date"]) <= HEAD:
                by[r["protocol"]][d(r["date"])] = float(r["aum_usd"])
    start = min(min(v) for v in by.values())
    days = [start + timedelta(i) for i in range((HEAD - start).days + 1)]
    series = {p: [by[p].get(x, 0.0) / 1e9 for x in days] for p in style.PROTOCOLS}
    total = {}
    with open(AUM, newline="") as fh:
        for r in csv.DictReader(fh):
            if r["protocol"] == "all" and d(r["date"]) <= HEAD:
                total[d(r["date"])] = float(r["aum_usd"]) / 1e9
    return days, series, total


def e5_onsets():
    """E5's t0 per event (the depeg trigger for depeg-loss events)."""
    out = {}
    with open(EVENT_VAULT, newline="") as fh:
        for r in csv.DictReader(fh):
            out.setdefault(r["event_id"], r["t0_utc"])
    return out


def load_events():
    fam_members = {}
    with open(FAMILIES) as fh:
        for line in fh:
            f = json.loads(line)
            fam_members[f["family_id"]] = [m["symbol"] for m in f.get("members", [])]
    main = {}
    with open(TABLE3_MAIN, newline="") as fh:
        for r in csv.DictReader(fh):
            if int(r["main_text_rank"]) <= N_LABELS:
                main[r["event_id"]] = int(r["main_text_rank"])
    marks, labels = [], []
    n_events = 0
    t0_e5 = e5_onsets()
    placed = {}
    with open(EVENTS) as fh:
        for line in fh:
            e = json.loads(line)
            if e.get("excluded"):
                continue
            n_events += 1
            total = e.get("exposure_third_party_usd_rank") or 0.0
            t0 = defaultdict(float)
            after = defaultdict(float)
            for v in e["exposed_vaults"]:
                if v.get("third_party"):
                    t0[v["protocol"]] += v.get("exposure_t0_usd_counted") or 0.0
                    after[v["protocol"]] += v.get("max_exposure_usd_after_t0") or 0.0
            basis = t0 if sum(t0.values()) > 0 else after
            bsum = sum(basis.values())
            parts = {p: (total * basis.get(p, 0.0) / bsum if bsum > 0 else 0.0)
                     for p in e["protocols"]}
            x = d(e["onset_utc"])
            if e["type"] == "depeg_loss":
                if e["event_id"] in t0_e5:
                    x = d(t0_e5[e["event_id"]])
                placed[e["event_id"]] = {"type": e["type"], "x": x.isoformat(),
                                         "e2_onset": e["onset_utc"][:10],
                                         "source": "e5_t0" if e["event_id"] in t0_e5 else "e2_onset"}
            fam = e["collateral_family"]
            name = style.event_display(e, fam_members.get(fam["family_id"], []), short=True)
            for p, usd in parts.items():
                marks.append({"event_id": e["event_id"], "x": x, "protocol": p,
                              "type": e["type"], "usd": usd})
            if e["event_id"] in main:
                p_lab = max(parts, key=lambda p: parts[p])
                labels.append({"event_id": e["event_id"], "rank": main[e["event_id"]],
                               "x": x, "protocol": p_lab, "text": name,
                               "type": e["type"],
                               "usd": parts[p_lab]})
    assert len(labels) == len(main), "every labelled event must be in events.jsonl"
    return marks, labels, n_events, placed


def area(usd: float) -> float:
    """Mark area in pt^2 (the area of the circle a lock-up gets)."""
    return max(MIN_AREA_PT2, AREA_PT2_PER_USD * usd)


DIAMOND_S = math.pi / 4


def half_extent(usd: float, kind: str) -> float:
    """Half the height of a mark in points."""
    if kind == "depeg_loss":
        return math.sqrt(2 * DIAMOND_S * area(usd)) / 2
    return math.sqrt(area(usd)) / 2


def month_axis(ax, show_labels: bool):
    import matplotlib.dates as mdates
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())

    def fmt(x, _pos):
        t = mdates.num2date(x)
        return f"{t:%b}\n{t:%Y}" if t.month == 1 else f"{t:%b}"
    ax.xaxis.set_major_formatter(fmt if show_labels else (lambda *_: ""))
    ax.set_xlim(X0, X1)


def place_labels(fig, ax, labels, marks_px, row_y):
    """Greedy placement in date order."""
    import matplotlib.dates as mdates
    from matplotlib.transforms import Bbox
    renderer = fig.canvas.get_renderer()
    ax_bb = ax.get_window_extent(renderer)
    pt = fig.dpi / 72.0
    placed, flagged = [], []
    for lab in sorted(labels, key=lambda l: (l["x"], -l["usd"])):
        r = half_extent(lab["usd"], lab["type"])
        g = 1.5
        near = [(0, r + g, "center", "bottom"), (0, -(r + g), "center", "top"),
                (r + g, 0, "left", "center"), (-(r + g), 0, "right", "center"),
                (r + g, r + g, "left", "bottom"), (-(r + g), r + g, "right", "bottom"),
                (r + g, -(r + g), "left", "top"), (-(r + g), -(r + g), "right", "top")]
        far = [(0, r + 9, "center", "bottom"), (0, -(r + 9), "center", "top"),
               (r + 9, r + 7, "left", "bottom"), (-(r + 9), r + 7, "right", "bottom"),
               (r + 9, -(r + 7), "left", "top"), (-(r + 9), -(r + 7), "right", "top"),
               (r + 14, 0, "left", "center"), (-(r + 14), 0, "right", "center"),
               (r + 14, r + 12, "left", "bottom"), (-(r + 14), r + 12, "right", "bottom")]
        if lab["event_id"] in PREFER_BELOW:
            near = [near[1], near[0]] + near[2:]
        cands = [(c, False) for c in near] + [(c, True) for c in far]
        xy = (mdates.date2num(lab["x"]), row_y[lab["protocol"]])
        chosen = None
        for (dx, dy, ha, va), leader in cands:
            t = ax.annotate(lab["text"], xy=xy, xytext=(dx, dy), textcoords="offset points",
                            ha=ha, va=va, color=style.INK, fontsize=style.FONT_PT, zorder=6)
            bb = t.get_window_extent(renderer)
            pad = Bbox.from_extents(bb.x0 - pt, bb.y0, bb.x1 + pt, bb.y1)
            inside = (pad.x0 >= ax_bb.x0 and pad.x1 <= ax_bb.x1 and pad.y0 >= ax_bb.y0
                      and pad.y1 <= ax_bb.y1)
            hit = any(pad.overlaps(o) for o in placed) or any(
                pad.overlaps(m) for key, m in marks_px if key != (lab["event_id"], lab["protocol"]))
            t.remove()
            if inside and not hit:
                chosen = (dx, dy, ha, va, leader, pad)
                break
        if chosen is None:
            (dx, dy, ha, va), leader = cands[0]
            chosen = (dx, dy, ha, va, leader, None)
            flagged.append(lab["text"])
        dx, dy, ha, va, leader, pad = chosen
        if VERBOSE:
            print(f"  label {lab['text']:8s} {lab['x']} {lab['protocol']:9s} offset=({dx:.1f},{dy:.1f}) leader={leader}")
        arrow = (dict(arrowstyle="-", color=style.MUTED, lw=0.5, shrinkA=0.5, shrinkB=r + 0.5)
                 if leader else None)
        t = ax.annotate(lab["text"], xy=xy, xytext=(dx, dy), textcoords="offset points",
                        ha=ha, va=va, color=style.INK, fontsize=style.FONT_PT, zorder=6,
                        arrowprops=arrow)
        placed.append(pad if pad is not None else t.get_window_extent(renderer))
    if flagged:
        print("labels placed with overlap:", ", ".join(flagged))


def main():
    plt = style.setup()
    import matplotlib.dates as mdates
    from matplotlib.lines import Line2D
    from matplotlib.patches import Rectangle

    days, series, total = load_aum()
    marks, labels, n_events, depeg_dates = load_events()

    W, H = style.COL_IN, 0.86 * style.COL_IN
    fig = plt.figure(figsize=(W, H))
    gs = fig.add_gridspec(2, 1, height_ratios=[1.0, 0.95], hspace=0.12,
                          left=0.56 / W, right=1 - 0.04 / W, top=1 - 0.05 / H, bottom=0.5 / H)
    ax = fig.add_subplot(gs[0])
    bx = fig.add_subplot(gs[1], sharex=ax)

    ys = [series[p] for p in style.PROTOCOLS]
    cols = [style.PROTOCOL_COLOR[p] for p in style.PROTOCOLS]
    ax.stackplot(days, *ys, colors=cols, edgecolor=style.SURFACE, linewidth=0.5, zorder=2)
    ax.set_ylim(0, 9.4)
    ax.set_yticks([0, 2, 4, 6, 8])
    ax.yaxis.grid(True, zorder=0)
    ax.set_ylabel("Curated AUM\n(USD billions)")
    month_axis(ax, show_labels=False)
    ax.tick_params(axis="x", which="both", labelbottom=False)

    pk = max(total, key=total.get)
    for when, txt, dx, ha in ((pk, f"${total[pk]:.2f}B peak", -5, "right"),
                              (HEAD, f"${total[HEAD]:.2f}B", -3, "right")):
        ax.plot([when], [total[when]], "o", ms=3.2, color=style.INK, mec=style.SURFACE, mew=0.8,
                zorder=5)
        ax.annotate(txt, xy=(when, total[when]), xytext=(dx, 4), textcoords="offset points",
                    ha=ha, va="bottom", color=style.INK2, fontsize=style.FONT_PT, zorder=5)
    sw = [Rectangle((0, 0), 1, 1, facecolor=style.PROTOCOL_COLOR[p], lw=0)
          for p in reversed(style.PROTOCOLS)]
    ax.legend(sw, [style.PROTOCOL_LABEL[p] for p in reversed(style.PROTOCOLS)], loc="upper left",
              bbox_to_anchor=(0.02, 1.0), handlelength=1.0, handleheight=0.9, handletextpad=0.5,
              labelspacing=0.3, borderaxespad=0.2, frameon=True, facecolor=style.SURFACE,
              edgecolor="none", framealpha=1.0, borderpad=0.3).set_zorder(6)

    row_y = {p: len(style.PROTOCOLS) - 1 - i for i, p in enumerate(style.PROTOCOLS)}
    for p, y in row_y.items():
        bx.axhline(y, color=style.GRID, lw=0.5, zorder=0)
    bx.set_ylim(-0.6, len(style.PROTOCOLS) - 0.4)
    bx.set_yticks([row_y[p] for p in style.PROTOCOLS])
    bx.set_yticklabels([style.PROTOCOL_LABEL[p] for p in style.PROTOCOLS], color=style.INK)
    bx.tick_params(axis="y", length=0, pad=10)
    bx.spines["left"].set_visible(False)
    for p, y in row_y.items():
        bx.add_patch(Rectangle((-0.0175, y - 0.12), 0.009, 0.24, transform=bx.get_yaxis_transform(),
                               color=style.PROTOCOL_COLOR[p], clip_on=False, lw=0))
    bx.xaxis.grid(True, which="major", zorder=0)
    month_axis(bx, show_labels=True)

    order = sorted(marks, key=lambda m: (m["type"] != "depeg_loss", -m["usd"]))
    for m in order:
        s = area(m["usd"])
        if m["type"] == "depeg_loss":
            bx.scatter([m["x"]], [row_y[m["protocol"]]], s=s * DIAMOND_S, marker="D", color=style.EXPOSED,
                       edgecolors=style.SURFACE, linewidths=0.6, zorder=4)
        else:
            bx.scatter([m["x"]], [row_y[m["protocol"]]], s=s, marker="o", facecolors="none",
                       edgecolors=style.NOT_EXPOSED, linewidths=0.7, zorder=5)

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    pt = fig.dpi / 72.0
    marks_px = []
    from matplotlib.transforms import Bbox
    for m in marks:
        cx, cy = bx.transData.transform((mdates.date2num(m["x"]), row_y[m["protocol"]]))
        r = half_extent(m["usd"], m["type"]) * pt
        marks_px.append(((m["event_id"], m["protocol"]), Bbox.from_extents(cx - r, cy - r, cx + r, cy + r)))
    place_labels(fig, bx, labels, marks_px, row_y)

    def size_handle(usd):
        return Line2D([], [], ls="", marker="o", ms=math.sqrt(area(usd)), mfc="none",
                      mec=style.NOT_EXPOSED, mew=0.7)
    handles = [Line2D([], [], ls="", marker="D", ms=6 * math.sqrt(DIAMOND_S), mfc=style.EXPOSED,
                      mec=style.SURFACE, mew=0.6),
               Line2D([], [], ls="", marker="o", ms=6, mfc="none", mec=style.NOT_EXPOSED, mew=0.7),
               *[size_handle(u) for u in SIZE_KEYS_USD]]
    texts = [style.EVENT_TYPE_LABEL["depeg_loss"], style.EVENT_TYPE_LABEL["lockup"],
             *[f"${u / 1e6:.0f}M" for u in SIZE_KEYS_USD]]
    fig.legend(handles, texts, loc="lower center", ncol=len(handles), bbox_to_anchor=(0.5, 0.0),
               handlelength=1.3, handletextpad=0.1, columnspacing=0.3, borderaxespad=0.2)

    inputs = [AUM, EVENTS, TABLE3_MAIN, FAMILIES, EVENT_VAULT]
    style.save(fig, "fig2_aum_events", inputs,
               "Fig 2: curated AUM by protocol (top) and the stress-event timeline (bottom)",
               {"events_shown": n_events, "events_labelled": sorted(l["event_id"] for l in labels),
                "aum_head": HEAD.isoformat(),
                "x_date_rule": "depeg-loss events at E5's t0 (event_vault.csv t0_utc), "
                               "lock-ups at E2's onset (events.jsonl onset_utc)",
                "depeg_loss_dates": depeg_dates,
                "aum_peak": {"date": pk.isoformat(), "usd_billions": round(total[pk], 3)},
                "aum_head_total_usd_billions": round(total[HEAD], 3)})
    plt.close(fig)
    print(f"fig2: {n_events} events, {len(marks)} marks, {len(labels)} labels; "
          f"peak ${total[pk]:.2f}B on {pk}, ${total[HEAD]:.2f}B on {HEAD}")


if __name__ == "__main__":
    main()
