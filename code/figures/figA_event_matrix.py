# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.8"]
# ///
"""Appendix figure: the curator x event matrix of E5."""
from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict

import style

EVENT_VAULT = style.DATA_OUT / "metrics/e5/event_vault.csv"
EVENTS = style.DATA_OUT / "events/events.jsonl"
FAMILIES = style.DATA_OUT / "events/collateral_families.jsonl"
E4_CURATORS = style.DATA_OUT / "metrics/e4/curator_scorecard.csv"

UNATTRIBUTED = "No named curator"
DENSE_PT = 6.0
CELL_MIN_PT = 4.5
CELL_FONT = ["Arial Narrow", "Arial", "DejaVu Sans"]
PAGE_H_IN = 7.45
LEG2_IN = 0.42
LEG1_IN = 0.38
LATE_LOSS_MIN_USD = 1_000.0
PER_DOLLAR_MIN_USD = 10_000.0
HATCH = "//////"
INFLOWS = style.DATA_OUT / "metrics/e5/post_onset_inflows.csv"
INFLOW_GROUPS = {"external_onbehalf": "outside", "public_allocator": "outside",
                 "supply_queue": "deposits", "allocator": "curator"}
INFLOW_HATCH = {"outside": "/////", "deposits": "....", "curator": "-----"}
INFLOW_LABEL = {"outside": "others", "deposits": "the supply queue", "curator": "the curator"}


def load():
    """Cells keyed by (curator entity, event)."""
    cells = defaultdict(list)
    late = defaultdict(list)
    reentered = defaultdict(list)
    names, t0, etype = {}, {}, {}
    with open(EVENT_VAULT, newline="") as fh:
        for r in csv.DictReader(fh):
            if r["third_party"] != "True":
                continue
            cur = r["curator"] or "unattributed"
            if r["group"] != "never_entered":
                names[cur] = r["curator_name"] or UNATTRIBUTED
                t0[r["event_id"]] = r["t0_utc"]
                etype[r["event_id"]] = r["event_type"]
                cells[(cur, r["event_id"])].append(r)
                if (r["entered_after_t0"] == "True" and r["event_type"] == "depeg_loss"
                        and float(r["loss_usd_counted"] or 0.0) >= LATE_LOSS_MIN_USD):
                    reentered[(cur, r["event_id"])].append(r)
            elif (r["entered_after_t0"] == "True" and r["event_type"] == "depeg_loss"
                  and float(r["loss_usd_counted"] or 0.0) >= LATE_LOSS_MIN_USD):
                late[(cur, r["event_id"])].append(r)
    for (cur, ev), rs in late.items():
        assert ev in t0, f"{ev} has a post-t0 loss but no matrix row"
        names.setdefault(cur, rs[0]["curator_name"] or UNATTRIBUTED)
    rank = {}
    with open(E4_CURATORS, newline="") as fh:
        for r in csv.DictReader(fh):
            rank[r["curator_entity"]] = int(r["rank_by_aum"])
    curators = sorted(names, key=lambda c: (c == "unattributed", rank.get(c, 10**6), names[c]))
    events = sorted(t0, key=lambda e: (t0[e], e))
    out = {}
    for key in sorted(set(cells) | set(late)):
        cur, ev = key
        rs, lr = cells.get(key, []), late.get(key, [])
        exited = bool(rs) and all(x["group"] == "exited_before_t0" for x in rs)
        leads = [float(x["exit_lead_days"]) for x in rs if x["exit_lead_days"] != ""]
        lead = round(statistics.median(leads)) if exited and leads else None
        if etype[ev] == "depeg_loss":
            loss = sum(float(x["loss_usd_counted"] or 0.0) for x in rs + lr)
            val = sum(float(x["value_t0_usd_counted"] or 0.0) for x in rs + lr)
            big = [x for x in rs + lr if float(x["value_t0_usd"] or 0.0) >= PER_DOLLAR_MIN_USD]
            loss_pd = sum(float(x["loss_usd_counted"] or 0.0) for x in big)
            val_pd = sum(float(x["value_t0_usd_counted"] or 0.0) for x in big)
            lpd = loss_pd / val_pd if val_pd > 0 else (0.0 if loss == 0 else None)
        else:
            loss, val, lpd = None, None, None
        out[key] = {"exited": exited, "lead": lead, "lpd": lpd, "loss": loss, "value": val,
                    "n_vaults": len(rs), "late_vaults": len(lr) + len(reentered.get(key, [])),
                    "late_loss_usd": sum(float(x["loss_usd_counted"]) for x in lr),
                    "missing_lead": exited and not leads}
    flow = defaultdict(lambda: defaultdict(float))
    with open(INFLOWS, newline="") as fh:
        for r in csv.DictReader(fh):
            cur = r["curator"] or "unattributed"
            for mech, grp in INFLOW_GROUPS.items():
                flow[(cur, r["event_id"])][grp] += float(r[f"inflow_usd_{mech}"] or 0.0)
    for key, c in out.items():
        f = flow.get(key)
        c["inflow"] = max(f, key=f.get) if f and max(f.values()) > 0 else None
        c["inflow_usd"] = {g: round(v) for g, v in f.items()} if f else {}
    return curators, names, rank, events, t0, etype, out


def main():
    plt = style.setup()
    plt.rcParams["hatch.linewidth"] = 0.5
    from matplotlib.lines import Line2D
    from matplotlib.patches import Rectangle
    from matplotlib.transforms import offset_copy

    curators, names, rank, events, t0, etype, cells = load()
    def col_name(c):
        return style.curator_display(names[c])
    fam_members, fam_of = {}, {}
    with open(FAMILIES) as fh:
        for line in fh:
            f = json.loads(line)
            fam_members[f["family_id"]] = [m["symbol"] for m in f.get("members", [])]
    with open(EVENTS) as fh:
        for line in fh:
            e = json.loads(line)
            fam_of[e["event_id"]] = e
    ev_name = {ev: style.event_display(fam_of[ev],
                                       fam_members.get(fam_of[ev]["collateral_family"]["family_id"], []),
                                       short=True)
               for ev in events}

    n_r, n_c = len(events), len(curators)
    W, H = style.TEXT_IN, PAGE_H_IN
    fig = plt.figure(figsize=(W, H))
    renderer = fig.canvas.get_renderer()

    def text_pt(txt, size, **kw):
        t = fig.text(0, 0, txt, fontsize=size, **kw)
        w = t.get_window_extent(renderer).width * 72 / fig.dpi
        t.remove()
        return w
    date_pt = max(text_pt(t0[ev][:10], DENSE_PT) for ev in events)
    name_pt = max(text_pt(ev_name[ev], DENSE_PT, fontweight="bold") for ev in events)
    name_x = 3 + date_pt + 7
    left_in = (name_x + name_pt + 14) / 72
    top_in = (max(text_pt(col_name(c), style.FONT_PT) for c in curators) + 8) / 72
    right_in, bottom_in = 0.03, LEG2_IN + LEG1_IN + 0.08
    ax = fig.add_axes([left_in / W, bottom_in / H, 1 - (left_in + right_in) / W,
                       1 - (top_in + bottom_in) / H])
    ax.set_xlim(-0.5, n_c - 0.5)
    ax.set_ylim(n_r - 0.5, -0.5)
    for sp in ax.spines.values():
        sp.set_visible(False)

    cw_pt = (W - left_in - right_in) * 72 / n_c
    ch_pt = (H - top_in - bottom_in) * 72 / n_r
    gx, gy = 0.5 / cw_pt, 0.5 / ch_pt

    w_pt = 0.0
    for txt in sorted({str(c["lead"]) for c in cells.values() if c["lead"] is not None}):
        t = fig.text(0, 0, txt, fontsize=DENSE_PT, family=CELL_FONT)
        w_pt = max(w_pt, t.get_window_extent(renderer).width * 72 / fig.dpi)
        t.remove()
    cell_pt = DENSE_PT
    while w_pt * cell_pt / DENSE_PT + 0.6 > cw_pt and cell_pt > CELL_MIN_PT:
        cell_pt -= 0.5
    w_pt = w_pt * cell_pt / DENSE_PT
    digit_pt = w_pt / max(len(str(c["lead"])) for c in cells.values() if c["lead"] is not None)
    assert w_pt + 0.6 <= cw_pt, f"lead times ({w_pt:.1f} pt) do not fit cells ({cw_pt:.1f} pt)"

    for i in range(4, n_r - 1, 5):
        ax.axhline(i + 0.5, color=style.GRID, lw=0.4, zorder=0)
    for j in range(4, n_c - 1, 5):
        ax.axvline(j + 0.5, color=style.GRID, lw=0.4, zorder=0)
    for (cur, ev), c in cells.items():
        i, j = events.index(ev), curators.index(cur)
        fill = (style.NO_LOSS_MEASURED if c["lpd"] is None
                else style.LOSS_CLASS_COLOR[style.loss_class(c["lpd"])])
        ax.add_patch(Rectangle((j - 0.5 + gx, i - 0.5 + gy), 1 - 2 * gx, 1 - 2 * gy, facecolor=fill,
                               lw=0, zorder=2))
        if c["inflow"]:
            ax.add_patch(Rectangle((j - 0.5 + gx, i - 0.5 + gy), 1 - 2 * gx, 1 - 2 * gy,
                                   facecolor="none", hatch=INFLOW_HATCH[c["inflow"]],
                                   hatchcolor=style.ink_on(fill), lw=0, zorder=2.5))
        if c["lead"] is not None:
            if c["inflow"]:
                pw = (len(str(c["lead"])) * digit_pt + 1.0) / cw_pt
                ax.add_patch(Rectangle((j - pw / 2, i - 0.5 + gy), pw, 1 - 2 * gy, facecolor=fill,
                                       lw=0, zorder=2.7))
            ax.text(j, i, f"{c['lead']:d}", ha="center", va="center_baseline", fontsize=cell_pt,
                    family=CELL_FONT, color=style.ink_on(fill), zorder=3)

    ax.xaxis.tick_top()
    ax.set_xticks(range(n_c))
    ax.set_xticklabels([col_name(c) for c in curators], rotation=90, ha="center", va="bottom",
                       color=style.INK)
    ax.tick_params(axis="x", length=0, pad=3)
    ax.set_yticks([])
    ytr = ax.get_yaxis_transform()
    for i, ev in enumerate(events):
        dep = etype[ev] == "depeg_loss"
        ax.text(0, i, t0[ev][:10], transform=offset_copy(ytr, fig=fig, x=-(left_in * 72 - 3), y=0,
                                                         units="points"),
                ha="left", va="center_baseline", fontsize=DENSE_PT, color=style.INK2)
        ax.text(0, i, ev_name[ev], transform=offset_copy(ytr, fig=fig, x=-(left_in * 72 - name_x), y=0,
                                                         units="points"),
                ha="left", va="center_baseline", fontsize=DENSE_PT, color=style.INK,
                fontweight="bold" if dep else "normal")
        if dep:
            ax.plot([0], [i], transform=offset_copy(ytr, fig=fig, x=-6, y=0, units="points"),
                    marker="D", ms=3.6, color=style.EXPOSED, mec=style.SURFACE, mew=0.4,
                    clip_on=False, zorder=4)

    sw = [Rectangle((0, 0), 1, 1, color=c, lw=0) for c in style.LOSS_CLASS_COLOR]
    leg1 = fig.legend(sw + [Rectangle((0, 0), 1, 1, color=style.NO_LOSS_MEASURED, lw=0)],
                      style.LOSS_CLASS_LABEL + ["lock-up, no loss"],
                      title="Realized loss, % of vault value at onset",
                      loc="lower left", bbox_to_anchor=(left_in / W - 0.005, LEG2_IN / H), ncol=6,
                      handlelength=1.4, handleheight=1.0, handletextpad=0.4, columnspacing=0.7,
                      alignment="left", title_fontsize=style.FONT_PT)
    leg1.get_title().set_color(style.INK2)
    fig.add_artist(leg1)
    pat = {g: Rectangle((0, 0), 1, 1, facecolor=style.LOSS_CLASS_COLOR[1], hatch=INFLOW_HATCH[g],
                        hatchcolor=style.ink_on(style.LOSS_CLASS_COLOR[1]), lw=0) for g in INFLOW_HATCH}
    leg2 = fig.legend([pat["outside"], pat["deposits"], pat["curator"]],
                      [INFLOW_LABEL["outside"], INFLOW_LABEL["deposits"], INFLOW_LABEL["curator"]],
                      title="Exposure added after onset by",
                      loc="lower left", bbox_to_anchor=(left_in / W - 0.005, 0.0), ncol=3,
                      handlelength=2.6, handleheight=1.3, handletextpad=0.4, columnspacing=1.2,
                      alignment="left", title_fontsize=style.FONT_PT)
    leg2.get_title().set_color(style.INK2)
    fig.add_artist(leg2)
    fig.legend([Line2D([], [], ls="", marker="D", ms=3.6, color=style.EXPOSED, mec=style.SURFACE, mew=0.4)],
               ["Collateral failure (bold row)"], loc="lower right", bbox_to_anchor=(1.0 - right_in / W, 0.2 / H),
               handlelength=1.0, handletextpad=0.3)
    n_lead = sum(1 for c in cells.values() if c["lead"] is not None)
    style.save(fig, "figA_event_matrix", [EVENT_VAULT, EVENTS, FAMILIES, E4_CURATORS, INFLOWS],
               "Appendix figure: curator x event matrix (loss per dollar, exit lead time)",
               {"rows_events": events, "columns_curators": [names[c] for c in curators],
                "curator_e4_rank": {names[c]: rank.get(c) for c in curators},
                "cells_filled": len(cells), "cells_with_lead": n_lead,
                "cells_exited_without_lead": sum(1 for c in cells.values() if c["missing_lead"]),
                "late_entry_rule": f"third-party vaults that entered only after t0 in a depeg-loss "
                                   f"event and lost at least ${LATE_LOSS_MIN_USD:,.0f}",
                "hatched_cells": [{"curator": names[k[0]], "event_id": k[1],
                                   "late_vaults": c["late_vaults"],
                                   "late_loss_usd": round(c["late_loss_usd"]),
                                   "cell_loss_per_dollar": round(c["lpd"], 4),
                                   "only_late": c["n_vaults"] == 0, "inflow_group": c["inflow"],
                                   "inflow_usd": c["inflow_usd"]}
                                  for k, c in sorted(cells.items()) if c["late_vaults"] or c["inflow"]],
                "dense_text_pt": DENSE_PT, "cell_number_pt": cell_pt,
                "cell_pt": [round(cw_pt, 2), round(ch_pt, 2)]})
    plt.close(fig)
    print(f"figA: {n_r} events x {n_c} curators, {len(cells)} filled cells, {n_lead} with a lead "
          f"time; cell {cw_pt:.1f} x {ch_pt:.1f} pt; widest number {w_pt:.1f} pt")


if __name__ == "__main__":
    main()
