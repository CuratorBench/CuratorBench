"""One-column AutoCurator message diagram in the style of WWW Figure 1."""
from __future__ import annotations

import sys

sys.dont_write_bytecode = True

from fig_layout import ROOT, arrow, box, canvas, line, save, text
from fig_overview import (
    BLUE, DATA, DATA_PALE, INK2, LINE, MODEL, MODEL_PALE, NAVY, PALE, RISK,
    compact_robot, document,
)
from matplotlib.patches import Arc, Circle, PathPatch, Polygon, Rectangle, Wedge
from matplotlib.path import Path
from itertools import combinations


WIDTH, HEIGHT = 241, 172
SANS = 6.1
BODY = 6.1
MONO = "DejaVu Sans Mono"
DESTINATION = ROOT / "figures" / "fig5_autocurator"


def message_card(ax, x, y, w, h, role, color, pale, *, edge=LINE):
    """Reuse Figure 1's band, outline, and radius, adding a small role chip."""
    box(ax, x, y, w, h, "white", edge, radius=1, lw=.65)
    ax.add_patch(Rectangle((x + .5, y + .5), w - 1, 11.5,
                           facecolor=pale, edgecolor="none"))
    line(ax, x, y + 12, x + w, y + 12, edge, .5)
    chip_width = {"System": 32, "User": 24, "Assistant": 40, "Tool": 23}[role]
    box(ax, x + 3, y + 1.6, chip_width, 8.8, color, color, radius=1, lw=.5)
    text(ax, x + 3 + chip_width / 2, y + 6, role, 6.4, "white", True, "center")


def excerpt(ax, x, y, rows, *, leading=7.8):
    """Wrapping changes only the display; all excerpt text is monospace."""
    for i, row in enumerate(rows):
        text(ax, x, y + leading * i, row, BODY, INK2, fontfamily=MONO)


def calendar_icon(ax, x, y):
    box(ax, x - 4, y - 3.5, 8, 7.5, "white", NAVY, radius=.6, lw=.65)
    line(ax, x - 4, y - 1, x + 4, y - 1, NAVY, .55)
    for dx in (-2, 2):
        line(ax, x + dx, y - 4.6, x + dx, y - 2.3, NAVY, .75)
    for dx, dy in ((-2, .7), (0, .7), (2, .7), (-2, 2.4), (0, 2.4)):
        ax.add_patch(Rectangle((x + dx - .4, y + dy - .4), .8, .8,
                               facecolor=NAVY, edgecolor="none"))


def bell_icon(ax, x, y):
    vertices = [(-3.6, 2.5), (-2.6, 1), (-2.6, -1), (-2.6, -3.6),
                (0, -3.6), (2.6, -3.6), (2.6, -1), (2.6, 1),
                (3.6, 2.5), (-3.6, 2.5)]
    codes = [Path.MOVETO] + [Path.CURVE3] * 8 + [Path.CLOSEPOLY]
    ax.add_patch(PathPatch(Path([(x + a, y + b) for a, b in vertices], codes),
                           facecolor="white", edgecolor=NAVY, lw=.65))
    ax.add_patch(Circle((x, y + 3.6), .65, facecolor=NAVY, edgecolor="none"))
    line(ax, x, y - 3.6, x, y - 4.5, NAVY, .65)
    for sign in (-1, 1):
        line(ax, x + sign * 4.5, y - 3.5, x + sign * 5.2, y - 1.5, NAVY, .55)


def cutoff_gate(ax, x, y):
    box(ax, x, y, 52, 14, DATA_PALE, LINE, radius=1, lw=.65)
    cx, cy = x + 7, y + 7
    ax.add_patch(Polygon([(cx - 3.5, cy - 3), (cx + 3.5, cy - 3),
                          (cx + 1, cy), (cx + 1, cy + 3),
                          (cx - 1, cy + 3), (cx - 1, cy)],
                         facecolor="white", edgecolor=DATA, lw=.65))
    text(ax, x + 13, cy, "published < t", BODY, DATA)


MARKETS = {"M001": "#386F9F", "M004": "#D08A2E", "M013": "#C2577A"}
CASH = "#B9BFC6"
COLUMNS = {"Collateral": ("#FFF4D6", "#8C6D1F"), "Oracle": ("#E3F1F5", "#23707F"),
           "APY": ("#E8F5EC", "#2C8068"), "Label": ("#EEF0F3", "#4A5560")}


def chip(ax, x, y, w, label, fill, ink, bold=False):
    box(ax, x, y - 3.4, w, 6.8, fill, fill, radius=1.2, lw=.3)
    text(ax, x + w / 2, y, label, SANS, ink, bold, "center")


def clock_icon(ax, x, y, color):
    ax.add_patch(Circle((x, y), 3, facecolor="white", edgecolor=color, lw=.6))
    line(ax, x, y, x, y - 2, color, .6)
    line(ax, x, y, x + 1.6, y, color, .6)


def warning_icon(ax, x, y, color):
    ax.add_patch(Polygon([(x, y - 3.3), (x + 3.4, y + 2.8), (x - 3.4, y + 2.8)], facecolor="white",
                         edgecolor=color, lw=.6, joinstyle="round"))
    line(ax, x, y - 1.2, x, y + .9, color, .7)
    ax.add_patch(Circle((x, y + 1.9), .35, facecolor=color, edgecolor="none"))


def pie(ax, x, y, r, parts):
    start = 90
    for w, col in parts:
        ax.add_patch(Wedge((x, y), r, start - 360 * w, start, facecolor=col, edgecolor="white", lw=.3))
        start -= 360 * w


def gauge_icon(ax, x, y, color):
    ax.add_patch(Arc((x, y + 1.5), 7, 7, theta1=180, theta2=360, edgecolor=color, lw=.8))
    line(ax, x, y + 1.5, x + 2.6, y - .6, color, .8)


def check_icon(ax, x, y, color):
    ax.plot([x - 2, x - .6, x + 2.2], [y, y + 1.6, y - 1.8], color=color, lw=.8, solid_capstyle="round")


def shield_icon(ax, x, y, color):
    ax.add_patch(Polygon([(x - 2.6, y - 2.8), (x + 2.6, y - 2.8), (x + 2.6, y), (x, y + 3.2), (x - 2.6, y)],
                         facecolor="white", edgecolor=color, lw=.6, joinstyle="round"))


def market_table(ax, x, y):
    """The user message: the market table, then holdings, past decisions and events."""
    heads = [("Label", x, 18), ("Collateral", x + 20, 27), ("Oracle", x + 49, 27), ("APY", x + 78, 20)]
    for name, cx, w in heads:
        chip(ax, cx, y, w, name, *COLUMNS[name], bold=True)
    rows = [("M001", "BTCB", "proxy feed", "3.10%"), ("M004", "slisBNB", "exch. rate", "3.63%"),
            ("M013", "SolvBTC", "proxy feed", "12.67%")]
    for i, (m, c, o, r) in enumerate(rows):
        yy = y + 9 + 8 * i
        chip(ax, x, yy, 18, m, MARKETS[m], "white", bold=True)
        text(ax, x + 20 + 13.5, yy, c, SANS, COLUMNS["Collateral"][1], ha="center")
        text(ax, x + 49 + 13.5, yy, o, SANS, COLUMNS["Oracle"][1], ha="center")
        text(ax, x + 78 + 10, yy, r, SANS, COLUMNS["APY"][1], ha="center")
    yb = y + 37
    pie(ax, x + 4, yb, 4, [(.40, MARKETS["M001"]), (.10, MARKETS["M004"]), (.30, MARKETS["M013"]), (.20, CASH)])
    text(ax, x + 10, yb, "holdings", SANS, INK2)
    clock_icon(ax, x + 41, yb, NAVY)
    text(ax, x + 46, yb, "decisions", SANS, INK2)
    warning_icon(ax, x + 76, yb, RISK)
    text(ax, x + 81, yb, "events", SANS, INK2)


def allocation(ax, x, y):
    """The answer: the new weight per market as a bar (the previous one outlined), with its reason as an icon."""
    rows = [("M001", .40, .40, "hold"), ("M004", .10, .10, "hold"), ("M013", .20, .30, "util. 98%"),
            ("Cash", .30, .20, "buffer")]
    for i, (m, w, prev, why) in enumerate(rows):
        yy = y + 9 * i
        col = MARKETS.get(m, CASH)
        chip(ax, x, yy, 18, m, col, "white", bold=True)
        bx = x + 21
        ax.add_patch(Rectangle((bx, yy - 2.6), 40 * w, 5.2, facecolor=col, edgecolor="none"))
        if prev != w:
            ax.add_patch(Rectangle((bx, yy - 2.6), 40 * prev, 5.2, facecolor="none",
                                   edgecolor=col if prev > w else "#5F6B76", lw=.55, ls=(0, (1.5, 1))))
        text(ax, bx + 40 * max(w, prev) + 1.5, yy, f"{round(100 * w)}%", SANS, INK2)
        ix = x + 61
        if why == "hold":
            check_icon(ax, ix, yy, "#7A8590")
        elif why == "buffer":
            shield_icon(ax, ix, yy, "#7A8590")
        else:
            gauge_icon(ax, ix, yy, RISK)
        text(ax, ix + 5, yy, why, SANS, RISK if why.startswith("util") else INK2)


def draw():
    fig, ax = canvas(WIDTH, HEIGHT)
    fig.set_dpi(300)

    message_card(ax, 5, 3, 234, 36, "System", MODEL, MODEL_PALE, edge=MODEL)
    excerpt(ax, 9, 21, ["You are the curator of a lending vault. ..."])
    text(ax, 9, 31, "Judged on four metrics. Mode:", BODY, INK2, fontfamily=MONO)
    chip(ax, 119, 31, 28, "default", "#E3ECF7", NAVY, bold=True)
    text(ax, 150.5, 31, "/", BODY, INK2, ha="center")
    chip(ax, 154, 31, 36, "high-yield", "#FBE6D2", "#A55A12", bold=True)

    box(ax, 58, 46, 53, 13, PALE, LINE, radius=1, lw=.65)
    calendar_icon(ax, 66, 52.5)
    text(ax, 74, 52.5, "weekly", 6.6, NAVY)
    box(ax, 137, 46, 55, 13, PALE, LINE, radius=1, lw=.65)
    bell_icon(ax, 145, 52.5)
    text(ax, 153, 52.5, "risk trigger", 6.6, NAVY)

    message_card(ax, 5, 63, 104, 62, "User", DATA, DATA_PALE)
    market_table(ax, 9, 80)

    box(ax, 114, 68, 20, 29, MODEL_PALE, MODEL, radius=1, lw=.75)
    compact_robot(ax, 124, 79)
    text(ax, 124, 91, "LLM", 7, MODEL, True, "center")
    arrow(ax, (124, 39), (124, 68), MODEL)
    arrow(ax, (105, 59), (118, 68), INK2)
    arrow(ax, (143, 59), (130, 68), INK2)
    arrow(ax, (109, 82), (114, 82), INK2)

    message_card(ax, 141, 63, 98, 52, "Assistant", BLUE, PALE)
    allocation(ax, 145, 80)
    arrow(ax, (134, 82), (141, 82), MODEL)

    box(ax, 161, 124, 72, 21, PALE, LINE, radius=1, lw=.65)
    text(ax, 197, 130, "Replay", 7, NAVY, True, "center")
    text(ax, 197, 139, "execute allocation", BODY, INK2, ha="center")
    arrow(ax, (197, 115), (197, 124), BLUE)

    message_card(ax, 5, 131, 133, 33, "Tool", DATA, DATA_PALE)
    text(ax, 9, 149, 'list_documents("SolvBTC")', BODY, INK2, fontfamily=MONO)
    text(ax, 9, 157.5, 'read_document(', BODY, INK2, fontfamily=MONO)
    text(ax, 59.5, 157.5, '"Binance repays depegs")', SANS, INK2)
    arrow(ax, (124, 97), (124, 131), DATA, both=True)

    cutoff_gate(ax, 143, 149)
    document(ax, 213, 153, .5)
    text(ax, 213, 163, "dated corpus", BODY, INK2, ha="center")
    arrow(ax, (209.5, 154), (195, 154), DATA)
    arrow(ax, (143, 156), (138, 156), DATA)

    line(ax, 233, 137, 238, 137, BLUE, .65)
    line(ax, 238, 137, 238, 168, BLUE, .65)
    line(ax, 238, 168, 2, 168, BLUE, .65)
    line(ax, 2, 168, 2, 117, BLUE, .65)
    arrow(ax, (2, 117), (5, 117), BLUE)
    text(ax, 162, 168, "holdings", BODY, BLUE, ha="center",
         bbox={"facecolor": "white", "edgecolor": "none", "pad": .5})

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    labels = [(label, label.get_window_extent(renderer)) for label in ax.texts]
    for label, extent in labels:
        assert label.get_fontsize() >= 6, label.get_text()
        bounds = extent.transformed(ax.transData.inverted())
        assert bounds.x0 >= 0 and bounds.x1 <= WIDTH, label.get_text()
        assert min(bounds.y0, bounds.y1) >= 0, label.get_text()
        assert max(bounds.y0, bounds.y1) <= HEIGHT, label.get_text()
    for (first, a), (second, b) in combinations(labels, 2):
        assert not a.overlaps(b), (first.get_text(), second.get_text())

    save(fig, str(DESTINATION), "AutoCurator: message roles, tools, and replay")


if __name__ == "__main__":
    draw()
    print(f"AutoCurator: {DESTINATION}.{{pdf,svg,png}} ({WIDTH} x {HEIGHT} pt)")
