"""Appendix figure: the prompt and dialogue of one AutoCurator decision, abbreviated."""
from __future__ import annotations

import sys

sys.dont_write_bytecode = True

from itertools import combinations

import numpy as np
from fig_layout import ROOT, arrow, box, canvas, human, line, text
from fig_overview import (
    BLUE, DATA, DATA_PALE, INK2, LINE, MODEL, MODEL_PALE, NAVY, PALE, WARM,
    WARM_PALE, document, route,
)
from matplotlib.patches import Circle, Polygon, Rectangle
import matplotlib.pyplot as plt

WIDTH, HEIGHT = 506, 392
MONO = "DejaVu Sans Mono"
FS = 6.0
LEAD = 7.15
DESTINATION = ROOT / "figures" / "figA_prompt_dialog"
DIM = "#8A8F96"

CHECKS = []


def put(ax, x, y, s, right, color=INK2, mono=True, bold=False, size=FS, ha="left"):
    kw = {"fontfamily": MONO} if mono else {}
    t = text(ax, x, y, s, size, color, bold, ha, parse_math=False, **kw)
    CHECKS.append((t, right))
    return t


def card(ax, x, y, w, h, role, color, pale, edge=None, extra=None):
    """A message card: white body, pale header band, a role chip in the role's colour."""
    edge = edge or color
    box(ax, x, y, w, h, "white", edge, radius=1, lw=.7)
    ax.add_patch(Rectangle((x + .5, y + .5), w - 1, 11.5, facecolor=pale, edgecolor="none"))
    line(ax, x, y + 12, x + w, y + 12, edge, .5)
    cw = 5.2 + 3.35 * len(role)
    box(ax, x + 3, y + 1.8, cw, 8.6, color, color, radius=1, lw=.5)
    put(ax, x + 3 + cw / 2, y + 6.1, role, x + w, "white", mono=False, bold=True, ha="center")
    if extra:
        put(ax, x + 3 + cw + 4, y + 6.1, extra, x + w, color, mono=False)


def block(ax, x, y, rows, right, color=INK2):
    """Prompt lines in monospace; a row "..." is an elision drawn in grey at half pitch."""
    for row in rows:
        if row == "...":
            put(ax, x, y - 1.2, "...", right, DIM)
            y += LEAD * .62
            continue
        put(ax, x, y, row, right, color)
        y += LEAD
    return y


def gear(ax, x, y, r, color):
    n, ro, ri = 8, r, r * .74
    pts = []
    for k in range(n):
        a = 2 * np.pi * k / n
        for da, rr in ((-.17, ri), (-.11, ro), (.11, ro), (.17, ri)):
            pts.append((x + rr * np.cos(a + da), y + rr * np.sin(a + da)))
        pts.append((x + ri * np.cos(a + np.pi / n), y + ri * np.sin(a + np.pi / n)))
    ax.add_patch(Polygon(pts, closed=True, facecolor="white", edgecolor=color, lw=.8,
                         joinstyle="round"))
    ax.add_patch(Circle((x, y), r * .32, facecolor="white", edgecolor=color, lw=.8))


def robot(ax, x, y, s, color):
    box(ax, x - 6 * s, y - 4.3 * s, 12 * s, 8.6 * s, "white", color, radius=1.5 * s, lw=.8)
    line(ax, x, y - 4.3 * s, x, y - 6.3 * s, color, .8)
    ax.add_patch(Circle((x, y - 7 * s), .8 * s, facecolor=color, edgecolor="none"))
    for dx in (-2.3, 2.3):
        ax.add_patch(Circle((x + dx * s, y - .5 * s), .75 * s, facecolor=color, edgecolor="none"))
    line(ax, x - 2 * s, y + 2 * s, x + 2 * s, y + 2 * s, color, .7)
    for dx in (-7.2, 7.2):
        line(ax, x + dx * s, y - 1.6 * s, x + dx * s, y + 1.6 * s, color, 1.1)


def funnel(ax, x, y, color):
    ax.add_patch(Polygon([(x - 3.2, y - 3), (x + 3.2, y - 3), (x + .9, y), (x + .9, y + 3),
                          (x - .9, y + 3), (x - .9, y)], facecolor="white", edgecolor=color, lw=.6))


SYSTEM_ROWS = [
    ("Role", ["You are the curator of a lending vault. The vault lends",
              "one asset into lending markets of one protocol; ..."]),
    ("Metrics", ["Your vault is compared with the human curators' vaults",
                 "lending the same asset on the same protocol over the",
                 "same days, on each of these measures separately ...:",
                 "- yield after fees: ...   - tail loss: ...",
                 "- liquidity: ...          - in stress events: ..."]),
    ("Mode", ["Mandate: you run a high-yield vault. Its depositors",
              "chose it for yield: ...     [high-yield mode only]"]),
    ("Replay", ["The markets are simulated by replaying their recorded",
                "history. ... A withdrawal is filled at most up to the",
                "market's idle liquidity at that moment ..."]),
    ("Calls", ["You are called at 00:00 UTC at least every seven days,",
               "and earlier when a market the vault holds or its",
               "collateral has a triggering event: ..."]),
    ("Message", ["The message has, in order: the protocol's mechanics; ..."]),
    ("Tools", ["- list_documents, arg = a corpus key ...: the id, date,",
               "  type and title of its documents, newest first, ...",
               "- read_document, arg = a document id: its text, ...",
               "At most three rounds of tool calls and five",
               "read_document calls per decision; after that, decide."]),
    ("Decision", ["Your decision is a list of operations, each a market",
                  "label, a target weight from 0 to 1 ... and a",
                  "one-sentence reason. ... the rest stays in the vault",
                  "as cash, earning nothing. ..."]),
    ("Output", ["Answer with one JSON object and nothing else:",
                '{"tool_calls": [], "operations": [{"market": "M001",',
                ' "target_weight": 0.25, "reason": "..."}], "note": ""}']),
]

USER_ROWS = [
    ("Protocol mechanics (Moolah):",
     ["Liquidation. ... Oracle. ... Rate model. ... Withdrawal limits. ..."]),
    ("This vault lends USD1 (0x8d0d...) on Moolah, BNB Chain ...", []),
    ("Markets (label | collateral [corpus key], ..., oracle ...):",
     ["M013 | SolvBTC [solvbtc], issuer Solv Protocol, BTC, LLTV 0.7,",
      "  oracle other/proxy_feed_other_asset (blind to a depeg ...)", "..."]),
    ("Decision time: 2025-10-19T00:00:00Z. Market data through ...",
     ["Vault value $7.73M, of which cash $1.54M (20.0%)."]),
    ("Holdings (label | position | weight | current target | ...):",
     ["M013 | $2.32M | 30.0% | 30.0% | 97.6% | 12.67% | $608.2k", "..."]),
    ("Your last 4 decisions, oldest first (date | why you were ...):",
     ["2025-10-12 | event price_drop | M001 to 40.0%: Increasing ...", "..."]),
    ("Markets at the end of 2025-10-18 (label | supply | borrow | ...):",
     ["M013 | $25.03M | $24.42M | 97.6% | 4.41% | $608.2k | 67% | 12", "..."]),
    ("Collateral token supply on this chain ... | 7-day change | ...):",
     ["SolvBTC [solvbtc] | 5.2k | $558.08M | -17.8% | -17.9%", "..."]),
    ("Supply APY with your position ... (label | 10% | 25% | 50% | 100%):",
     ["M013 | 12.67% | 12.67% | 12.67% | 12.67%", "..."]),
    ("Events in the last 7 days (date of the data | subject | ...):",
     ["2025-10-18 | M013 | idle liquidity fell from $2.11M to $608.2k",
      "2025-10-12 | [usde] | new document: \"Binance pays $283 ...\"",
      "  (news, 2025-10-12T19:31:56Z, doc f35b3a934dc0b29a)", "..."]),
    ("Withdrawals still pending (retried every day at the current ...):", ["none"]),
]

ROUND1 = ['{"tool_calls": [{"tool": "list_documents", "arg":',
          '  "solvbtc"}, {"tool": "read_document", "arg":',
          '  "f35b3a934dc0b29a"}], "operations": [], "note":',
          '  "Checking SolvBTC news given 17.8% supply drop ..."}']

TOOL_RESULTS = ["Tool results:",
                "list_documents(solvbtc): 3 documents, the newest 3 (...):",
                "f6eec52e66c9b144 | 2025-06-23 | governance_forum | ...",
                "...",
                "read_document(f35b3a934dc0b29a): Binance pays $283",
                "  million in compensation following Friday's depegs,",
                "  covering user losses | 2025-10-12 | news",
                "... Binance said it paid out around $283 million to ..."]

ROUND2 = ['{"tool_calls": [], "operations": [{"market": "M013",',
          '  "target_weight": 0.20, "reason": "Reducing from 30%',
          '  due to dangerously high 97.6% utilization (near 99%',
          '  trigger), ..."}], "note": "M013\'s 12.67% APY reflects',
          '  extreme utilization risk; ... Keeping M001 and M004',
          '  unchanged. ..."}']

REPLAY = [("1", "withdraw M013 to 20%, each withdrawal up to the market's"),
          ("", "idle liquidity; the rest is retried daily"),
          ("2", "supply raised targets from cash (none in this decision)"),
          ("3", "accrue on the recorded market history until the next call")]


def draw():
    fig, ax = canvas(WIDTH, HEIGHT)
    fig.set_dpi(300)

    ox, oy, ow, oh = 140, 2, 226, 25
    box(ax, ox, oy, ow, oh, WARM_PALE, WARM, radius=1, lw=.7)
    human(ax, ox + 12, oy + 13, WARM, scale=.62)
    put(ax, ox + 23, oy + 7.5, "Operator", ox + ow, WARM, mono=False, bold=True)
    put(ax, ox + 23, oy + 17.5, "sets the vault and the mode", ox + ow, INK2, mono=False)
    put(ax, ox + 128, oy + 7.5, "vault: USD1, Moolah, BNB Chain", ox + ow, INK2, mono=False)
    put(ax, ox + 128, oy + 17.5, "mode: default | high-yield", ox + ow, INK2, mono=False)

    sx, sy, sw = 4, 36, 241
    ux, uw = 251, 251
    tag_w = 31
    y = sy + 20
    for tag, rows in SYSTEM_ROWS:
        put(ax, sx + 5, y, tag, sx + 5 + tag_w, MODEL, mono=False, bold=True)
        y = block(ax, sx + 5 + tag_w, y, rows, sx + sw) + 2.2
    s_bottom = y - 2.2 + 1
    y = sy + 20
    for head, rows in USER_ROWS:
        if head.startswith("Holdings"):
            holdings_y = y
        put(ax, ux + 5, y, head, ux + uw, DATA)
        y = block(ax, ux + 5, y + LEAD, rows, ux + uw) + 2.2
    u_bottom = y - 2.2 + 1
    bottom = max(s_bottom, u_bottom)
    card(ax, sx, sy, sw, bottom - sy, "System", MODEL, MODEL_PALE)
    card(ax, ux, sy, uw, bottom - sy, "User", DATA, DATA_PALE, extra="built at each call from the replay state")

    route(ax, [(ox - 0, oy + 12.5), (sx + 60, oy + 12.5), (sx + 60, sy)], WARM)
    put(ax, sx + 64, oy + 7.5, "mode", ox - 2, WARM, mono=False)
    route(ax, [(ox + ow, oy + 12.5), (ux + uw - 60, oy + 12.5), (ux + uw - 60, sy)], WARM)
    put(ax, ux + uw - 64, oy + 7.5, "vault", WIDTH, WARM, mono=False, ha="right")

    ly = bottom + 9
    lx, lw_, lh = 234, 38, 40
    box(ax, lx, ly, lw_, lh, MODEL_PALE, MODEL, radius=1, lw=.8)
    robot(ax, lx + lw_ / 2, ly + 14, 1.15, MODEL)
    put(ax, lx + lw_ / 2, ly + 27, "LLM", lx + lw_, MODEL, mono=False, bold=True, ha="center")
    put(ax, lx + lw_ / 2, ly + 35, "Opus 4.6", lx + lw_, INK2, mono=False, ha="center")
    arrow(ax, (sx + sw - 20, bottom), (lx + 8, ly), MODEL)
    arrow(ax, (ux + 20, bottom), (lx + lw_ - 8, ly), DATA)

    ax_, aw = 4, 226
    a1y = ly
    a1h = 14 + LEAD * len(ROUND1) + 2
    card(ax, ax_, a1y, aw, a1h, "Assistant", BLUE, PALE, extra="round 1: tool calls")
    block(ax, ax_ + 5, a1y + 19, ROUND1, ax_ + aw)
    arrow(ax, (lx, ly + 12), (ax_ + aw, ly + 12), MODEL)

    ty = a1y + a1h + 7
    th = 14 + LEAD * 7 + LEAD * .62 + 3
    card(ax, ax_, ty, aw, th, "User", DATA, DATA_PALE, extra="tool results")
    document(ax, ax_ + aw - 131, ty + 6.1, .42)
    put(ax, ax_ + aw - 125, ty + 6.1, "corpus", ax_ + aw, DATA, mono=False)
    funnel(ax, ax_ + aw - 99, ty + 6.1, DATA)
    put(ax, ax_ + aw - 94, ty + 6.1, "published before the decision time", ax_ + aw, DATA, mono=False)
    block(ax, ax_ + 5, ty + 19, TOOL_RESULTS, ax_ + aw)
    route(ax, [(ax_ + aw, ty + th / 2), (lx + 10, ty + th / 2), (lx + 10, ly + lh)], DATA)
    put(ax, lx + 12, ty + th / 2 - 5, "round 2", WIDTH, DATA, mono=False)

    bx, bw = 276, 226
    b2y = ly
    b2h = 14 + LEAD * len(ROUND2) + 2
    card(ax, bx, b2y, bw, b2h, "Assistant", BLUE, PALE, extra="round 2: answer")
    block(ax, bx + 5, b2y + 19, ROUND2, bx + bw)
    arrow(ax, (lx + lw_, ly + 12), (bx, ly + 12), MODEL)

    ry = b2y + b2h + 7
    rh = 14 + LEAD * len(REPLAY) + 4
    box(ax, bx, ry, bw, rh, PALE, NAVY, radius=1, lw=.7)
    ax.add_patch(Rectangle((bx + .5, ry + .5), bw - 1, 11.5, facecolor="#DCE7F2", edgecolor="none"))
    line(ax, bx, ry + 12, bx + bw, ry + 12, NAVY, .5)
    gear(ax, bx + 9, ry + 6.2, 4.6, NAVY)
    put(ax, bx + 17, ry + 6.1, "Replay engine", bx + bw, NAVY, mono=False, bold=True)
    put(ax, bx + 66, ry + 6.1, "executes the operations on recorded markets", bx + bw, INK2, mono=False)
    for i, (k, s) in enumerate(REPLAY):
        yy = ry + 19.5 + LEAD * i
        if k:
            put(ax, bx + 6, yy, k, bx + 14, NAVY, mono=False, bold=True)
        put(ax, bx + 13, yy, s, bx + bw, INK2, mono=False)
    arrow(ax, (bx + bw / 2, b2y + b2h), (bx + bw / 2, ry), BLUE)

    hy = ry + rh + 6
    route(ax, [(bx + bw - 40, ry + rh), (bx + bw - 40, hy), (WIDTH - 1.5, hy),
               (WIDTH - 1.5, holdings_y), (ux + uw, holdings_y)], NAVY)
    put(ax, bx + bw - 44, hy, "holdings to the next call", bx + bw, NAVY, mono=False, ha="right")

    height = max(ty + th, hy + 3) + 2
    ax.set_ylim(height, 0)
    fig.set_size_inches(WIDTH / 72, height / 72)

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    inv = ax.transData.inverted()
    ext, bad = [], []
    for t, right in CHECKS:
        b = t.get_window_extent(renderer).transformed(inv)
        if t.get_fontsize() < 6:
            bad.append(("size", t.get_text()))
        if b.x0 < 0 or b.x1 > min(right, WIDTH) + .3:
            bad.append(("width", t.get_text(), round(b.x1 - min(right, WIDTH), 1)))
        if min(b.y0, b.y1) < 0 or max(b.y0, b.y1) > height:
            bad.append(("height", t.get_text()))
        ext.append((t.get_text(), t.get_window_extent(renderer).shrunk(.98, .9)))
    for (s1, a), (s2, b) in combinations(ext, 2):
        if a.overlaps(b):
            bad.append(("overlap", s1, s2))

    meta = {"Title": "AutoCurator prompt and dialogue", "Author": "", "Creator": "",
            "Producer": "", "CreationDate": None, "ModDate": None}
    fig.savefig(f"{DESTINATION}.pdf", metadata=meta)
    fig.savefig(f"{DESTINATION}.png", dpi=300)
    plt.close(fig)
    assert not bad, "\n".join(map(str, bad))
    return height


if __name__ == "__main__":
    h = draw()
    print(f"prompt dialog: {DESTINATION}.{{pdf,png}} ({WIDTH} x {h:.0f} pt)")
