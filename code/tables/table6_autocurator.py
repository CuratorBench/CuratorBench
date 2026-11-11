# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Table 6, the main comparison of AutoCurator with the human curators (E8)."""
from __future__ import annotations

import statistics as st
from collections import defaultdict

import tabfmt as T

GENERATOR = "code/tables/table6_autocurator.py"
RES = T.DATA / "autocurator/results"
SRC = [RES / "table6.csv", RES / "group_span.csv", RES / "humans_span.csv", RES / "outcomes_events.csv",
       RES / "humans_events.csv", RES / "eth_usdc_adjusted.csv", RES / "event_runs.csv"]
ETH = "morpho_1_USDC_a0b869"
SPAN = "common"
BLOCKS = [
    ("Proprietary", [("claude-opus-4-6-medium", "Claude Opus 4.6"), ("claude-haiku-4-5-medium", "Claude Haiku 4.5"),
                     ("gpt-5.4-2026-03-05-medium", "GPT-5.4"), ("gpt-5.4-mini-medium", "GPT-5.4 mini"),
                     ("gemini-2.5-flash-medium", "Gemini 2.5 Flash")]),
    ("Open weights", [("deepseek-v3.1-thinking", "DeepSeek V3.1"), ("qwen3-235b-a22b-2507", "Qwen3-235B"),
                      ("kimi-k2-0711", "Kimi K2"), ("glm-4.5-air-thinking", "GLM 4.5-Air"),
                      ("gpt-oss-120b-medium", "gpt-oss-120b"), ("Llama-3.3-70B-Instruct", "Llama 3.3 70B")]),
    ("Leakage controls", [("claude-opus-5-5-medium", "Claude Opus 5.5"), ("gpt-6.1-sol-medium", "GPT-6.1")]),
]
BASELINES = [("baseline-max_apy@common", "Max-APY"), ("baseline-equal@common", "Equal weight"),
             ("baseline-rule_based@common", "Rule-based"), ("baseline-passive_aave", "Passive Aave")]


def num(x, nd=1, signed=False):
    v = T.number(x)
    if v is None:
        return T.MISSING
    s = f"{v:+.{nd}f}" if signed else f"{v:.{nd}f}"
    return s.replace("-", "$-$").replace("+", "$+$")


def wins(wtl):
    return T.count(str(wtl).split("/")[0]) if wtl and "/" in str(wtl) else T.MISSING


_ABS = {}


def absolutes(label):
    """The vault's yield before fee and headline tail loss, each the median over its groups, in percent."""
    if not _ABS:
        by = {}
        for g in T.read_csv(SRC[1]):
            if g["span"] == SPAN:
                by.setdefault(g["label"], []).append(g)
        for lab, gs in by.items():
            _ABS[lab] = tuple(100 * st.median(T.number(g[k]) for g in gs if T.number(g[k]) is not None)
                              for k in ("yield_before_fee", "tl_headline"))
    return _ABS[label]


def with_gap(value, gap, nd):
    """A value and, in parentheses, its gap to the human median."""
    return f"{num(value, nd)} ({num(gap, nd, signed=True)})"


_ETH = {}


def eth_cell(label):
    """Yield in the Ethereum USDC group (two pinned spells frozen by hand, see eth_usdc_adjusted.py) and, in parentheses, its gap to the group's human median."""
    if not _ETH:
        _ETH.update({r["label"]: 100 * T.number(r["yield_adjusted"]) for r in T.read_csv(SRC[5])})
        for g in T.read_csv(SRC[1]):
            if g["span"] == SPAN and g["group"] == ETH:
                _ETH.setdefault(g["label"], 100 * T.number(g["yield_before_fee"]))
                _ETH["__human__"] = 100 * T.number(g["human_median_yield"])
    if label not in _ETH:
        return ""
    return with_gap(_ETH[label], _ETH[label] - _ETH["__human__"], 2)


def cells(r):
    if r is None:
        return [""] * 7
    y, tl = absolutes(r["label"])
    exposed = int(float(r["events_in_span"])) - int(float(r["events_zero_exposure_t0"]))
    return [with_gap(y, r["median_yield_gap_pp"], 2), wins(r["wtl_yield_vs_median"]),
            with_gap(tl, r["median_tail_loss_gap_pp"], 1), num(r["mean_vault_L_p10"], 2),
            T.count(exposed), num(float(r["mean_loss_per_dollar"]) * 100, 2), eth_cell(r["label"])]


HIT_USD = 1e4


def hit(exposure, loss):
    return (T.number(exposure) or 0) >= HIT_USD or (T.number(loss) or 0) >= HIT_USD


_HITS = {}


def hits(label):
    """Events of the window in which the vault had HIT_USD exposed at onset or lost HIT_USD, entering after onset included."""
    if not _HITS:
        for r in T.read_csv(SRC[3]):
            if r["span"] == SPAN:
                _HITS[r["label"]] = _HITS.get(r["label"], 0) + hit(r["exposure_t0_usd"], r["loss_usd"])
    return _HITS.get(label, 0)


def all_cells(r):
    """All groups: Y and TL (medians over groups) with gaps, >, L, events hit and Loss."""
    y, tl = absolutes(r["label"])
    exposed = hits(r["label"])
    return [with_gap(y, r["median_yield_gap_pp"], 2), wins(r["wtl_yield_vs_median"]),
            with_gap(tl, r["median_tail_loss_gap_pp"], 1), num(r["mean_vault_L_p10"], 2),
            T.count(exposed), num(float(r["mean_loss_per_dollar"]) * 100, 2)]


class Eth:
    """The Ethereum USDC group."""

    def __init__(self):
        self.gs = {g["label"]: g for g in T.read_csv(SRC[1]) if g["span"] == SPAN and g["group"] == ETH}
        self.y = {r["label"]: 100 * T.number(r["yield_adjusted"]) for r in T.read_csv(SRC[5])}
        self.hum = [h for h in T.read_csv(SRC[2]) if h["span"] == SPAN and h["group"] == ETH]
        self.hy = [100 * T.number(h["yield_before_fee"]) for h in self.hum if T.number(h["yield_before_fee"]) is not None]
        self.ev = defaultdict(dict)
        for r in T.read_csv(SRC[6]):
            if r["group"] == ETH:
                self.ev[r["label"]][r["event_id"]] = r
        self.events = sorted(self.ev["baseline-max_apy@common"])
        per = defaultdict(list)
        for h in T.read_csv(SRC[4]):
            if h["group"] == ETH and h["event_id"] in self.events:
                per[h["event_id"]].append(T.number(h["loss_per_dollar"]) or 0.0)
        self.h_loss = st.mean(st.mean(per[e]) if per[e] else 0.0 for e in self.events)

    def cells(self, label):
        g = self.gs.get(label)
        if g is None:
            return [""] * 6
        y = self.y.get(label, 100 * T.number(g["yield_before_fee"]))
        ev = self.ev.get(label, {})
        exposed = sum(hit(ev[e]["exposure_t0_usd"], ev[e]["loss_usd"]) for e in self.events if e in ev)
        loss = st.mean((T.number(ev[e]["loss_per_dollar"]) or 0.0) if e in ev else 0.0 for e in self.events)
        pct = 100 * sum(h < y for h in self.hy) / len(self.hy)
        return [num(y, 2), f"{pct:.0f}", num(100 * T.number(g["tl_headline"]), 1), num(g["L_p10"], 2),
                T.count(exposed), num(100 * loss, 2)]

    def humans(self):
        tl = [100 * T.number(h["tl_headline"]) for h in self.hum if T.number(h["tl_headline"]) is not None]
        lq = [T.number(h["L_p10"]) for h in self.hum if T.number(h["L_p10"]) is not None]
        return [num(st.median(self.hy), 2), "", num(st.median(tl), 1), num(st.median(lq), 2), "",
                num(100 * self.h_loss, 2)]


def pair(a, b):
    """Default / high-yield in one cell."""
    return f"{a} / {b}" if a or b else ""


def aggressive(label):
    return label.replace("-medium", "-medium-aggressive") if label.endswith("-medium") else label + "-aggressive"


def main():
    rows = {r["label"]: r for r in T.read_csv(SRC[0]) if r["span"] == SPAN}
    humans = [h for h in T.read_csv(SRC[2]) if h["span"] == SPAN]
    by_g = {}
    for h in humans:
        by_g.setdefault(h["group"], []).append(h)
    groups = sorted({r["group"] for r in T.read_csv(SRC[1]) if r["span"] == SPAN
                     and r["label"] == "baseline-max_apy@common"})
    hy = {g: st.median(T.number(h["yield_before_fee"]) for h in by_g[g] if T.number(h["yield_before_fee"]) is not None)
          for g in groups}
    ht = {g: st.median(T.number(h["tl_headline"]) for h in by_g[g] if T.number(h["tl_headline"]) is not None)
          for g in groups}
    ev = {r["event_id"] for r in T.read_csv(SRC[3]) if r["span"] == SPAN and r["label"] == "baseline-max_apy@common"}
    per = {}
    for h in T.read_csv(SRC[4]):
        if h["event_id"] in ev:
            per.setdefault(h["event_id"], []).append(T.number(h["loss_per_dollar"]) or 0.0)
    h_loss = st.mean(st.mean(per[e]) if e in per else 0.0 for e in ev)
    eth = Eth()
    stack = lambda *xs: r"\shortstack[r]{" + r" \\ ".join(xs) + "}"
    cols = [stack("Yield,", r"\% ($\Delta$)"), stack("Groups above", "human median"), stack("Tail loss,", r"\% ($\Delta$)"),
            stack("Liquidity", "$L$ p10"), stack("Events exposed", f"(of {len(ev)})"), stack("Event loss,", r"\%")]
    eth_cols = [stack("Yield,", r"\%"), stack("Human", "percentile"), stack("Tail loss,", r"\%"), cols[3],
                stack("Events exposed", f"(of {len(eth.events)})"), cols[5]]
    cash_y = st.median(-hy[g] * 100 for g in groups)
    cash_t = st.median(-ht[g] * 100 for g in groups)
    hl = {g: st.median(T.number(h["L_p10"]) for h in by_g[g] if T.number(h["L_p10"]) is not None) for g in groups}

    def render(name, keep):
        """The table with the Ethereum USDC columns `keep` (indices into the six of Eth.cells)."""
        pick = lambda xs: [xs[i] for i in keep]
        n_cols = 7 + len(keep)
        body = [r"\begin{tabular}{l rrrrrr " + "r" * len(keep) + "}", r"\toprule",
                T.row(["", r"\multicolumn{6}{c}{All groups}", rf"\multicolumn{{{len(keep)}}}{{c}}{{Ethereum USDC}}"]),
                rf"\cmidrule(lr){{2-7}} \cmidrule(lr){{8-{n_cols}}}",
                T.row(["Policy"] + cols + pick(eth_cols)), r"\midrule"]
        for block, members in BLOCKS:
            body.append(rf"\multicolumn{{{n_cols}}}{{l}}{{\textit{{{block}}}}} \\")
            for label, show in members:
                a, b = all_cells(rows[label]), all_cells(rows[aggressive(label)])
                ea, eb = pick(eth.cells(label)), pick(eth.cells(aggressive(label)))
                body.append(T.row([show] + [pair(x, y) for x, y in zip(a, b)] + [pair(x, y) for x, y in zip(ea, eb)]))
            body.append(r"\midrule")
        body.append(rf"\multicolumn{{{n_cols}}}{{l}}{{\textit{{Baselines}}}} \\")
        for label, show in BASELINES:
            body.append(T.row([show] + all_cells(rows[label]) + pick(eth.cells(label))))
        body.append(T.row(["All cash", with_gap(0.0, cash_y, 2), "0", with_gap(0.0, cash_t, 1), "1.00", "0", "0.00"]
                          + pick(["0.00", "0", "0.0", "1.00", "0", "0.00"])))
        body.append(r"\midrule")
        body.append(T.row([r"Humans", num(st.median(hy[g] for g in groups) * 100, 2), "",
                           num(st.median(ht[g] for g in groups) * 100, 1), num(st.median(hl[g] for g in groups), 2), "",
                           num(h_loss * 100, 2)] + pick(eth.humans())))
        body.append(r"\bottomrule")
        body.append(r"\end{tabular}")
        return T.write(name, GENERATOR, SRC, body)

    print(T.rel(render("table6_autocurator", range(6))))
    print(T.rel(render("table6_autocurator_compact", (0, 1, 5))))


if __name__ == "__main__":
    main()
