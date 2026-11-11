# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "pycryptodome"]
# ///
"""E8 decision loop: one hypothetical vault per group, run day by day through the replay."""
import gzip
import hashlib
import json
import re
import math
import os
import statistics
import time
from collections import Counter, defaultdict

import context as C
import models as M
import observe as O
import replay as R
import tools as T

REBALANCE_MIN = 0.005
WEIGHT_TOL = 1e-6
SAME_TOL = 1e-9
DEC_TOKENS_BASE = 600
DEC_TOKENS_PER_MARKET = 40
DEC_TOKENS_MAX = 4000
HEARTBEAT_D = 7
TOOL_ROUNDS = 3
READ_MAX = 5
HISTORY_N = 4
FLOW_WINDOW_D = 7
APY_GRID = (0.1, 0.25, 0.5, 1.0)
SCHEDULES = ("triggered", "daily")
TRIGGER_DOC_TYPES = ("issuer_announcement", "curator_announcement", "audit", "news")
TRIGGERS = {"severe": ("util_pin", "peg_off", "price_drop", "bad_debt", "new_docs"),
            "all": O.EVENT_ORDER}
LAYERS = ("mechanics", "facts", "tail")

DECISION_SCHEMA = {
    "type": "object", "additionalProperties": False, "required": ["tool_calls", "operations", "note"],
    "properties": {
        "tool_calls": {"type": "array", "items": {
            "type": "object", "additionalProperties": False, "required": ["tool", "arg"],
            "properties": {"tool": {"type": "string", "enum": ["list_documents", "read_document"]},
                           "arg": {"type": "string"}}}},
        "operations": {"type": "array", "items": {
            "type": "object", "additionalProperties": False, "required": ["market", "target_weight", "reason"],
            "properties": {"market": {"type": "string"}, "target_weight": {"type": "number"},
                           "reason": {"type": "string"}}}},
        "note": {"type": "string"}}}

OBJECTIVE_TEXT = """Your vault is compared with the human curators' vaults lending the same asset on the same protocol over the same days, on each of these measures separately (there is no combined score):
- yield after fees: the vault's annualized return to its depositors;
- tail loss: the loss the vault's holdings would take if collateral prices fell under stress, as each market's oracle sees them;
- liquidity: the share of the vault its depositors could withdraw at once;
- in stress events (a collateral depeg, a price crash, bad debt, a market lenders cannot leave): the loss per dollar the vault held when the event began, and the days its depositors could not withdraw."""

MANDATE_TEXT = {
    "default": "",
    "aggressive": "\nMandate: you run a high-yield vault. Its depositors chose it for yield: the highest yield after fees comes first, and they accept higher tail loss and lower liquidity in exchange. The other measures are still reported.",
}

SCHEDULE_TEXT = {
    "severe": "You are called at 00:00 UTC at least every seven days, and earlier when a market the vault holds or its collateral has a triggering event: utilization reaches 99 percent, the collateral goes off its peg or its price falls 10 percent or more in a day, a liquidation leaves bad debt, or a new issuer announcement, curator announcement, audit or news item about the collateral appears. Other events appear in the event log at the next call.",
    "all": "You are called at 00:00 UTC at least every seven days, and earlier when any event in the event log concerns a market the vault holds or its collateral.",
    "daily": "You are called at 00:00 UTC every day.",
}

TOOLS_TEXT = """Tools. You can read the local document corpus about the collaterals of the table (governance forums, documentation, audits, news, issuer and curator announcements; only documents public before the decision time):
- list_documents, arg = a corpus key (the [key] of a collateral in the table): the id, date, type and title of its documents, newest first, at most 30;
- read_document, arg = a document id: its text, cut at about 3,000 tokens.
To use them, answer with tool_calls filled and operations empty, for example {"tool_calls": [{"tool": "list_documents", "arg": "usde"}, {"tool": "read_document", "arg": "a334a1dfe6889e90"}], "operations": [], "note": ""}; the results come back and you answer again. At most three rounds of tool calls and five read_document calls per decision; after that, decide."""

SYSTEM = """You are the curator of a lending vault. The vault lends one asset into lending markets of one protocol; each market accepts specific collateral, prices it with a specific oracle, and liquidates borrowers at its LLTV. Lenders in a market lose money when collateral loses value faster than liquidations can cover the debt.
{objective}
{market_rules}{flows} A withdrawal cut short is retried every day at the same target, without asking you, until it completes or your next decision changes the target. Moves smaller than 0.5 percent of the vault's value are skipped, except a full exit.
{schedule} Between calls the vault keeps its weights.
The message has, in order: the protocol's mechanics; the static facts of every market in the table (the markets of the loan asset with at least $100k of supply that have not been at 99 percent utilization or more for the last 30 days, and the markets the vault holds); then the decision time, the vault's value{flows_part}, its holdings, your last four decisions with their reasons, each market's supply, borrow, utilization, supply APY, idle liquidity, largest borrower's share of debt and number of borrowers at the last day end, the supply APY each market would pay with your position at several sizes, the events of the last seven days, and any withdrawal still pending. Everything in it was public before the decision time.
{tools}
Your decision is a list of operations, each a market label, a target weight from 0 to 1 (the share of the vault's value to hold there) and a one-sentence reason. A market you do not list keeps its current target; a target of 0 exits; targets sum to at most 1 and the rest stays in the vault as cash, earning nothing. A market marked "keep or reduce only" cannot be raised. No operations keeps everything as it is. The targets are executed as given: there are no caps or limits besides the rules above.
Answer with one JSON object and nothing else: {{"tool_calls": [], "operations": [{{"market": "M001", "target_weight": 0.25, "reason": "..."}}], "note": ""}}."""


MARKET_TEXT = {
    "displace": "The markets are simulated by replaying their recorded history. Your vault takes the place of other lenders up to a market's recorded supply: while your position is within it, the market's utilization, rate and idle liquidity are the recorded ones, and you share that idle liquidity with every other lender. Only the part of a position above the recorded supply is added supply; borrowing follows its recorded path, so that part lowers the utilization and the rate. A withdrawal is filled at most up to the market's idle liquidity at that moment (supply minus borrow).",
    "impact": "The markets are simulated by replaying their recorded history with your vault's capital added. Borrowing follows its recorded path and does not respond to your supply, so your supply lowers a market's utilization and its rate. A withdrawal is filled at most up to the market's idle liquidity at that moment (supply minus borrow); in a market at 99 percent utilization or more, borrowers absorb any supply you add.",
}
IMPACT_NOTE = {"displace": "your supply above the market's recorded supply lowers utilization and the rate",
               "impact": "your supply lowers utilization and the rate; borrowing as now"}

FLOWS_TEXT = " Depositors add and withdraw capital at 00:00 UTC each day, before any call: a deposit is supplied to the markets in proportion to your current targets (the rest stays cash); a withdrawal is paid from cash, then from the markets in proportion to your positions, within their idle liquidity, and what cannot be paid waits for the next day."


def system_text(schedule="triggered", triggers="severe", tools=True, flows=False, pricing="displace",
                mandate="default"):
    """The system prompt; flows."""
    return SYSTEM.format(market_rules=MARKET_TEXT[pricing], objective=OBJECTIVE_TEXT + MANDATE_TEXT[mandate],
                         schedule=SCHEDULE_TEXT["daily" if schedule == "daily" else triggers],
                         tools=TOOLS_TEXT if tools else "You have no tools: decide from the message.",
                         flows=FLOWS_TEXT if flows else "",
                         flows_part=" and its net deposits over the last seven days" if flows else "")


class GroupContext:
    """Everything one group's runs share."""

    def __init__(self, group, d0=None, d1=None, tokens=None, assets=None, panel=None, prices=None, stream=None):
        self.g = group
        self.d0 = group.first_day if d0 is None else d0
        self.d1 = group.last_day if d1 is None else d1
        self.tokens = tokens or C.Tokens()
        self.assets = assets or C.CorpusAssets()
        self.panel = panel or C.PanelDaily()
        self.mk = C.Markets(group.protocol, group.chain_id)
        self.dist = C.euler_debt_shares(group.chain_id) if group.protocol == "euler_v2" else None
        self.prices = prices or C.Prices(self.tokens, {(group.chain_id, group.loan_token)})
        self.stream = stream or O.load_stream(group)
        self.tables = {}
        for d in range(self.d0, self.d1 + 1):
            self.tables[d - 1] = self.stream.table(d)
        union = set()
        for v in self.tables.values():
            union.update(v)
        self.markets = sorted(m for m in union if self.panel.get(group.protocol, group.chain_id, m))
        self.dec = self.tokens.decimals(group.chain_id, group.loan_token) or group.loan_decimals or 18
        self._fam = {}

    def usd_per_unit(self, d):
        px = self.prices.usd(self.g.chain_id, self.g.loan_token, d)
        dec = self.tokens.decimals(self.g.chain_id, self.g.loan_token) or self.g.loan_decimals
        if px is None or dec is None:
            return None
        return px / 10 ** dec

    def families(self, m, ts):
        """market_families at the day of ts (cached per market and day)."""
        key = (m, C.day_of_ts(ts))
        if key not in self._fam:
            self._fam[key] = C.market_families(self.mk, self.assets, self.tokens, m, ts, self.dist)
        return self._fam[key]

    def keys(self, ms, ts):
        """Collateral keys of markets (families carrying at least 2 percent of an Euler market's debt), sorted."""
        return sorted({f for m in ms for f, *_ in self.families(m, ts)})

    def series(self, m):
        return self.panel.get(self.g.protocol, self.g.chain_id, m)


def aum_path(group, d_start, d_end, e4_values, multiplier=1.0):
    """{day: target value in USD} for the day ends d_start - 1 .."""
    out, last = {}, None
    for d in range(d_start - 1, d_end + 1):
        vals = []
        for h in group.humans:
            v = e4_values.get((h["protocol"], int(h["chain_id"]), h["vault"].lower()), {}).get(d)
            if v is not None and v >= C.MATERIAL_USD:
                vals.append(v)
        if vals:
            last = statistics.median(vals) * multiplier
        if last is not None:
            out[d] = last
    return out or None


CAPITAL_SUPPLY_SHARE = 0.10
CAPITAL_MIN_DAYS = 30
NESTED_SHARE = 0.5


def nested_humans(group, e4_values):
    """Human vaults of the group held mostly (a median day share of at least NESTED_SHARE of their value) by other human vaults of the group (E3's nesting table)."""
    import csv
    hs = {h["vault"].lower(): h for h in group.humans}
    held = defaultdict(lambda: defaultdict(float))
    with open(C.INPUTS / "aum/nesting_daily.csv", newline="") as fh:
        for r in csv.DictReader(fh):
            if int(r["chain_id"]) == group.chain_id and r["holder"].lower() in hs and r["held"].lower() in hs:
                held[r["held"].lower()][C.e2.day_of(r["date"])] += float(r["usd"] or 0.0)
    out = set()
    for v, by_day in held.items():
        h = hs[v]
        val = e4_values.get((h["protocol"], int(h["chain_id"]), v), {})
        sh = [by_day.get(d, 0.0) / x for d, x in val.items() if group.first_day <= d <= group.last_day and x > 0]
        if sh and statistics.median(sh) >= NESTED_SHARE:
            out.add(v)
    return out


def capital_usd(group, e4_values, records, multiplier=1.0):
    """The vault's fixed capital in USD for the group, one value for every policy and span."""
    nested = nested_humans(group, e4_values)
    means, short = [], []
    for h in group.humans:
        v = h["vault"].lower()
        if v in nested:
            continue
        s = e4_values.get((h["protocol"], int(h["chain_id"]), v), {})
        xs = [s[d] for d in range(group.first_day, group.last_day + 1) if d in s]
        if xs:
            (means if len(xs) >= CAPITAL_MIN_DAYS else short).append(sum(xs) / len(xs))
    means = means or short
    sup = [sum(x[0] or 0.0 for x in (r.get("snapshot") or {}).values())
           for r in records if group.first_day <= r["d"] <= group.last_day]
    sup = [x for x in sup if x > 0]
    parts = {"human_mean_median_usd": statistics.median(means) if means else None, "humans_counted": len(means),
             "humans_nested_out": sorted(nested),
             "supply_median_usd": statistics.median(sup) if sup else None}
    caps = [x for x in (parts["human_mean_median_usd"],
                        CAPITAL_SUPPLY_SHARE * parts["supply_median_usd"] if sup else None) if x]
    parts["binding"] = None if not caps else ("supply share" if len(caps) == 2 and caps[1] < caps[0] else "humans")
    return (min(caps) * multiplier if caps else None), parts


def _json(text):
    s = text.strip()
    return json.loads(s[s.find("{"):s.rfind("}") + 1])


TOOL_FORMAT = ('each tool call is {"tool": "list_documents", "arg": "<corpus key>"} or '
               '{"tool": "read_document", "arg": "<document id>"}')
ARG_KEYS = ("arg", "key", "corpus_key", "collateral", "id", "doc_id", "document_id")


def tool_calls_of(text):
    """([(tool, arg)], [malformed entries]) of an answer's tool_calls; both empty when it asks for none."""
    try:
        obj = _json(text)
    except (json.JSONDecodeError, ValueError):
        return [], []
    tc = obj.get("tool_calls") if isinstance(obj, dict) else None
    if not isinstance(tc, list):
        return [], []
    calls, bad = [], []
    for x in tc:
        tool = (x.get("tool") or x.get("name")) if isinstance(x, dict) else None
        arg = x.get("arg") if isinstance(x, dict) else None
        if arg is None and isinstance(x, dict):
            a = x.get("arguments", x.get("args"))
            if isinstance(a, str):
                try:
                    a = json.loads(a)
                except json.JSONDecodeError:
                    pass
            if isinstance(a, dict):
                hit = [a[k] for k in ARG_KEYS if k in a] or (list(a.values()) if len(a) == 1 else [])
                arg = hit[0] if hit else None
            elif isinstance(a, str):
                arg = a
        if tool in ("list_documents", "read_document") and isinstance(arg, (str, int)) and str(arg):
            calls.append((tool, str(arg)))
        else:
            bad.append(json.dumps(x)[:200])
    return calls, bad


def tool_request(text):
    """The [(tool, arg)] of an answer that asks for tools, or None."""
    return tool_calls_of(text)[0] or None


def parse_decision(text, labels, prev, current, below):
    """Validate an answer against DECISION_SCHEMA and the weight rules, and turn its operations into target weights."""
    problems = []
    try:
        obj = _json(text)
    except (json.JSONDecodeError, ValueError):
        return {}, {}, ["answer is not a JSON object"]
    if not isinstance(obj, dict) or not isinstance(obj.get("operations"), list):
        return {}, {}, ["answer has no operations list"]
    if set(obj) - {"operations", "note", "tool_calls"}:
        problems.append(f"unknown keys {sorted(set(obj) - {'operations', 'note', 'tool_calls'})}")
    w = {lab: x for lab, x in prev.items() if lab in labels}
    reasons = {}
    for it in obj["operations"]:
        if not isinstance(it, dict) or set(it) != {"market", "target_weight", "reason"}:
            problems.append("malformed operation (keys must be market, target_weight, reason)")
            continue
        lab = it["market"]
        if lab not in labels:
            problems.append(f"unknown market label {lab}")
            continue
        if lab in reasons:
            problems.append(f"market {lab} has two operations")
            continue
        x = it["target_weight"]
        if isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) or not 0 <= x <= 1:
            problems.append(f"market {lab}: target_weight must be a number from 0 to 1")
            continue
        if lab in below and x > max(current.get(lab, 0.0), prev.get(lab, 0.0)) + 1e-4:
            problems.append(f"market {lab} is out of the table: keep or reduce only")
            continue
        w[lab] = float(x)
        reasons[lab] = str(it["reason"])
    total = sum(w.values())
    if total > 1 + WEIGHT_TOL:
        kept = sorted(lab for lab in w if lab not in reasons and w[lab] > 0)
        problems.append(f"targets sum to {total:.6f}, more than 1 ("
                        + ", ".join(f"{lab} {100 * w[lab]:.1f}%" + (" kept: not listed" if lab in kept else "")
                                    for lab in sorted(w) if w[lab] > 0) + ")")
    return {lab: x for lab, x in w.items() if x > 0}, reasons, problems


STUB_TOOL_EVERY = 5
STUB_READS = 2
STUB_MARKETS = 5


def stub_decision(meta):
    """Dry-run answer. On a call whose day index is a multiple of STUB_TOOL_EVERY (with tools)."""
    rnd = meta.get("round", 1)
    if meta.get("tools") and meta.get("d", 1) % STUB_TOOL_EVERY == 0 and meta.get("keys"):
        if rnd == 1:
            return json.dumps({"tool_calls": [{"tool": "list_documents", "arg": meta["keys"][0]}],
                               "operations": [], "note": "dry-run stub"})
        if rnd == 2 and meta.get("listed"):
            return json.dumps({"tool_calls": [{"tool": "read_document", "arg": x} for x in meta["listed"][:STUB_READS]],
                               "operations": [], "note": "dry-run stub"})
    ms = meta["markets"]
    pick = sorted((lab for lab, info in ms.items() if not info["below"]),
                  key=lambda lab: (-(ms[lab].get("supply_usd") or 0), lab))[:STUB_MARKETS]
    target = {lab: (round(0.95 / len(pick), 8) if lab in pick else 0.0) for lab in ms}
    ops = [{"market": lab, "target_weight": x, "reason": "dry-run stub"} for lab, x in sorted(target.items())
           if abs(x - ms[lab]["prev"]) > SAME_TOL and (x > 0 or ms[lab]["prev"] > 0 or ms[lab]["current"] > 0)]
    return json.dumps({"tool_calls": [], "operations": ops, "note": "dry-run stub"})


def fmt_usd(x):
    return O.money(x)


def prefix_digests(system, parts):
    """sha256[:16] of the prompt prefix through each part (the cache breakpoints)."""
    h = hashlib.sha256(system.encode())
    out = []
    for p in parts:
        h.update(p.encode())
        out.append(h.copy().hexdigest()[:16])
    return out


CALL_REASONS = {"start": "first call", "heartbeat": "weekly call", "daily": "daily call"}


def history_text(S, history):
    """The vault's last decisions, oldest first."""
    lines = [f"Your last {HISTORY_N} decisions, oldest first (date | why you were called | operations: "
             f"market to target weight, your reason):"]
    for r in history[-HISTORY_N:]:
        why = ", ".join(CALL_REASONS.get(t, t.replace("event:", "event ")) for t in r.get("triggers") or [])
        if r.get("kept_previous_weights"):
            what = "invalid answer: weights kept"
        else:
            ops = sorted(((S.label_of.get(m, m[:10]), w, why_) for m, (w, why_) in (r.get("operations") or {}).items()))
            what = "; ".join(f"{lab} to {O.pct(w)}: {txt}" for lab, w, txt in ops) or "no operations: weights kept"
        if r.get("pending_after"):
            what += (" (idle liquidity cut short the withdrawal from "
                     + ", ".join(sorted(S.label_of.get(m, m[:10]) for m in r["pending_after"])) + ")")
        lines.append(f"{r['time_utc'][:10]} | {why} | {what}")
    if len(lines) == 1:
        lines.append("none: this is the vault's first decision")
    return "\n".join(lines) + "\n"


def flows_text(dc, upu):
    """Net depositor flow of the last FLOW_WINDOW_D days and any withdrawal the vault could not pay today."""
    net, unpaid, nav = dc.get("flows_net", 0.0), dc.get("unpaid", 0.0), dc["nav"]
    sign = "+" if net >= 0 else "-"
    t = (f"Net deposits over the last {FLOW_WINDOW_D} days: {sign}{fmt_usd(abs(net) * upu)} "
         f"({sign}{O.pct(abs(net) / nav if nav > 0 else 0.0)} of the vault's value).")
    if unpaid > 0:
        t += f" Withdrawals the vault could not pay today: {fmt_usd(unpaid * upu)}."
    return t


def impact_text(dc, labels):
    """Supply APY of each table market with the vault's position there at each share of APY_GRID of its value, under the run's pricing."""
    nav, obs, f = dc["nav"], dc["obs"], dc.get("apy_after")
    if f is None or nav <= 0:
        return ""
    lines = [f"Supply APY with your position in the market at a share of the vault's value "
             f"({IMPACT_NOTE[dc.get('pricing', 'displace')]}) (label | " + " | ".join(O.pct(x, 0) for x in APY_GRID)
             + "):"]
    for lab in sorted(labels):
        m = labels[lab]
        cur = obs[m]["position"]
        lines.append(f"{lab} | " + " | ".join(O.pct(f(m, x * nav - cur), 2) for x in APY_GRID))
    return "\n".join(lines) + "\n"


def tail_text(gc, dc, labels, keys, below, prev):
    """The uncached last part of a decision prompt (module docstring)."""
    d, ts, obs, nav = dc["d"], dc["ts"], dc["obs"], dc["nav"]
    S = gc.stream
    upu = gc.usd_per_unit(d - C.PRICE_LAG_D) or 0.0
    idle = dc.get("idle", 0.0)
    lines = [f"Decision time: {C.ts_iso(ts)}. Market data through the end of {C.day_iso(d - 1)}; prices through "
             f"{C.day_iso(d - C.PRICE_LAG_D)}.",
             f"Vault value {fmt_usd(nav * upu)}, of which cash {fmt_usd(idle * upu)} "
             f"({O.pct(idle / nav if nav > 0 else 1.0)}).",
             *([flows_text(dc, upu)] if dc.get("flows_on") else []), ""]
    held = [lab for lab in sorted(labels) if obs[labels[lab]]["position"] > 0]
    lines.append("Holdings (label | position | weight | current target | utilization with the vault | supply APY "
                 "with the vault | withdrawable now):")
    for lab in held:
        o = obs[labels[lab]]
        lines.append(f"{lab} | {fmt_usd(o['position'] * upu)} | {O.pct(o['position'] / nav if nav > 0 else 0.0)} | "
                     f"{O.pct(prev.get(lab, 0.0))} | {O.pct(o['util'])} | {O.pct(o['supply_apy'], 2)} | "
                     f"{fmt_usd(min(o['liquidity'], o['position']) * upu)}")
    if not held:
        lines.append("none: all cash")
    lines.append("")
    lines.append(history_text(S, dc.get("history") or []))
    lines.append(S.numbers_text(d, sorted(labels), below))
    lines.append(impact_text(dc, labels))
    lines.append(S.events_text(d, set(labels), set(keys)))
    pend = []
    for m, w in sorted((dc.get("pending") or {}).items(), key=lambda x: S.label_of.get(x[0], x[0])):
        o = obs.get(m)
        if o is None:
            continue
        left = o["position"] - w * nav
        if left > 0:
            pend.append(f"{S.label_of.get(m, m[:10])} | {fmt_usd(left * upu)} still to withdraw toward target "
                        f"{O.pct(w)} | withdrawable now {fmt_usd(min(o['liquidity'], o['position']) * upu)}")
    lines.append("Withdrawals still pending (retried every day at the current target):")
    lines += pend or ["none"]
    return "\n".join(lines) + "\n"


class Policy:
    """decide(dc) returns target weights {market."""

    name = "policy"
    uses_model = False

    def decide(self, dc):
        raise NotImplementedError


class ReplayPolicy(Policy):
    """The recorded decisions of another run, replayed on their own call times."""

    def __init__(self, decisions, name):
        self.name = name
        self.by_ts = {int(x["ts"]): (None if x.get("weights_source") == "previous" else dict(x.get("weights") or {}))
                      for x in decisions}

    def decide(self, dc):
        return self.by_ts.get(dc["ts"])


_OVER_ONE = re.compile(r"(M\d{3}) ([\d.]+)%( kept: not listed)?")


def intended_weights(prev, listed, table):
    """The weights an answer means."""
    w = {m: x for m, x in (prev or {}).items() if m in table and x > 0}
    w.update(listed)
    room = 1.0 - sum(listed.values())
    if sum(w.values()) > 1 + WEIGHT_TOL:
        if room < -WEIGHT_TOL:
            return None
        rest = {m: x for m, x in w.items() if m not in listed}
        k = max(room, 0.0) / sum(rest.values()) if rest else 0.0
        w.update({m: x * k for m, x in rest.items()})
    return {m: x for m, x in w.items() if x > 0}


class IntentReplayPolicy(Policy):
    """The recorded answers of another run, replayed on their call times with each answer read as meant."""

    def __init__(self, decisions, name):
        self.name = name
        self.by_ts = {int(x["ts"]): x for x in decisions}
        self.interpreted = 0

    def decide(self, dc):
        x = self.by_ts.get(dc["ts"])
        if x is None:
            return None
        if "operations" not in x and "problems" not in x:
            return None if x.get("weights_source") == "previous" else dict(x.get("weights") or {})
        table, prev = set(dc["markets"]), dc["prev"] or {}
        if x.get("weights_source") == "previous":
            over = [p for p in x.get("problems") or [] if "more than 1" in str(p)]
            if not over or len(x.get("problems") or []) != 1:
                return None
            S = dc["gc"].stream
            listed = {S.market_of[lab]: float(v) / 100 for lab, v, kept in _OVER_ONE.findall(over[0]) if not kept}
            w = intended_weights(prev, listed, table)
            if w is not None:
                self.interpreted += 1
            return w
        ops = {m: float(v[0]) for m, v in (x.get("operations") or {}).items()}
        if not ops:
            return None
        return intended_weights(prev, ops, table)


def load_decisions(label, g, repeat=1):
    with gzip.open(C.OUT / "decisions" / label / f"{g.key}__r{repeat}.jsonl.gz", "rt") as fh:
        return [json.loads(x) for x in fh if x.strip()]


class LLMPolicy(Policy):
    """AutoCurator: the observation stream and the corpus tools -> the model's operations."""

    uses_model = True

    def __init__(self, client, corpus, label, tools=True, name="autocurator", schedule="triggered",
                 triggers="severe", mandate="default"):
        self.client, self.corpus, self.label = client, corpus, label
        self.tools = tools
        self.name = name
        self.mandate = mandate
        self.systems = {(f, p): system_text(schedule, triggers, tools, flows=f, pricing=p, mandate=mandate)
                        for f in (False, True) for p in MARKET_TEXT}
        self.system = self.systems[(False, "displace")]

    def parts(self, dc, labels, below, prev):
        """The three user-message parts (mechanics, facts, tail) and the table's collateral keys."""
        gc, d = dc["gc"], dc["d"]
        S = gc.stream
        keys = sorted({k for lab in labels for k in S.collateral_keys(lab, d)})
        return [S.mechanics_text(), "\n" + S.facts_text(labels, d),
                "\n" + tail_text(gc, dc, labels, keys, below, prev)], keys

    def run_tools(self, calls, ts, keys, state):
        """Text of one round's tool results; state counts read_document calls and keeps the ids listed so far."""
        out = []
        for tool, arg in calls:
            if tool == "list_documents":
                txt = T.list_documents(self.corpus, arg, ts, keys)
                state["listed"] += [x.split(" | ")[0] for x in txt.split("\n")[1:]]
            elif state["reads"] >= READ_MAX:
                txt = f"read_document({arg}): refused, the limit of {READ_MAX} documents per decision is reached"
            else:
                state["reads"] += 1
                txt = T.read_document(self.corpus, arg, ts, keys)
            out.append(txt)
        return "Tool results:\n\n" + "\n\n".join(out) + "\n"

    def decide(self, dc):
        gc, ts, nav, obs = dc["gc"], dc["ts"], dc["nav"], dc["obs"]
        table = dc["markets"]
        if not table:
            dc["record"]["note"] = "empty table: no model call"
            return {}
        S = gc.stream
        labels = {S.label_of[m]: m for m in table}
        below = {S.label_of[m] for m in dc["below"]}
        prev = {S.label_of[m]: x for m, x in (dc.get("prev") or {}).items() if m in S.label_of}
        current = {lab: (obs[m]["position"] / nav if nav > 0 else 0.0) for lab, m in labels.items()}
        parts, keys = self.parts(dc, labels, below, prev)
        system = self.systems[(bool(dc.get("flows_on")), dc.get("pricing", "displace"))]
        snap = S.record(dc["d"])["snapshot"]
        meta = {"d": dc["d"], "repeat": dc.get("repeat", 1), "round": 1, "tools": self.tools, "keys": keys,
                "listed": [],
                "markets": {lab: {"market": m, "below": lab in below, "prev": prev.get(lab, 0.0),
                                  "current": current[lab],
                                  "supply_usd": dict(zip(O.SNAP_COLS, snap.get(lab) or [])).get("supply_usd")}
                            for lab, m in sorted(labels.items())}}
        stub = stub_decision if self.client.spec["provider"] == "stub" else None
        max_tokens = min(DEC_TOKENS_BASE + DEC_TOKENS_PER_MARKET * len(labels), DEC_TOKENS_MAX)
        cache_key = f"{self.label}:{gc.g.key}"
        msgs = [{"role": "user", "parts": parts}]

        def ask(msgs, rnd):
            breaks = [(0, 0), (0, 1), (0, 2)] + ([(len(msgs) - 1, len(msgs[-1]["parts"]) - 1)] if len(msgs) > 1 else [])
            return self.client.chat(system, msgs, max_tokens, DECISION_SCHEMA, purpose="decision", stub=stub,
                                    meta=dict(meta, round=rnd), breaks=breaks, cache_key=cache_key)
        comps = [ask(msgs, 1)]
        rounds, state = [], {"reads": 0, "listed": []}
        while self.tools and len(rounds) < TOOL_ROUNDS:
            calls, bad = tool_calls_of(comps[-1].text)
            if not calls:
                break
            body = self.run_tools(calls, ts, keys, state)
            if bad:
                body += "\nNot run (malformed; " + TOOL_FORMAT + "): " + "; ".join(bad) + "\n"
            last = len(rounds) + 1 == TOOL_ROUNDS
            body += ("\nThat was the last round of tool calls: now answer with your operations and empty tool_calls.\n"
                     if last else "")
            meta["listed"] = list(state["listed"])
            rounds.append({"calls": [list(c) for c in calls], "result_tokens": M.estimate_tokens(body)})
            msgs = msgs + [{"role": "assistant", "text": comps[-1].text}, {"role": "user", "parts": [body]}]
            comps.append(ask(msgs, len(comps) + 1))
        def check(text):
            w, reasons, problems = parse_decision(text, labels, prev, current, below)
            calls, bad = tool_calls_of(text)
            if not problems and (calls or bad):
                if not self.tools:
                    problems = ["this decision has no tools: tool_calls must be empty"]
                elif bad and not calls:
                    problems = [f"malformed tool_calls ({TOOL_FORMAT}): " + "; ".join(bad[:5])]
                else:
                    problems = ["no more tool calls: answer with operations and empty tool_calls"]
            return w, reasons, problems
        w, reasons, problems = check(comps[-1].text)
        if problems:
            msgs = msgs + [{"role": "assistant", "text": comps[-1].text},
                           {"role": "user", "parts": ["Your previous answer had problems:\n- " + "\n- ".join(problems[:20])
                                                      + "\nAnswer again with operations and empty tool_calls.\n"]}]
            comps.append(ask(msgs, len(comps) + 1))
            w, reasons, problems = check(comps[-1].text)
        rec = dc["record"]
        rec.update({"archive": [str(c.path.relative_to(C.ROOT)) for c in comps],
                    "request_sha256": [c.request_sha for c in comps],
                    "tokens_in": sum(c.tokens_in for c in comps), "tokens_out": sum(c.tokens_out for c in comps),
                    "cache_read": sum(c.cache_read for c in comps), "cache_write": sum(c.cache_write for c in comps),
                    "cost_usd": sum(c.cost for c in comps), "prompt_tokens": comps[0].tokens_in,
                    "reused": sum(getattr(c, "reused", False) for c in comps),
                    "layers": {"system": M.estimate_tokens(system),
                               **{k: M.estimate_tokens(p) for k, p in zip(LAYERS, parts)}},
                    "prefix": prefix_digests(system, parts), "tool_rounds": rounds,
                    "reads": state["reads"], "keys": keys, "used_tools": self.tools, "problems": problems})
        if problems:
            rec["kept_previous_weights"] = True
            return None
        rec["operations"] = {labels[lab]: [round(w.get(lab, 0.0), 8), r] for lab, r in reasons.items()}
        return {labels[lab]: x for lab, x in w.items()}


class Unpriced(RuntimeError):
    """The group's loan token has no USD price or no capital at the span start."""


def same_weights(a, b):
    keys = set(a) | set(b)
    return all(abs(a.get(k, 0.0) - b.get(k, 0.0)) <= SAME_TOL for k in keys)


def fires(e, triggers="severe"):
    """True when an event triggers a call under the trigger set (module docstring)."""
    if e["t"] not in TRIGGERS[triggers]:
        return False
    if triggers == "severe":
        if e["t"] == "util_pin":
            return e.get("dir") == "up"
        if e["t"] == "new_docs":
            types = e.get("types") or {x["type"]: 1 for x in e.get("docs", [])}
            return any(t in TRIGGER_DOC_TYPES for t in types)
    return True


def triggers(rec, held_labels, held_keys, last_call, d, schedule="triggered", trigger_set="severe"):
    """Why the policy is called at the 00:00 UTC check of day d ([] = no call)."""
    if schedule not in SCHEDULES:
        raise ValueError(schedule)
    out = set()
    if last_call is None:
        out.add("start")
    for e in (rec or {}).get("events", []):
        if e.get("m") in held_labels or (e.get("c") is not None and e.get("c") in held_keys):
            if fires(e, trigger_set):
                out.add("event:" + e["t"])
    if last_call is not None and d - last_call >= HEARTBEAT_D:
        out.add("heartbeat")
    if schedule == "daily":
        out.add("daily")
    return sorted(out)


def event_tags(gc, d0, d1, shortfall_times):
    """{ts: [tags]} of event moments inside (d0 00:00, end of d1]."""
    lo, hi = d0 * C.DAY, (d1 + 1) * C.DAY
    tags = defaultdict(list)
    for e, aff in C.group_events(gc.g):
        t0 = C.e5_t0(e["event_id"])
        if not lo < t0 <= hi:
            continue
        tags[t0].append(f"t0:{e['event_id']}")
        for a in aff:
            if a.get("first_trigger_ts") and lo < int(a["first_trigger_ts"]) <= hi:
                tags[int(a["first_trigger_ts"])].append(f"trigger:{e['event_id']}:{a['market']}")
            tv = shortfall_times.get((e["event_id"], a["market"]))
            if tv is not None:
                tags[min(max(tv, lo + 1), hi)].append(f"valuation:{e['event_id']}:{a['market']}")
    return tags


def plan_moves(w, table, cur, obs, nav, idle, flags=None):
    """Positions to move to ({market."""
    floor = REBALANCE_MIN * nav
    plan, short, cash, clipped = {}, False, float(idle), set()
    for m in table:
        c, t = cur.get(m, 0.0), w.get(m, 0.0) * nav
        liq = max(float(obs[m]["liquidity"]), 0.0)
        if t <= 0 < c:
            amt = min(c, liq)
            if amt >= c:
                plan[m] = 0.0
            elif amt > 0:
                plan[m] = c - amt
            if amt < c:
                short = True
                clipped.add(m)
            cash += amt
        elif c - t >= floor:
            amt = min(c - t, liq)
            if amt < c - t - 1e-9 * max(nav, 1.0):
                short = True
                clipped.add(m)
            if amt >= floor:
                plan[m] = c - amt
                cash += amt
    need = {m: w.get(m, 0.0) * nav - cur.get(m, 0.0) for m in table}
    need = {m: x for m, x in need.items() if x >= floor}
    tot = sum(need.values())
    scale = min(1.0, cash / tot) if tot > 0 else 0.0
    for m, x in need.items():
        if x * scale >= floor:
            plan[m] = cur.get(m, 0.0) + x * scale
    if tot > 0 and scale < 1.0 - 1e-9:
        short = True
    if flags is not None:
        flags["clipped"] = bool(clipped)
        flags["clipped_markets"] = clipped
    return plan, short


def run_group(gc, policy, d0, d1, capital_usd, label, repeat=1, capital_mult=1.0, model_id="stub",
              rule="stated", allow_leakage=False, shortfall_times=None, decisions_out=True, write=True,
              schedule="triggered", trigger_set="severe", progress=None, aum_usd=None, pricing="displace",
              call_times=None):
    """Run one policy on one group from the 00:00 UTC check of day d0 to the end of day d1 on `schedule`; return the run record."""
    g = gc.g
    if schedule not in SCHEDULES:
        raise ValueError(schedule)
    if trigger_set not in TRIGGERS:
        raise ValueError(trigger_set)
    leak_note = None
    if policy.uses_model and model_id != "stub":
        _ok, leak_note = M.require_eligible(model_id, d0 * C.DAY, rule,
                                            allow_leakage or bool(M.MODELS[model_id].get("leakage_control")))
    upu = gc.usd_per_unit(d0 - 1) or next((gc.usd_per_unit(d) for d in range(d0, d1 + 1) if gc.usd_per_unit(d)), None)
    if upu is None or capital_usd is None:
        raise Unpriced(f"{g.key}: {g.loan_symbol} has no price or no capital at the span start")
    capital = int(capital_usd / upu)
    eng = R.make_replay(g.protocol, g.chain_id, gc.markets, d0 * C.DAY, (d1 + 1) * C.DAY, panel=gc.panel,
                        pricing=pricing)
    eng.fund(capital)
    tags = event_tags(gc, d0, d1, shortfall_times or {})
    for d in range(d0, d1 + 2):
        tags[d * C.DAY].append("day")
    grid = sorted(tags)
    days = list(range(d0 - 1, d1 + 1))
    idle = [0.0] * len(days)
    held_rows = defaultdict(dict)
    ev_snaps, decisions, sched, retries, flows = [], [], [], [], []
    st = {"prev": None, "pending": False, "pending_w": {}, "n_rebal": 0, "last_call": None, "started": False,
          "unpaid": 0.0}
    started = time.time()

    def hook(engine, ts):
        tg = tags[ts]
        ev = [t for t in tg if t != "day"]
        if ev:
            snap = engine.snapshot(ts)
            snap["tags"] = ev
            ev_snaps.append(snap)
        if "day" not in tg:
            return
        d = C.day_of_ts(ts)
        _i0, before = engine.day_state(ts)
        if d <= d1:
            if aum_usd is not None:
                flow(engine, ts, d)
                _i0, before = engine.day_state(ts)
            check_day(engine, ts, d, [m for m, x in before.items() if x[0] > 0])
            report(d)
        idle_now, after = engine.day_state(ts, markets=list(before))
        k = d - 1 - days[0]
        idle[k] = idle_now
        for m, x in after.items():
            if x[0] <= 0 and before.get(m, (0,))[0] > 0:
                x = (x[0], before[m][1]) + tuple(x[2:])
            held_rows[m][k] = x

    def flow(engine, ts, d):
        """Depositor flow at the 00:00 check of day d."""
        target, upu_d = aum_usd.get(d - 1), gc.usd_per_unit(d - 1)
        if target is None or not upu_d:
            return
        nav = engine.nav(ts)
        delta = target / upu_d - nav
        st["unpaid"] = 0.0
        if abs(delta) < REBALANCE_MIN * max(nav, 1.0):
            return
        n0 = len(engine.actions)
        rec = {"ts": ts, "target_usd": target, "asked": delta}
        if delta > 0:
            engine.fund(delta)
            w = {m: x for m, x in (st["prev"] or {}).items() if m in set(gc.stream.table(d)) and m in eng._ix}
            if w:
                obs = engine.observe(ts, sorted(w))
                engine.execute(ts, {m: obs[m]["position"] + x * delta for m, x in w.items()})
            rec["done"] = delta
        else:
            need = -delta
            paid = engine.payout(need)
            if paid < need:
                held = [m for m, x in engine.day_state(ts)[1].items() if x[0] > 0]
                obs = engine.observe(ts, held)
                tot = sum(obs[m]["position"] for m in held)
                rest = need - paid
                if tot > 0:
                    engine.execute(ts, {m: obs[m]["position"] * (1 - min(rest / tot, 1.0)) for m in held})
                    paid += engine.payout(rest)
                    rest = need - paid
                    for m in sorted(held, key=lambda m: -engine.observe(ts, [m])[m]["liquidity"]):
                        if rest <= 0:
                            break
                        o = engine.observe(ts, [m])[m]
                        if o["position"] > 0 and o["liquidity"] > 0:
                            engine.execute(ts, {m: o["position"] - min(rest, o["position"], o["liquidity"])})
                            paid += engine.payout(rest)
                            rest = need - paid
            rec["done"] = -paid
            st["unpaid"] = need - paid
        for a in engine.actions[n0:]:
            a["flow"] = True
        flows.append(rec)

    def check_day(engine, ts, d, held):
        rec_o = gc.stream.record(d)
        held_labels = {gc.stream.label_of[m] for m in held}
        if call_times is not None:
            trig = ["recorded"] if ts in call_times else []
        else:
            trig = triggers(rec_o, held_labels, set(gc.keys(held, ts)), st["last_call"], d, schedule, trigger_set)
        sched.append([d, int(bool(trig)), ";".join(trig)])
        if not trig:
            if st["pending_w"]:
                retry(engine, ts, d)
            return
        st["last_call"] = d
        decide_day(engine, ts, d, held, trig)

    def report(d):
        if progress is not None:
            progress(d - d0 + 1, d1 - d0 + 1, len(decisions), sum(x.get("cost_usd", 0.0) for x in decisions))

    def settle(acts, w, flags):
        """Pending withdrawals after an execution."""
        ms = set(flags.get("clipped_markets") or ()) | {a["market"] for a in acts if a["clipped"]}
        st["pending_w"] = {m: w.get(m, 0.0) for m in sorted(ms)}

    def retry(engine, ts, d):
        """A day without a call."""
        w, pend = st["prev"] or {}, st["pending_w"]
        ms = sorted(set(w) | set(pend))
        obs = engine.observe(ts, ms)
        nav = engine.nav(ts)
        cur = {m: obs[m]["position"] for m in ms}
        floor = REBALANCE_MIN * nav
        table = [m for m in ms if m in pend or w.get(m, 0.0) * nav - cur[m] >= floor]
        flags = {}
        plan, short = plan_moves(w, table, cur, obs, nav, engine.idle, flags)
        n_before = len(engine.actions)
        if plan:
            engine.execute(ts, plan)
        acts = engine.actions[n_before:]
        moved = any(float(a["done"]) > 0 for a in acts)
        settle(acts, w, flags)
        st["pending"] = short or bool(st["pending_w"])
        st["n_rebal"] += int(moved)
        retries.append({"ts": ts, "pending": sorted(pend), "rebalance": moved,
                        "markets_moved": len({a["market"] for a in acts if float(a["done"]) > 0}),
                        "still_pending": sorted(st["pending_w"])})

    def decide_day(engine, ts, d, held, trig):
        open_ = [m for m in gc.stream.table(d) if m in eng._ix]
        table = sorted(set(open_) | set(held), key=lambda m: gc.stream.label_of[m])
        if not table and not st["started"]:
            st["last_call"] = None
        elif table:
            st["started"] = True
        obs = engine.observe(ts, table)
        open_ = [m for m in open_ if obs[m].get("created", True)]
        table = [m for m in table if m in open_ or m in held]
        below = [m for m in table if m not in open_]
        nav = engine.nav(ts)
        rec = {"ts": ts, "time_utc": C.ts_iso(ts), "triggers": trig, "open": len(open_), "table": table,
               "nav_before": nav}
        dc = {"gc": gc, "ts": ts, "d": d, "obs": obs, "markets": table, "open": set(open_), "below": below,
              "nav": nav, "idle": float(engine.idle), "record": rec, "pending": dict(st["pending_w"]),
              "prev": dict(st["prev"] or {}),
              "flows_net": sum(float(x["done"]) for x in flows if x["ts"] > ts - FLOW_WINDOW_D * C.DAY),
              "unpaid": st["unpaid"], "flows_on": aum_usd is not None, "pricing": pricing, "repeat": repeat,
              "history": [x for x in decisions if x.get("archive")][-HISTORY_N:],
              "apy_after": lambda m, delta: engine.apy_after(m, ts, delta), "engine": engine}
        w = policy.decide(dc)
        if w is None:
            w = dict(st["prev"] or {})
            rec["weights_source"] = "previous"
        cur = {m: obs[m]["position"] for m in table}
        clipped_up = {}
        for m in list(w):
            if m not in table:
                w.pop(m)
                continue
            if m not in open_ and nav > 0 and w[m] > cur.get(m, 0.0) / nav + SAME_TOL:
                clipped_up[m] = w[m]
                w[m] = cur.get(m, 0.0) / nav
        tot = sum(w.values())
        if tot > 1 + WEIGHT_TOL:
            w = {m: x / tot for m, x in w.items()}
        w = {m: x for m, x in w.items() if x > 0}
        rec["weights"] = w
        if clipped_up:
            rec["below_capped"] = clipped_up
        if st["prev"] is not None and same_weights(w, st["prev"]) and not st["pending"]:
            rec["rebalance"] = False
            st["pending_w"] = {}
            decisions.append(rec)
            return
        flags = {}
        plan, short = plan_moves(w, table, cur, obs, nav, engine.idle, flags)
        n_before = len(engine.actions)
        engine.execute(ts, plan)
        acts = engine.actions[n_before:]
        rec["executed"] = acts
        rec["rebalance"] = any(float(a["done"]) > 0 for a in acts)
        settle(acts, w, flags)
        st["pending"] = short or bool(st["pending_w"])
        if st["pending_w"]:
            rec["pending_after"] = sorted(st["pending_w"])
        if short:
            rec["short_of_target"] = True
        st["n_rebal"] += int(rec["rebalance"])
        st["prev"] = w
        decisions.append(rec)

    eng.run(grid, hook)
    events = {}
    for e, aff in C.group_events(g):
        t0 = C.e5_t0(e["event_id"])
        if not d0 * C.DAY < t0 <= (d1 + 1) * C.DAY:
            continue
        t0d = C.day_of_ts(t0)
        ms = {a["market"] for a in aff}
        n_c = sum(1 for d in range(max(t0d - C.EXPOSURE_LOOKBACK_D, d0) - 1, t0d) if ms & set(gc.tables.get(d, [])))
        events[e["event_id"]] = {"t0": t0, "affected_table_days": n_c}
    markets = {}
    n = len(days)
    for m, rows in held_rows.items():
        pos, cfi, u, liq = [0.0] * n, [None] * n, [None] * n, [None] * n
        for k, (p, c, x, q) in rows.items():
            pos[k], cfi[k], u[k], liq[k] = p, c, x, q
        markets[m] = {"pos": pos, "cfi": cfi, "u": u, "liq": liq}
    trig_n = Counter(t for _d, c, tr in sched if c for t in tr.split(";") if t)
    meta = {"group": g.key, "protocol": g.protocol, "chain_id": g.chain_id, "loan_token": g.loan_token,
            "loan_symbol": g.loan_symbol, "loan_unit": g.loan_unit, "decimals": gc.dec,
            "policy": policy.name, "label": label, "repeat": repeat, "capital_raw": capital,
            "mandate": getattr(policy, "mandate", None),
            "capital_usd": capital_usd, "capital_multiplier": capital_mult, "usd_per_unit_start": upu,
            "capital_rule": "fixed (capital_usd)" if aum_usd is None else "follows the median human vault",
            "pricing": pricing, "replayed_calls": call_times is not None,
            "n_flows": len(flows), "deposits_raw": sum(float(x["done"]) for x in flows if float(x["done"]) > 0),
            "withdrawals_raw": -sum(float(x["done"]) for x in flows if float(x["done"]) < 0),
            "unpaid_flow_days": sum(1 for x in flows if float(x["done"]) - float(x["asked"]) > 1e-6 * abs(float(x["asked"]))),
            "first_day": d0, "last_day": d1, "start_utc": C.day_iso(d0), "end_utc": C.day_iso(d1),
            "engine": "approximate (linear utilization response)" if eng.approximate else "exact (IRM port)",
            "markets_loaded": len(gc.markets), "rows_replayed": eng.n_rows,
            "model": model_id if policy.uses_model else None, "eligibility": leak_note,
            "leakage_diagnostic": bool(allow_leakage), "clips": eng.clips,
            "foreign_withdrawals_cut": getattr(eng, "foreign_clips", 0),
            "approx_capped_days": getattr(eng, "capped_days", 0), "seconds": round(time.time() - started, 1),
            "schedule": schedule, "trigger_set": trigger_set, "days_checked": len(sched), "n_calls": len(decisions),
            "n_retries": len(retries), "n_retry_rebalances": sum(1 for x in retries if x["rebalance"]),
            "triggers": dict(sorted(trig_n.items())),
            "events": events, "n_decisions": len(decisions), "n_rebalances": st["n_rebal"],
            "n_kept_previous": sum(1 for x in decisions if x.get("weights_source") == "previous"),
            "n_tool_rounds": sum(len(x.get("tool_rounds") or []) for x in decisions),
            "n_reads": sum(x.get("reads", 0) for x in decisions),
            "tokens_in": sum(x.get("tokens_in", 0) for x in decisions),
            "tokens_out": sum(x.get("tokens_out", 0) for x in decisions),
            "cache_read": sum(x.get("cache_read", 0) for x in decisions),
            "cost_usd": sum(x.get("cost_usd", 0.0) for x in decisions),
            "n_requests_reused": sum(x.get("reused", 0) for x in decisions)}
    keep = ("ts", "rebalance", "weights_source", "tokens_in", "tokens_out", "cache_read", "cache_write",
            "prompt_tokens", "cost_usd", "keys", "used_tools", "reads", "problems", "open", "triggers")
    brief = []
    for x in decisions:
        b = {k: x[k] for k in keep if k in x}
        b["markets_moved"] = len({a["market"] for a in x.get("executed") or [] if float(a["done"]) > 0})
        b["calls"] = len(x.get("archive") or [])
        b["tool_rounds"] = len(x.get("tool_rounds") or [])
        b["table"] = len(x.get("table") or [])
        brief.append(b)
    run = {"meta": meta, "days": days, "usd_per_unit": [gc.usd_per_unit(d) for d in days], "idle": idle,
           "markets": markets, "events": ev_snaps, "schedule": sched, "retries": retries, "flows": flows,
           "actions": eng.actions, "decisions": brief}
    if write:
        write_run(run, label, g, policy.name, repeat)
        if decisions_out:
            write_decisions(decisions, label, g, repeat)
    return run


def _jsonable(o):
    if isinstance(o, dict):
        return {k: _jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(v) for v in o]
    if isinstance(o, int) and not isinstance(o, bool) and abs(o) > 2 ** 53:
        return str(o)
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    return o


def _gz(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as fh:
        fh.write(text)
    os.replace(tmp, path)


def run_path(label, g, policy_name, repeat):
    return C.OUT / "runs" / label / f"{g.key}__{policy_name}__r{repeat}.json.gz"


def frame_path(label, g, policy_name, repeat):
    return C.OUT / "runs" / label / f"{g.key}__{policy_name}__r{repeat}.e4.csv.gz"


def write_run(run, label, g, policy_name, repeat):
    _gz(run_path(label, g, policy_name, repeat), json.dumps(_jsonable(run)))


def write_decisions(decisions, label, g, repeat):
    path = C.OUT / "decisions" / label / f"{g.key}__r{repeat}.jsonl.gz"
    _gz(path, "\n".join(json.dumps(_jsonable(x), ensure_ascii=False) for x in decisions) + "\n")


def load_run(path):
    with gzip.open(path, "rt") as fh:
        return json.load(fh)


def make_llm_policy(model, label, live=False, batch=False, tools=True, name="autocurator", target=None,
                    client=None, corpus=None, schedule="triggered", trigger_set="severe"):
    """Client, corpus and policy for one model (stub client unless live)."""
    client = client or M.make_client(model, label, live=live, batch=batch, target=target)
    corpus = corpus or T.Corpus()
    return LLMPolicy(client, corpus, label, tools=tools, name=name, schedule=schedule, triggers=trigger_set), corpus
