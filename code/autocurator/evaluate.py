# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "scipy>=1.11", "pycryptodome", "matplotlib>=3.8"]
# ///
"""E8 evaluation per model span."""
import csv
import gzip
import json
import math
import re
import statistics
from collections import defaultdict

import pandas as pd

import baselines as B
import context as C
import decide as D
import e4span as S
import models as M

RES = C.OUT / "results"
TOL = 1e-4
LOCK_L = 0.05
MIN_ACTIVE_DAYS = 30
LOOKBACK_D = C.EXPOSURE_LOOKBACK_D


def num(x):
    if x in (None, ""):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) else v


def ok(x):
    """A number: not None and not NaN."""
    return x is not None and x == x


def mean(xs):
    xs = [x for x in xs if ok(x)]
    return sum(xs) / len(xs) if xs else None


def median(xs):
    xs = [x for x in xs if ok(x)]
    return statistics.median(xs) if xs else None


def spans(stub_start, models=None, rule="stated"):
    """[(span name, first day, cutoff text)]."""
    out = [("stub", stub_start, f"--span-start {C.day_iso(stub_start)}"),
           ("common", C.COMMON_START, "the common window")]
    for m in models if models is not None else M.PROPOSED:
        out.append((m, C.span_start_day(m, rule), M.bound_text(m)))
    return out


def label_model(label):
    """Model id behind a label ('stub' included, also a dry run under another --label such as stub-s20251002), None for a baseline."""
    base = re.sub(r"-x[0-9.]+$", "", re.sub(r"-(impact|displace)$", "", label.split("@")[0]))
    if base.startswith(("baseline-", "ablate-")):
        return None
    base = re.sub(r"-(daily|aggressive)$", "", base.split("-ablate-")[0])
    if base in M.MODELS:
        return base
    return "stub" if base.startswith("stub") else None


_IDLE_MARKETS = {}


def idle_markets(group):
    """Markets of the group's protocol and chain with no collateral (a protocol's idle market, which holds a vault's cash and pays nothing)."""
    k = (group.protocol, group.chain_id)
    if k not in _IDLE_MARKETS:
        mk = C.Markets(group.protocol, group.chain_id)
        _IDLE_MARKETS[k] = {m for m, i in mk.info.items()
                            if all(c[0] in (None, "", C.ZERO) for c in (i.get("collaterals") or [(None, None)]))}
    return _IDLE_MARKETS[k]


def invested_share(run, group, d0, d1):
    """Mean over the days d0..d1 of the share of the vault's assets lent in markets with collateral; cash and idle-market positions count as cash."""
    days = run["days"]
    ks = [k for k, d in enumerate(days) if d0 <= d <= d1]
    if not ks:
        return None
    idle = idle_markets(group)
    out = []
    for k in ks:
        lent = sum(float(x["pos"][k] or 0) for m, x in run["markets"].items() if m not in idle)
        tot = sum(float(x["pos"][k] or 0) for x in run["markets"].values()) + float(run["idle"][k] or 0)
        out.append(lent / tot if tot > 0 else 0.0)
    return sum(out) / len(out)


def part(group, start, run_meta=None):
    """(d0, d1) of the group's part of a span starting at `start`, or None; with run_meta, None unless the run covers it."""
    w = group.window(start)
    if w is None:
        return None
    if run_meta is not None and (run_meta["first_day"] > w[0] or run_meta["last_day"] < w[1]):
        return None
    return w


def percentile(value, humans, better="lower", tol=TOL):
    """Share of humans the value beats, ties counting half, in percent."""
    hs = [h for h in humans if ok(h)]
    if not ok(value) or not hs:
        return None
    beat = tie = 0
    for h in hs:
        if abs(value - h) <= tol:
            tie += 1
        elif (value < h) if better == "lower" else (value > h):
            beat += 1
    return 100.0 * (beat + 0.5 * tie) / len(hs)


def compare(value, ref, better="lower", tol=TOL):
    """'win', 'tie' or 'loss' of value against a reference."""
    if not ok(value) or not ok(ref):
        return None
    if abs(value - ref) <= tol:
        return "tie"
    return "win" if ((value < ref) if better == "lower" else (value > ref)) else "loss"


def versus(value, humans, better):
    """Percentile, median, best, gap to the median (points) and win/tie/loss."""
    hs = [h for h in humans if ok(h)]
    if not hs:
        return {"n": 0}
    med = statistics.median(hs)
    best = min(hs) if better == "lower" else max(hs)
    return {"n": len(hs), "median": med, "best": best, "pct": percentile(value, hs, better),
            "gap_pp": 100 * (value - med) if ok(value) else None,
            "vs_median": compare(value, med, better), "vs_best": compare(value, best, better)}


def wtl(rows, key):
    c = defaultdict(int)
    for r in rows:
        if r.get(key):
            c[r[key]] += 1
    return f"{c['win']}/{c['tie']}/{c['loss']}"


class Inputs:
    """E5 shortfall ratios and valuation times, E4 human daily rows, groups."""

    def __init__(self, with_humans=True):
        self.events = C.load_events()
        self.ratio, self.valuation = {}, {}
        with open(C.INPUTS / "metrics/e5/market_shortfall.csv") as fh:
            for r in csv.DictReader(fh):
                self.ratio[(r["event_id"], r["market"])] = num(r["shortfall_ratio"]) or 0.0
                if r.get("valuation_utc"):
                    self.valuation[(r["event_id"], r["market"])] = C.parse_utc(r["valuation_utc"])
        self.groups = {g.key: g for g in C.load_groups()}
        self.humans = S.HumanDaily() if with_humans else None
        self._hspan = {}

    def human_span(self, group, d0, d1):
        """[(scorecard row, span values)] of the group's humans with at least MIN_ACTIVE_DAYS E4 active days in d0..d1."""
        key = (group.key, d0, d1)
        if key not in self._hspan:
            out = []
            for h in group.humans:
                v = self.humans.span(h, d0, d1)
                if v and v["active_days"] >= MIN_ACTIVE_DAYS:
                    out.append((h, v))
            self._hspan[key] = out
        return self._hspan[key]


def shortfall_times():
    """{(event, market): E5 valuation time} for run.py's event snapshots."""
    out = {}
    with open(C.INPUTS / "metrics/e5/market_shortfall.csv") as fh:
        for r in csv.DictReader(fh):
            if r.get("valuation_utc"):
                out[(r["event_id"], r["market"])] = C.parse_utc(r["valuation_utc"])
    return out


def event_outcome(run, frame, e, aff, inp):
    """E5's measures of one run in one event (module docstring), or None when the run does not cover t0 or the group never had."""
    meta = run["meta"]
    eid = e["event_id"]
    info = (meta.get("events") or {}).get(eid)
    if not info or not info.get("affected_table_days"):
        return None
    snaps = run["events"]
    s_t0 = next((s for s in snaps if f"t0:{eid}" in s["tags"]), None)
    if s_t0 is None:
        return None
    t0 = int(s_t0["ts"])
    days = run["days"]
    d_first = days[0]
    upu = run["usd_per_unit"]
    g_aff = {a["market"]: a for a in aff}

    def weight(m):
        w = g_aff[m].get("family_debt_share_at_t0")
        return 1.0 if w is None else float(w)

    def usd_at(raw, d):
        k = min(max(d - d_first, 0), len(days) - 1)
        return float(raw) * (upu[k] or 0.0)

    nav_t0 = float(s_t0["idle"]) + sum(float(x["position"]) for x in s_t0["markets"].values())
    exp_t0 = sum(float(x["position"]) * weight(m) for m, x in s_t0["markets"].items() if m in g_aff)
    t0d = C.day_of_ts(t0)
    exp_t0_usd = usd_at(exp_t0, t0d - 1)
    path = []
    for d in range(max(t0d - LOOKBACK_D, d_first), t0d):
        k = d - d_first
        x = sum(run["markets"][m]["pos"][k] * weight(m) for m in g_aff if m in run["markets"])
        path.append(((d + 1) * C.DAY, usd_at(x, d)))
    path.append((t0, exp_t0_usd))
    peak = max((x for _d, x in path), default=0.0)
    held = peak >= C.MATERIAL_USD
    grp = "never_entered" if not held else "exited_before_t0" if exp_t0_usd < 0.10 * peak else "present_at_t0"
    lead = None
    if grp == "exited_before_t0":
        idx = [i for i, (_d, x) in enumerate(path) if x >= 0.5 * peak]
        if idx and idx[-1] + 1 < len(path):
            lead = (t0 - path[idx[-1] + 1][0]) / C.DAY
    loss_raw, detail = 0.0, {}
    if e["type"] == "depeg_loss":
        for m, a in g_aff.items():
            ratio = inp.ratio.get((eid, m))
            if ratio is None:
                continue
            s_v = next((s for s in snaps if f"valuation:{eid}:{m}" in s["tags"]), None)
            if s_v is None:
                continue
            s_tr = next((s for s in snaps if f"trigger:{eid}:{m}" in s["tags"]), None)
            z = s_v["markets"].get(m, {})
            st = (s_tr or {}).get("markets", {}).get(m, {})
            pos_on = float(st.get("position", 0))
            flows = float(z.get("ledger", 0)) - float(st.get("ledger", 0))
            wo = float(z.get("writeoff", 0)) - float(st.get("writeoff", 0))
            principal = max(pos_on + flows - wo, 0.0)
            loss = principal * ratio + wo
            if loss > 0:
                detail[m] = {"principal": principal, "ratio": ratio, "writeoff": wo}
            loss_raw += loss
    stop_d = min(C.day_of_ts(int(e["end_ts"])), t0d + 30, days[-1])
    locked = 0
    if frame is not None:
        L = dict(zip(frame["day"], frame["L"]))
        for d in range(t0d, stop_d + 1):
            k = d - d_first
            if k >= len(days):
                break
            x = sum(run["markets"][m]["pos"][k] * weight(m) for m in g_aff if m in run["markets"])
            Ld = L.get(d)
            if Ld is not None and Ld == Ld and Ld < LOCK_L and usd_at(x, d) >= C.MATERIAL_USD:
                locked += 1
    return {"label": meta["label"], "policy": meta["policy"], "repeat": meta["repeat"], "event_id": eid,
            "group": meta["group"], "approx_replay": str(meta["engine"]).startswith("approximate"),
            "nav_t0_usd": usd_at(nav_t0, t0d - 1), "exposure_t0_usd": exp_t0_usd,
            "exposure_share_t0": exp_t0 / nav_t0 if nav_t0 > 0 else None, "e5_group": grp,
            "exit_lead_days": lead, "loss_usd": usd_at(loss_raw, t0d - 1),
            "loss_per_dollar": loss_raw / nav_t0 if nav_t0 > 0 else None, "days_locked": locked,
            "loss_detail": json.dumps(detail)}


def pool_events(rows):
    """Event outcome per (span, label, repeat, event)."""
    by = defaultdict(list)
    for r in rows:
        by[(r["span"], r["label"], r["repeat"], r["event_id"])].append(r)
    out = []
    for (span, label, rep, eid), rs in sorted(by.items()):
        nav = sum(r["nav_t0_usd"] or 0 for r in rs)
        loss = sum(r["loss_usd"] or 0 for r in rs)
        out.append({"span": span, "label": label, "repeat": rep, "event_id": eid, "groups": len(rs),
                    "nav_t0_usd": nav, "exposure_t0_usd": sum(r["exposure_t0_usd"] or 0 for r in rs),
                    "loss_usd": loss, "loss_per_dollar": loss / nav if nav > 0 else None,
                    "days_locked": max(r["days_locked"] for r in rs),
                    "approx_replay_groups": sum(1 for r in rs if r.get("approx_replay")),
                    "group_keys": ";".join(sorted(r["group"] for r in rs))})
    return out


def table6(group_rows, event_rows, span_meta):
    """One row per (span, label)."""
    by_g = defaultdict(lambda: defaultdict(list))
    for r in group_rows:
        by_g[(r["span"], r["label"])][r["repeat"]].append(r)
    by_e = defaultdict(lambda: defaultdict(list))
    for r in event_rows:
        by_e[(r["span"], r["label"])][r["repeat"]].append(r)
    out = []
    for key in sorted(set(by_g) | set(by_e), key=lambda k: ([s for s, *_ in span_meta].index(k[0]), k[1])):
        span, label = key
        reps = sorted(set(by_g[key]) | set(by_e[key]))
        per = []
        for rep in reps:
            gs, es = by_g[key].get(rep, []), by_e[key].get(rep, [])
            cmp_ = [r for r in gs if r.get("humans_yield")]
            per.append({
                "groups": len(gs), "groups_compared": len(cmp_),
                "approx": sum(1 for r in gs if r.get("approx_replay")),
                "yield_gap": mean([r.get("yield_gap_pp") for r in cmp_]),
                "tl_gap": mean([r.get("tail_loss_gap_pp") for r in cmp_]),
                "yield_gap_med": median([r.get("yield_gap_pp") for r in cmp_]),
                "tl_gap_med": median([r.get("tail_loss_gap_pp") for r in cmp_]),
                "pct_yield": mean([r.get("pct_yield") for r in cmp_]),
                "pct_tl": mean([r.get("pct_tail_loss") for r in cmp_]),
                "vault_yield": mean([r.get("yield_before_fee") for r in cmp_]),
                "human_median_yield": mean([r.get("human_median_yield") for r in cmp_]),
                "vault_tl": mean([r.get("tl_headline") for r in cmp_]),
                "human_median_tl": mean([r.get("human_median_tail_loss") for r in cmp_]),
                "L_p10": mean([r.get("L_p10") for r in cmp_]), "idle": mean([r.get("share_idle_mean") for r in cmp_]),
                "wtl_my": wtl(cmp_, "yield_vs_median"), "wtl_by": wtl(cmp_, "yield_vs_best"),
                "wtl_mt": wtl(cmp_, "tail_loss_vs_median"), "wtl_bt": wtl(cmp_, "tail_loss_vs_best"),
                "events": len(es), "zero": sum(1 for r in es if (r["exposure_t0_usd"] or 0) < 1.0),
                "loss": mean([r["loss_per_dollar"] for r in es]), "pct_loss": mean([r.get("pct_loss") for r in es]),
                "wtl_ml": wtl(es, "vs_median_loss"), "wtl_bl": wtl(es, "vs_best_loss")})
        sm = next(x for x in span_meta if x[0] == span)
        gaps = [p["yield_gap"] for p in per if p["yield_gap"] is not None]
        losses = [p["loss"] for p in per if p["loss"] is not None]
        p0 = per[0]
        out.append({
            "span": span, "label": label, "cutoff": sm[2], "span_start": C.day_iso(max(sm[1], C.e2.day_of("2024-01-01"))),
            "span_days": C.END_DAY - max(sm[1], C.e2.day_of("2024-01-01")) + 1, "repeats": len(per),
            "groups": p0["groups"], "groups_compared": p0["groups_compared"], "groups_on_approx_replay": p0["approx"],
            "events_in_span": p0["events"],
            "mean_yield_gap_pp": mean([p["yield_gap"] for p in per]),
            "yield_gap_range_pp": f"{min(gaps):.3f}..{max(gaps):.3f}" if gaps else "",
            "mean_tail_loss_gap_pp": mean([p["tl_gap"] for p in per]),
            "median_yield_gap_pp": mean([p["yield_gap_med"] for p in per]),
            "median_tail_loss_gap_pp": mean([p["tl_gap_med"] for p in per]),
            "mean_pct_yield": mean([p["pct_yield"] for p in per]),
            "mean_pct_tail_loss": mean([p["pct_tl"] for p in per]),
            "wtl_yield_vs_median": p0["wtl_my"], "wtl_yield_vs_best": p0["wtl_by"],
            "wtl_tail_loss_vs_median": p0["wtl_mt"], "wtl_tail_loss_vs_best": p0["wtl_bt"],
            "events_zero_exposure_t0": p0["zero"],
            "mean_loss_per_dollar": mean([p["loss"] for p in per]),
            "loss_range": f"{min(losses):.6f}..{max(losses):.6f}" if losses else "",
            "mean_pct_loss": mean([p["pct_loss"] for p in per]),
            "wtl_loss_vs_median": p0["wtl_ml"], "wtl_loss_vs_best": p0["wtl_bl"],
            "mean_vault_yield": mean([p["vault_yield"] for p in per]),
            "mean_human_median_yield": mean([p["human_median_yield"] for p in per]),
            "mean_vault_tail_loss": mean([p["vault_tl"] for p in per]),
            "mean_human_median_tail_loss": mean([p["human_median_tl"] for p in per]),
            "mean_vault_L_p10": mean([p["L_p10"] for p in per]), "mean_vault_idle": mean([p["idle"] for p in per])})
    return out


def write_csv(path, rows, meta):
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if v is None else v) for k, v in r.items()})
    C.write_sidecar(path, {"generator": "code/autocurator/evaluate.py", "rows": len(rows), "inputs": str(C.INPUTS),
                           **meta})


def draw_fig8(fig_rows, path, span="stub"):
    """Draft Fig 8: loss per dollar per event (top) and yield per group (bottom); humans grey dots with a box plot, model runs filled markers, baselines hollow markers."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    rows = [r for r in fig_rows if r["span"] == span]
    fig, axes = plt.subplots(2, 1, figsize=(16, 9))
    for ax, panel, ylab in ((axes[0], "event", "Realized loss per dollar"), (axes[1], "group", "Yield before fee (span)")):
        rs = [r for r in rows if r["panel"] == panel and r["value"] is not None]
        xs = sorted({r["x"] for r in rs}, key=lambda x: (x.split("|")[0], x))
        if not xs:
            continue
        xi = {x: i for i, x in enumerate(xs)}
        data = [[r["value"] for r in rs if r["x"] == x and r["series"] == "human"] for x in xs]
        ax.boxplot([d or [float("nan")] for d in data], positions=range(len(xs)), widths=0.5, showfliers=False,
                   medianprops={"color": "0.3"}, boxprops={"color": "0.6"}, whiskerprops={"color": "0.6"},
                   capprops={"color": "0.6"})
        for i, d in enumerate(data):
            ax.scatter([i] * len(d), d, s=6, color="0.6", alpha=0.5, zorder=2)
        labels = sorted({r["label"] for r in rs if r["series"] != "human"})
        for j, lab in enumerate(labels):
            pts = [(xi[r["x"]] + 0.08 * (j - len(labels) / 2), r["value"]) for r in rs if r["label"] == lab]
            hollow = lab.startswith(("baseline", "ablate"))
            ax.scatter([p[0] for p in pts], [p[1] for p in pts], s=16, marker="s" if hollow else "o",
                       facecolors="none" if hollow else f"C{j % 10}", edgecolors=f"C{j % 10}", label=lab, zorder=3)
        ax.set_ylabel(ylab)
        ax.set_xticks(range(len(xs)))
        ax.set_xticklabels([x.split("|")[-1] for x in xs], rotation=90, fontsize=5)
        ax.grid(axis="y", color="0.9")
        if panel == "group":
            ax.set_ylim(-0.2, 0.4)
    axes[0].legend(fontsize=6, ncol=4, frameon=False)
    fig.suptitle(f"Draft Fig 8, span {span} (dry run: the model rows are the deterministic stub)", fontsize=9)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path)
    plt.close(fig)


def load_frame(path):
    if not path.exists():
        return None
    df = pd.read_csv(path, dtype={"date": str, "vault": str, "protocol": str, "kind": str,
                                  "curator_entity": str, "curator_confidence": str,
                                  "worst_family_headline": str, "apy_source": str})
    if "day" not in df:
        df["day"] = [C.e2.day_of(x) for x in df["date"]]
    return df


_EV = {}


def _eval_init(span_meta):
    _EV.update(inp=Inputs(), span_meta=span_meta)


def _eval_run(path):
    """One run's group rows (every span it is evaluated on), run-event rows and human span rows."""
    inp, span_meta = _EV["inp"], _EV["span_meta"]
    group_rows, ev_runs, human_rows = [], [], {}
    run = D.load_run(path)
    meta = run["meta"]
    g = inp.groups.get(meta["group"])
    if g is not None:
        frame = load_frame(path.with_name(path.name.replace(".json.gz", ".e4.csv.gz")))
        lm = label_model(meta["label"])
        for e, aff in C.group_events(g):
            r = event_outcome(run, frame, e, aff, inp)
            if r is not None:
                r["onset_ts"] = int(e["onset_ts"])
                ev_runs.append(r)
        for span, start, _cut in span_meta:
            if lm is not None and lm != "stub" and lm != span and span != "common":
                continue
            if "@" in meta["label"] and meta["label"].split("@")[1] != span:
                continue
            w = part(g, start, meta)
            if w is None or frame is None:
                continue
            d0, d1 = w
            v = S.summarize(frame, d0, d1, {"chain_id": g.chain_id, "vault": meta["group"], "protocol": g.protocol,
                                             "kind": "autocurator", "asset_symbol": g.loan_symbol,
                                             "loan_unit": g.loan_unit, "name": meta["label"]})
            hs = inp.human_span(g, d0, d1)
            for h, hv in hs:
                human_rows[(span, g.key, h["vault"])] = {"span": span, "group": g.key, "vault": h["vault"],
                                                         "name": h.get("name"), "curator": h.get("curator_name"),
                                                         "kind": h["kind"], "first_day": C.day_iso(d0),
                                                         "last_day": C.day_iso(d1), **hv}
            row = {"span": span, "label": meta["label"], "policy": meta["policy"], "repeat": meta["repeat"],
                   "group": g.key, "protocol": g.protocol, "chain_id": g.chain_id, "loan": g.loan_symbol,
                   "first_day": C.day_iso(d0), "last_day": C.day_iso(d1), "days": d1 - d0 + 1,
                   "approx_replay": str(meta["engine"]).startswith("approximate"),
                   "capital_usd": meta["capital_usd"], "capital_multiplier": meta.get("capital_multiplier", 1.0),
                   "invested_share": invested_share(run, g, d0, d1), **(v or {})}
            add_group_stats(row, v, [hv for _h, hv in hs])
            group_rows.append(row)
    return group_rows, ev_runs, human_rows


def evaluate(stub_start, labels=None, models=None, log=print, with_aave=True, rule="stated", workers=1):
    """Every run under data/autocurator/runs/ (or `labels`) on every span; `workers` processes over the runs."""
    inp = Inputs()
    span_meta = spans(stub_start, models, rule)
    run_paths = sorted(p for p in (C.OUT / "runs").glob("*/*.json.gz") if not p.name.endswith(".e4.csv.gz"))
    if labels:
        run_paths = [p for p in run_paths if p.parent.name in labels]
    group_rows, ev_runs, human_rows = [], [], {}
    if workers > 1:
        from concurrent.futures import ProcessPoolExecutor
        with ProcessPoolExecutor(workers, initializer=_eval_init, initargs=(span_meta,)) as ex:
            parts = list(ex.map(_eval_run, run_paths, chunksize=8))
    else:
        _eval_init(span_meta)
        parts = [_eval_run(p) for p in run_paths]
    for gr, er, hr in parts:
        group_rows += gr
        ev_runs += er
        human_rows.update(hr)
    coverage = []
    if with_aave:
        aave = B.Aave()
        bench = {x.key for x in C.bench_groups()}
        for g in (x for x in inp.groups.values() if x.key in bench):
            r = B.passive_aave(g, aave) if g.chain_id in B.LLAMA_CHAIN else None
            n_all = g.last_day - g.first_day + 1
            q = sum(1 for d in range(g.first_day, g.last_day + 1) if r and d in r["series"])
            coverage.append({"group": g.key, "protocol": g.protocol, "chain_id": g.chain_id, "loan_token": g.loan_token,
                             "loan": g.loan_symbol, "pool": (r or {}).get("pool"), "pool_symbol": (r or {}).get("symbol"),
                             "days_quoted": q, "days_window": n_all, "available": bool(r) and q >= 0.5 * n_all,
                             "reason": "" if r else "no aave-v3 pool for this chain and token in DefiLlama's pool list"})
            if not r:
                continue
            for span, start, _cut in span_meta:
                w = part(g, start)
                if w is None:
                    continue
                y, nq, nd = B.aave_span(r["series"], *w)
                if y is None:
                    continue
                v = {"active_days": nd, "return_days": nq, "yield_before_fee": y, "yield_after_fee": y,
                     "tl_headline": 0.0, "L_p10": 1.0, "L_median": 1.0, "share_idle_mean": 0.0}
                hs = inp.human_span(g, *w)
                row = {"span": span, "label": "baseline-passive_aave", "policy": "passive_aave", "repeat": 1,
                       "group": g.key, "protocol": g.protocol, "chain_id": g.chain_id, "loan": g.loan_symbol,
                       "first_day": C.day_iso(w[0]), "last_day": C.day_iso(w[1]), "days": w[1] - w[0] + 1,
                       "approx_replay": False, "capital_usd": None, "capital_multiplier": 1.0, **v}
                add_group_stats(row, v, [hv for _h, hv in hs])
                group_rows.append(row)
    ev_span = []
    for r in ev_runs:
        lm = label_model(r["label"])
        for span, start, _cut in span_meta:
            if lm is not None and lm != "stub" and lm != span and span != "common":
                continue
            if "@" in r["label"] and r["label"].split("@")[1] != span:
                continue
            g = inp.groups[r["group"]]
            w = part(g, start)
            if w is None or not r["onset_ts"] > w[0] * C.DAY:
                continue
            if lm not in (None, "stub") and not M.MODELS[lm].get("leakage_control"):
                M.require_eligible(lm, r["onset_ts"], rule)
            ev_span.append({**r, "span": span})
    aave_ok = {(r["span"], r["group"]) for r in group_rows if r["label"] == "baseline-passive_aave"}
    for span, eid, gk, onset in sorted({(r["span"], r["event_id"], r["group"], r["onset_ts"]) for r in ev_span}):
        if (span, gk) in aave_ok:
            ev_span.append({"span": span, "label": "baseline-passive_aave", "policy": "passive_aave", "repeat": 1,
                            "event_id": eid, "group": gk, "approx_replay": False, "nav_t0_usd": 1.0,
                            "exposure_t0_usd": 0.0, "exposure_share_t0": 0.0, "e5_group": "never_entered",
                            "exit_lead_days": None, "loss_usd": 0.0, "loss_per_dollar": 0.0, "days_locked": 0,
                            "loss_detail": "{}", "onset_ts": onset})
    ev_pooled = pool_events(ev_span)
    hum_ev = {}
    for r in ev_pooled:
        hs = []
        for gk in r["group_keys"].split(";"):
            for h in C.e5_humans(r["event_id"], inp.groups[gk]):
                hs.append(h)
                hum_ev[(r["event_id"], h["vault"])] = {"event_id": r["event_id"], "group": gk, "vault": h["vault"],
                                                       "name": h["name"], "curator": h["curator_name"],
                                                       "e5_group": h["group"],
                                                       "loss_per_dollar": num(h["loss_per_dollar"]) or 0.0,
                                                       "exposure_t0_usd": num(h["exposure_t0_usd_counted"]) or 0.0,
                                                       "exit_lead_days": num(h["exit_lead_days"]),
                                                       "days_locked": num(h["days_locked"])}
        hl = [num(h["loss_per_dollar"]) or 0.0 for h in hs]
        x = versus(r["loss_per_dollar"], hl, "lower")
        r.update({"humans": x["n"], "human_median_loss": x.get("median"), "human_best_loss": x.get("best"),
                  "pct_loss": x.get("pct"), "vs_median_loss": x.get("vs_median"), "vs_best_loss": x.get("vs_best")})
    t6 = table6(group_rows, ev_pooled, span_meta)
    meta = {"spans": [(s, C.day_iso(d), c) for s, d, c in span_meta], "tolerance": TOL, "lock_threshold": LOCK_L,
            "min_active_days": MIN_ACTIVE_DAYS}
    write_csv(RES / "group_span.csv", group_rows, meta)
    write_csv(RES / "humans_span.csv", list(human_rows.values()), meta)
    write_csv(RES / "event_runs.csv", ev_runs, meta)
    write_csv(RES / "outcomes_events.csv", ev_pooled, meta)
    write_csv(RES / "humans_events.csv", list(hum_ev.values()), meta)
    write_csv(RES / "table6.csv", t6, meta)
    if with_aave:
        write_csv(RES / "passive_aave_coverage.csv", coverage,
                  {**meta, "pool_list": "https://yields.llama.fi/pools (aave-v3, chain, loan token among "
                                        "underlyingTokens, largest plain pool by TVL)", "min_quoted_share": 0.5})
    fig = []
    for h in hum_ev.values():
        for span, *_ in span_meta:
            if any(r["span"] == span and r["event_id"] == h["event_id"] for r in ev_pooled):
                fig.append({"panel": "event", "span": span, "x": f"{C.load_events()[h['event_id']]['onset_utc'][:10]}|"
                            f"{h['event_id']}", "series": "human", "label": h["vault"], "value": h["loss_per_dollar"]})
    for r in ev_pooled:
        if r["repeat"] == 1:
            fig.append({"panel": "event", "span": r["span"], "x": f"{C.load_events()[r['event_id']]['onset_utc'][:10]}|"
                        f"{r['event_id']}", "series": "baseline" if label_model(r["label"]) is None else "model",
                        "label": r["label"], "value": r["loss_per_dollar"]})
    for h in human_rows.values():
        fig.append({"panel": "group", "span": h["span"], "x": f"{h['group']}|{h['group']}", "series": "human",
                    "label": h["vault"], "value": h["yield_before_fee"]})
    for r in group_rows:
        if r["repeat"] == 1 and r.get("humans_yield"):
            fig.append({"panel": "group", "span": r["span"], "x": f"{r['group']}|{r['group']}",
                        "series": "baseline" if label_model(r["label"]) is None else "model",
                        "label": r["label"], "value": r.get("yield_before_fee")})
    write_csv(RES / "fig8_data.csv", fig, meta)
    epm = []
    for span, start, cut in span_meta:
        for eid in C.all_event_ids():
            e = C.load_events()[eid]
            ok = int(e["onset_ts"]) > start * C.DAY
            epm.append({"model": span, "span_start": C.day_iso(start), "event_id": eid, "onset_utc": e["onset_utc"],
                        "eligible": ok, "evaluated_groups": sum(1 for r in ev_pooled if r["span"] == span
                                                               and r["event_id"] == eid and r["repeat"] == 1
                                                               and label_model(r["label"]) in (span, "stub"))})
    write_csv(RES / "events_per_model.csv", epm, meta)
    draw_fig8(fig, C.OUT / "figures" / "fig8_draft.pdf")
    log(f"evaluated {len(run_paths)} runs: {len(group_rows)} group rows, {len(ev_runs)} run-event rows, "
        f"{len(ev_pooled)} event rows, {len(human_rows)} human span rows")
    return t6


def add_group_stats(row, v, hvs):
    """Percentiles, gaps and win/tie/loss of a vault's span values against the humans'."""
    if not v:
        row["humans_yield"] = 0
        return row
    y = versus(v.get("yield_before_fee"), [h.get("yield_before_fee") for h in hvs], "higher")
    t = versus(v.get("tl_headline"), [h.get("tl_headline") for h in hvs], "lower")
    row.update({"humans_yield": y["n"], "human_median_yield": y.get("median"), "human_best_yield": y.get("best"),
                "pct_yield": y.get("pct"), "yield_gap_pp": y.get("gap_pp"), "yield_vs_median": y.get("vs_median"),
                "yield_vs_best": y.get("vs_best"),
                "humans_tail_loss": t["n"], "human_median_tail_loss": t.get("median"),
                "human_best_tail_loss": t.get("best"), "pct_tail_loss": t.get("pct"),
                "tail_loss_gap_pp": t.get("gap_pp"), "tail_loss_vs_median": t.get("vs_median"),
                "tail_loss_vs_best": t.get("vs_best")})
    return row


if __name__ == "__main__":
    print(__doc__)
