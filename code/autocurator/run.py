# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "scipy>=1.11", "pycryptodome", "matplotlib>=3.8"]
# ///
"""AutoCurator entry point (E8 decisions and evaluation, E9 ablations)."""
import argparse
import gzip
import json
import os
import pathlib
import shutil
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

import context as C
import models as M

LOG = C.WORK / "logs"


def log(msg):
    LOG.mkdir(parents=True, exist_ok=True)
    line = f"{C.now_iso()} {msg}"
    if Progress.live is not None:
        Progress.live.clear()
    print(line, flush=True)
    with open(LOG / "run.log", "a") as fh:
        fh.write(line + "\n")


_W = {}


def progress_writer(group_key, label, rep):
    """A run_group progress callback that writes the run's state to a file in $AC_PROGRESS_DIR (set by the main process; none."""
    root = os.environ.get("AC_PROGRESS_DIR")
    if not root:
        return None
    path = pathlib.Path(root) / f"{group_key}__{label}__r{rep}.json"
    last = {"t": 0.0, "calls": -1}

    def write(done, total, calls, cost):
        now = time.time()
        if calls == last["calls"] and done < total and now - last["t"] < 1.0:
            return
        last.update(t=now, calls=calls)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps({"done": done, "total": total, "calls": calls, "cost": cost}))
        os.replace(tmp, path)
    return write


class Progress:
    """A one-line progress bar over every run of a command."""

    POLL_S = 2.0
    LINE_S = 60.0
    live = None

    def __init__(self, total_days, n_runs, label):
        self.total, self.n_runs = total_days, n_runs
        self.dir = C.WORK / "progress" / f"{label}_{os.getpid()}"
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        os.environ["AC_PROGRESS_DIR"] = str(self.dir)
        self.tty = sys.stdout.isatty()
        self.t0 = time.time()
        self._stop = threading.Event()
        self._last_line = 0.0
        self._drawn = False
        self.th = threading.Thread(target=self._loop, daemon=True)

    def __enter__(self):
        Progress.live = self
        self.th.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self.th.join()
        self.draw(final=True)
        Progress.live = None
        os.environ.pop("AC_PROGRESS_DIR", None)
        shutil.rmtree(self.dir, ignore_errors=True)

    def state(self):
        done = calls = finished = 0
        cost = 0.0
        for p in self.dir.glob("*.json"):
            try:
                x = json.loads(p.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            done += x["done"]
            calls += x["calls"]
            cost += x["cost"]
            finished += x["done"] >= x["total"]
        return done, calls, cost, finished

    def line(self):
        done, calls, cost, finished = self.state()
        f = done / self.total if self.total else 1.0
        el = time.time() - self.t0
        eta = el * (1 - f) / f if f > 0 else None
        bar = "#" * int(30 * f) + "-" * (30 - int(30 * f))
        return (f"[{bar}] {100 * f:5.1f}%  days {done:,}/{self.total:,}  calls {calls:,}  ${cost:,.2f}  "
                f"runs {finished}/{self.n_runs}  elapsed {_dur(el)}  ETA {_dur(eta) if eta is not None else '?'}")

    def clear(self):
        if self.tty and self._drawn:
            sys.stdout.write("\r\033[K")
            sys.stdout.flush()
            self._drawn = False

    def draw(self, final=False):
        txt = self.line()
        if self.tty:
            sys.stdout.write("\r\033[K" + txt + ("\n" if final else ""))
            sys.stdout.flush()
            self._drawn = not final
        elif final or time.time() - self._last_line >= self.LINE_S:
            print(f"{C.now_iso()} progress {txt}", flush=True)
            self._last_line = time.time()

    def _loop(self):
        while not self._stop.wait(self.POLL_S):
            self.draw()


def _dur(s):
    s = int(s)
    return f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"


def _init_worker(label, model, live, target, rule="stated"):
    import decide as D
    client = M.make_client(model, label, live=live, target=target)
    _W.update(client=client, tokens=C.Tokens(), assets=C.CorpusAssets(), panel=C.PanelDaily(), D=D,
              model=model, rule=rule, label=label, mandate=os.environ.get("AUTOCURATOR_MANDATE", "default"))


def _corpus():
    if "corpus" not in _W:
        import tools as T
        _W["corpus"] = T.Corpus()
    return _W["corpus"]


def _gc(group_key):
    D = _W["D"]
    return D.GroupContext(C.group_by_key(group_key), tokens=_W["tokens"], assets=_W["assets"], panel=_W["panel"])


def job_observe(group_key):
    """Build and write one group's observation stream."""
    import observe as O
    g = C.group_by_key(group_key)
    t = time.time()
    if "obs_corpus" not in _W:
        _W.update(obs_corpus=O.Corpus(), pegs=O.PegTable())
    inp = O.Inputs(g, tokens=_W["tokens"], assets=_W["assets"], panel=_W["panel"], corpus=_W["obs_corpus"],
                   pegs=_W["pegs"])
    recs = O.build_group(g, inp)
    meta = O.write_group(g, recs, inp)
    meta["seconds"] = round(time.time() - t, 1)
    return meta


def make_policy(kind, tools=True, schedule="triggered", trigger_set="severe"):
    import ablate as A
    import baselines as B
    D = _W["D"]
    if kind == "autocurator":
        return D.LLMPolicy(_W["client"], _corpus(), _W["label"], tools=tools,
                           name="autocurator" if tools else "no_docs", schedule=schedule, triggers=trigger_set,
                           mandate=_W.get("mandate", "default"))
    if kind in B.POLICIES:
        return B.POLICIES[kind]()
    if kind == "no_llm":
        return A.NoLLM()
    if kind == "cheap_nlp":
        return A.CheapNLP(_corpus())
    raise ValueError(kind)


def write_frame(gc, run, label, policy_name, rep):
    """E4's daily table of a run (e4span.frame), next to the run; returns its notes."""
    import e4span as S
    D = _W["D"]
    nan = float("nan")
    markets = {m: (x["pos"], [c if c is not None else nan for c in x["cfi"]],
                   [u if u is not None else nan for u in x["u"]],
                   [q if q is not None else nan for q in x["liq"]]) for m, x in run["markets"].items()}
    upr = [x if x is not None else nan for x in run["usd_per_unit"]]
    df, notes = S.frame(gc.g.protocol, gc.g.chain_id, run["days"], run["idle"], markets, upr,
                        key=f"autocurator:{label}:{gc.g.key}", loan_symbol=gc.g.loan_symbol, loan_unit=gc.g.loan_unit)
    df["day"] = run["days"]
    path = D.frame_path(label, gc.g, policy_name, rep)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with gzip.open(tmp, "wt", compresslevel=6) as fh:
        df.to_csv(fh, index=False)
    tmp.replace(path)
    return notes


def priced_label(source, pricing):
    """Label of a source run's decisions under another replay pricing."""
    base, at, span = source.partition("@")
    return f"{base}-{pricing}{at}{span}"


def job_reprice(group_key, sources, shortfall):
    """Replay each source label's recorded decisions on one group under the other replay pricing (decide.ReplayPolicy)."""
    import replay as R
    D = _W["D"]
    gc = _gc(group_key)
    out = []
    try:
        for source in sources:
            for p in sorted((C.OUT / "runs" / source).glob(f"{group_key}__*.json.gz")):
                if p.name.endswith(".e4.csv.gz"):
                    continue
                meta = D.load_run(p)["meta"]
                decs = D.load_decisions(source, gc.g, meta["repeat"])
                pol = D.IntentReplayPolicy(decs, meta["policy"])
                pricing = "impact" if meta.get("pricing", "impact") == "displace" else "displace"
                label = priced_label(source, pricing)
                run = D.run_group(gc, pol, meta["first_day"], meta["last_day"], meta["capital_usd"], label,
                                  repeat=meta["repeat"], capital_mult=meta.get("capital_multiplier", 1.0),
                                  shortfall_times=shortfall, pricing=pricing, decisions_out=False,
                                  call_times={int(x["ts"]) for x in decs})
                write_frame(gc, run, label, pol.name, meta["repeat"])
                out.append({"group": group_key, "label": label, "decisions": run["meta"]["n_decisions"],
                            "rebalances": run["meta"]["n_rebalances"], "clips": run["meta"]["clips"]})
    finally:
        R.clear_row_cache()
    return out


def job_reaccount(group_key, sources, shortfall):
    """Replay each source label's recorded answers on one group as meant (decide.IntentReplayPolicy), same pricing, capital, sp."""
    import replay as R
    D = _W["D"]
    gc = _gc(group_key)
    out = []
    try:
        for source in sources:
            for p in sorted((C.OUT / "runs" / source).glob(f"{group_key}__*.json.gz")):
                if p.name.endswith(".e4.csv.gz"):
                    continue
                meta = D.load_run(p)["meta"]
                decs = D.load_decisions(source, gc.g, meta["repeat"])
                if not any(x.get("weights_source") == "previous" and "more than 1" in " ".join(map(str, x.get("problems") or []))
                           for x in decs):
                    continue
                pol = D.IntentReplayPolicy(decs, meta["policy"])
                run = D.run_group(gc, pol, meta["first_day"], meta["last_day"], meta["capital_usd"], source,
                                  repeat=meta["repeat"], capital_mult=meta.get("capital_multiplier", 1.0),
                                  shortfall_times=shortfall, pricing=meta.get("pricing", "displace"),
                                  decisions_out=False, call_times={int(x["ts"]) for x in decs},
                                  write=False)
                run["meta"].update({k: meta[k] for k in ("model", "label", "mandate", "cost_usd", "n_calls",
                                                         "n_requests_reused", "tokens_in", "tokens_out",
                                                         "cache_read", "n_reads", "n_tool_rounds", "eligibility",
                                                         "leakage_diagnostic", "schedule", "trigger_set",
                                                         "triggers") if k in meta})
                run["meta"]["intent_reaccounted"] = pol.interpreted
                D.write_run(run, source, gc.g, pol.name, meta["repeat"])
                write_frame(gc, run, source, pol.name, meta["repeat"])
                out.append({"group": group_key, "label": source, "interpreted": pol.interpreted,
                            "decisions": run["meta"]["n_decisions"], "rebalances": run["meta"]["n_rebalances"]})
    finally:
        R.clear_row_cache()
    return out


def job_group(group_key, specs, shortfall):
    """Run every policy spec on one group; write each run and its E4 table."""
    import replay as R
    D = _W["D"]
    gc = _gc(group_key)
    out = []
    try:
        for kind, label, rep, start, capinfo, mult, tools, leak, schedule, trig in specs:
            cap_usd, aum = capinfo
            w = gc.g.window(start)
            if w is None:
                continue
            if cap_usd is None:
                out.append({"group": group_key, "label": label, "skipped": "no capital"})
                continue
            pol = make_policy(kind, tools, schedule, trig)
            t = time.time()
            try:
                run = D.run_group(gc, pol, w[0], w[1], cap_usd * mult, label, repeat=rep, capital_mult=mult,
                                  model_id=_W["model"] if kind == "autocurator" else "stub", rule=_W["rule"],
                                  allow_leakage=leak, shortfall_times=shortfall, schedule=schedule,
                                  trigger_set=trig, progress=progress_writer(group_key, label, rep),
                                  aum_usd={d: v * mult for d, v in aum.items()} if aum else None)
            except D.Unpriced as e:
                out.append({"group": group_key, "label": label, "skipped": str(e)})
                continue
            t_run = time.time() - t
            notes = write_frame(gc, run, label, pol.name, rep)
            m = run["meta"]
            out.append({"group": group_key, "label": label, "days": len(run["days"]) - 1,
                        "decisions": m["n_decisions"], "tool_rounds": m["n_tool_rounds"],
                        "rebalances": m["n_rebalances"], "clips": m["clips"],
                        "kept_previous": m["n_kept_previous"], "markets": m["markets_loaded"],
                        "rows": m["rows_replayed"], "tokens_in": m["tokens_in"], "seconds_run": round(t_run, 1),
                        "seconds_frame": round(time.time() - t - t_run, 1),
                        "outside_e4": len(notes["markets_outside_e4"])})
    finally:
        R.clear_row_cache()
    return out


def run_jobs(fn, jobs, init_args, workers, quiet=False):
    out = []
    if workers <= 1:
        _init_worker(*init_args)
        for j in jobs:
            out.append(fn(*j))
            if not quiet:
                log(f"done {fn.__name__} {str(j[0])[:60]} -> {_brief(out[-1])}")
        return out
    with ProcessPoolExecutor(workers, initializer=_init_worker, initargs=init_args) as ex:
        futs = [ex.submit(fn, *j) for j in jobs]
        for j, f in zip(jobs, futs):
            out.append(f.result())
            if not quiet:
                log(f"done {fn.__name__} {str(j[0])[:60]} -> {_brief(out[-1])}")
    return out


def _brief(r):
    if isinstance(r, list):
        return "; ".join(f"{x['label']}: " + (x["skipped"] if "skipped" in x else
                                              f"{x['decisions']} dec, {x['rebalances']} reb"
                                              + (f", {x['seconds_run']}+{x['seconds_frame']} s" if "seconds_run" in x
                                                 else "")) for x in r)
    if "records" in r:
        return f"{r['records']} records, {r['days_with_event']} with an event, {r['seconds']} s"
    return r


def select_groups(keys=None):
    gs = C.bench_groups()
    if keys:
        gs = [g for g in gs if g.key in set(keys)]
    return sorted(gs, key=lambda g: -(len(g.humans) * (g.last_day - g.first_day + 1)))


def e4_values():
    """{(protocol, chain, vault)."""
    import pandas as pd
    df = pd.read_csv(C.INPUTS / "metrics/e4/vault_daily.csv", usecols=["protocol", "chain_id", "vault", "date",
                                                                         "value_usd", "active"],
                     dtype={"protocol": str, "vault": str, "date": str})
    dmap = {s: C.e2.day_of(s) for s in df["date"].unique()}
    out = defaultdict(dict)
    for p, c, v, d, x, a in zip(df["protocol"], df["chain_id"], df["vault"].str.lower(), df["date"], df["value_usd"],
                                df["active"]):
        if x == x and a == 1:
            out[(p, int(c), v)][dmap[d]] = float(x)
    return out


def stub_start_day(arg):
    return C.e2.day_of(arg) if arg else M.earliest_cutoff_first_day()


def model_start(model, args):
    """Span start of a live model."""
    d = C.span_start_day(model, args.cutoff_rule)
    if args.span_start:
        s = C.e2.day_of(args.span_start)
        if s < d:
            sys.exit(f"--span-start {args.span_start} is before {model}'s span start {C.day_iso(d)}")
        d = s
    return d


OUT_TOKENS_BASE = 60
OUT_TOKENS_PER_OP = 45
OPS_ASSUMED = 6
OUT_TOKENS_TOOL = 40
LIST_TOKENS = 1200
LAT_BASE_S = 3.0
LAT_PER_KIN_S = 0.313
LAT_PER_OUT_S = 0.0497
SCENARIOS = ("no_tools", "stub_tools", "max_tools")
SCHED_LABELS = {"severe": "stub", "all": "stub-all", "daily": "stub-daily"}
LAYERS = ("system",) + ("mechanics", "facts", "tail")


def dry_run_calls(label="stub"):
    """{group: [(ts, markets in the table, {layer."""
    out = {}
    for p in sorted((C.OUT / "decisions" / label).glob("*__r1.jsonl.gz")):
        rows = []
        with gzip.open(p, "rt") as fh:
            for line in fh:
                d = json.loads(line)
                if d.get("layers"):
                    rows.append((int(d["ts"]), len(d.get("table") or []), d["layers"], d["prefix"],
                                 d.get("tool_rounds") or []))
        out[p.name.split("__r1")[0]] = rows
    return out


def dry_run_schedule(label="stub"):
    """{group: [(day, called)]} from the dry run's run files (the schedule)."""
    out = {}
    for p in sorted((C.OUT / "runs" / label).glob("*__r1.json.gz")):
        with gzip.open(p, "rt") as fh:
            run = json.load(fh)
        out[run["meta"]["group"]] = [(int(d), int(c)) for d, c, _t in run.get("schedule") or []]
    return out


def tool_extras(rounds, scenario):
    """Tokens each tool round appends (the answer that asked plus the results)."""
    import decide as D
    import tools as T
    if scenario == "no_tools":
        return []
    if scenario == "max_tools":
        return [OUT_TOKENS_TOOL + LIST_TOKENS, OUT_TOKENS_TOOL * 2 + D.READ_MAX * T.DOC_TOKENS]
    return [OUT_TOKENS_TOOL + r["result_tokens"] for r in rounds]


def call_requests(rows, rule, scenario, think=0):
    """Requests of one run's calls in order under a vendor's caching rule (models.cache_rule)."""
    out, prev = [], None
    kind, mn, step = rule["kind"], rule["min_tokens"], rule["step"]
    for ts, n, L, pref, rounds in rows:
        cum = [L["system"] + L["mechanics"], L["system"] + L["mechanics"] + L["facts"]]
        total = cum[1] + L["tail"]
        out_dec = OUT_TOKENS_BASE + OUT_TOKENS_PER_OP * min(OPS_ASSUMED, n) + think
        extras = tool_extras(rounds, scenario)
        hit = 0
        if kind is not None and prev is not None:
            for k in range(2):
                if pref[k] == prev[k] and cum[k] >= mn:
                    hit = cum[k]
        if kind == "prefix":
            hit = hit // step * step
        write = max(total - hit, 0) if kind == "explicit" and total >= mn else 0
        reqs = [(total, hit, write, OUT_TOKENS_TOOL + think if extras else out_dec)]
        size = total
        for i, x in enumerate(extras):
            rd = size if kind == "explicit" and size >= mn else (size // step * step if kind == "prefix" and size >= mn
                                                                 else 0)
            size += x
            reqs.append((size, rd, x if kind == "explicit" else 0,
                         OUT_TOKENS_TOOL + think if i + 1 < len(extras) else out_dec))
        out.append((ts, reqs))
        prev = pref
    return out


def seconds(reqs):
    return sum(LAT_BASE_S + LAT_PER_KIN_S * r[0] / 1000 + LAT_PER_OUT_S * r[3] for r in reqs)


def estimate_rows(models, groups=None, calls=None, scheds=None):
    """Calls, tokens, cost and wall time of one live run per model, trigger set and tool scenario."""
    groups = groups or C.bench_groups()
    calls = calls if calls is not None else {k: dry_run_calls(v) for k, v in SCHED_LABELS.items()}
    scheds = scheds if scheds is not None else {k: dry_run_schedule(v) for k, v in SCHED_LABELS.items()}
    rows = []
    for mrow in models:
        if not mrow.get("cutoff"):
            continue
        d0 = M.first_day(mrow["cutoff"])
        ws = {g.key: g.window(d0) for g in groups}
        n_groups = sum(1 for w in ws.values() if w)
        n_days = sum(w[1] - w[0] + 1 for w in ws.values() if w)
        spec = M.MODELS.get(mrow.get("key") or "", {})
        think = M.THINKING_TOKENS if spec.get("thinking") else 0
        pin, pout = mrow.get("price_in"), mrow.get("price_out")
        rule = M.cache_rule(mrow.get("key") or mrow["id"])
        for sched in SCHED_LABELS:
            if not calls.get(sched):
                continue
            for scen in SCENARIOS:
                per, by_group, days_priced = [], {}, 0
                for gk, rws in calls[sched].items():
                    w = ws.get(gk)
                    if not w:
                        continue
                    days_priced += sum(1 for d, _c in scheds[sched].get(gk, []) if w[0] <= d <= w[1])
                    sel = [x for x in call_requests(rws, rule, scen, think) if w[0] * C.DAY <= x[0] <= w[1] * C.DAY]
                    per += sel
                    by_group[gk] = (len(sel), sum(seconds(r) for _t, r in sel))
                if not per or not days_priced:
                    continue
                scale = n_days / days_priced
                reqs = [q for _t, r in per for q in r]
                n_calls = len(per) * scale
                t_in = sum(q[0] for q in reqs) * scale
                c_rd = sum(q[1] for q in reqs) * scale
                c_wr = sum(q[2] for q in reqs) * scale
                t_out = sum(q[3] for q in reqs) * scale
                usd = ((t_in - c_rd - c_wr) * pin + c_rd * pin * rule["read"] + c_wr * pin * rule["write"]
                       + t_out * pout) / 1e6 if pin is not None else None
                nocache = (t_in * pin + t_out * pout) / 1e6 if pin is not None else None
                slow = max(by_group, key=lambda k: by_group[k][1])
                rows.append({
                    "model": mrow["id"], "pipeline_id": mrow.get("key") or "", "cutoff": mrow["cutoff"],
                    "span_start": C.day_iso(d0), "triggers": sched, "tools": scen, "groups": n_groups,
                    "days": n_days, "days_priced": days_priced, "calls": round(n_calls), "calls_priced": len(per),
                    "requests": round(len(reqs) * scale), "call_rate": len(per) / days_priced,
                    "input_tokens_per_call": t_in / n_calls, "cached_tokens_per_call": c_rd / n_calls,
                    "cache_hit_share": c_rd / t_in if t_in else None,
                    "cache_write_tokens_per_call": c_wr / n_calls,
                    "uncached_tokens_per_call": (t_in - c_rd - c_wr) / n_calls,
                    "output_tokens_per_call": t_out / n_calls,
                    "input_tokens": round(t_in), "cached_tokens": round(c_rd), "output_tokens": round(t_out),
                    "max_request_tokens": max(q[0] for q in reqs), "context": mrow.get("context"),
                    "requests_over_context": sum(1 for q in reqs if mrow.get("context") and q[0] > mrow["context"]),
                    "usd": usd, "usd_no_cache": nocache,
                    "hours_parallel": by_group[slow][1] / 3600, "slowest_group": slow,
                    "slowest_group_calls": by_group[slow][0], "hours_sequential": sum(x[1] for x in by_group.values())
                    / 3600 * scale,
                    "price_in": pin, "price_out": pout, "cache_vendor": rule["vendor"], "cache_read_x": rule["read"],
                    "cache_write_x": rule["write"], "cache_min_tokens": rule["min_tokens"],
                    "status": mrow.get("status", ""),
                    "source": "stub dry run: prompts and schedule" + ("" if scale == 1 else
                                                                      f" ({days_priced} of {n_days} group-days priced)")})
    return rows


def layer_stats(label="stub"):
    """Median, mean and 90th percentile of tokens per call by prompt layer over the dry run's calls."""
    import statistics as st
    calls = [x for rws in dry_run_calls(label).values() for x in rws]
    out = {}
    if not calls:
        return out
    for k in LAYERS + ("total", "table"):
        if k == "total":
            xs = [sum(L[j] for j in LAYERS) for _t, _n, L, _p, _r in calls]
        elif k == "table":
            xs = [n for _t, n, _L, _p, _r in calls]
        else:
            xs = [L[k] for _t, _n, L, _p, _r in calls]
        xs.sort()
        out[k] = {"median": st.median(xs), "mean": round(st.fmean(xs), 1), "p90": xs[int(0.9 * (len(xs) - 1))],
                  "max": xs[-1]}
    out["calls"] = len(calls)
    out["calls_with_tools"] = sum(1 for c in calls if c[4])
    return out


def call_rates(label="stub", starts=("2025-10-02", "2026-03-04")):
    """Calls per checked day of the dry run, overall and from each start day on."""
    sch = [x for v in dry_run_schedule(label).values() for x in v]
    out = {"all": {"days": len(sch), "calls": sum(c for _d, c in sch)}}
    for s in starts:
        d0 = C.e2.day_of(s)
        sel = [c for d, c in sch if d >= d0]
        out[s] = {"days": len(sel), "calls": sum(sel)}
    for v in out.values():
        v["rate"] = v["calls"] / v["days"] if v["days"] else None
    return out


def estimate(model=None, log=log, write=True):
    """Print the estimate table (every d6 model, or one) and write it to results/."""
    import evaluate as E
    allm = M.d6_models()
    if model:
        pick = [m for m in allm if model in (m["id"], m.get("key"))]
        if not pick:
            spec = M.MODELS[model]
            pick = [{"id": model, "key": model, "cutoff": M.bound_text(model), "price_in": spec["price_in"],
                     "price_out": spec["price_out"], "context": spec.get("context"), "status": ""}]
        allm = pick
    rows = estimate_rows(allm)
    head = ("model", "span_start", "triggers", "tools", "groups", "calls", "requests", "input_tokens_per_call",
            "cached_tokens_per_call", "output_tokens_per_call", "usd", "hours_parallel", "hours_sequential")
    lines = [" | ".join(head)]
    for r in rows:
        lines.append(" | ".join(_fmt(r[k]) for k in head))
    log("estimate of one live run (prompts and schedule from the stub's dry runs; output tokens assumed; "
        "list prices from d6, cache prices from models.CACHE; time at the DeepSeek V3.2 trial's latency):\n"
        + "\n".join(lines))
    if write and not model:
        E.write_csv(E.RES / "cost_estimate.csv", rows, {
            "out_tokens_base": OUT_TOKENS_BASE, "out_tokens_per_op": OUT_TOKENS_PER_OP, "ops_assumed": OPS_ASSUMED,
            "out_tokens_tool": OUT_TOKENS_TOOL, "list_tokens": LIST_TOKENS, "thinking_tokens": M.THINKING_TOKENS,
            "latency_s": {"base": LAT_BASE_S, "per_1k_in": LAT_PER_KIN_S, "per_out": LAT_PER_OUT_S},
            "prices": "docs/d6-llm-selection.md section 1; cache multiples and minimums: models.CACHE (assumptions)",
            "layers": {k: layer_stats(v) for k, v in SCHED_LABELS.items()},
            "call_rates": {k: call_rates(v) for k, v in SCHED_LABELS.items()}})
    return rows


def _fmt(x):
    if x is None:
        return "n/a"
    if isinstance(x, float):
        return f"{x:,.0f}" if abs(x) >= 100 else f"{x:,.2f}"
    if isinstance(x, int):
        return f"{x:,}"
    return str(x)


def confirm_live(args, total):
    print(f"\nLIVE RUN of {args.model}, estimated at most ${total:,.2f} (see the table above).")
    typed = input(f"Type the model id ({args.model}) to send requests, anything else to stop: ").strip()
    if typed != args.model:
        sys.exit("stopped, nothing was sent")


def main():
    ap = argparse.ArgumentParser(description="AutoCurator pipeline")
    ap.add_argument("cmd", choices=["observe", "decide", "baselines", "reprice", "reaccount", "evaluate", "ablate",
                                    "estimate", "all"])
    ap.add_argument("--source", nargs="*", default=None)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True)
    mode.add_argument("--live", action="store_true")
    ap.add_argument("--model", default=None)
    ap.add_argument("--span-start", default=None)
    ap.add_argument("--groups", nargs="*")
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--capital-multiplier", type=float, default=1.0)
    ap.add_argument("--capital", choices=("fixed", "follow"), default="fixed")
    ap.add_argument("--workers", type=int, default=None)
    ap.add_argument("--leakage-diagnostic", action="store_true")
    ap.add_argument("--variants", nargs="*", default=None, choices=["no_llm", "cheap_nlp", "no_docs", "daily"],
                    help="ablate only these E9 variants (and skip the evaluation and Table 7)")
    ap.add_argument("--cutoff-rule", default="stated", choices=["stated", "weight_release"])
    ap.add_argument("--schedule", default="triggered", choices=["triggered", "daily"])
    ap.add_argument("--triggers", default="severe", choices=["severe", "all"])
    ap.add_argument("--label", default=None)
    ap.add_argument("--mandate", default="default", choices=["default", "aggressive"],
                    help="the vault's mandate in the system prompt (decide.MANDATE_TEXT); "
                         "a non-default mandate labels the model's runs <label>-<mandate>")
    ap.add_argument("--common-window", action="store_true",
                    help="baselines and model-free ablations start on the common window (label @common)")
    ap.add_argument("--resume", action="store_true",
                    help="answer requests already archived under the label (same request and repeat) from the archive")
    args = ap.parse_args()
    if args.resume:
        os.environ["AUTOCURATOR_RESUME"] = "1"
    os.environ["AUTOCURATOR_MANDATE"] = args.mandate
    live = bool(args.live)
    model = args.model or ("stub" if not live else None)
    if live and model in (None, "stub"):
        sys.exit("--live needs --model with a real model id")
    if args.cmd == "all" and live:
        sys.exit("'all' runs only in a dry run")
    if args.label and live:
        sys.exit("--label is for dry runs; a live run is labelled by its model id")
    label = model if live else (args.label or "stub")
    if args.mandate != "default":
        label = f"{label}-{args.mandate}"
    target = model if model != "stub" else None
    csuffix = ("" if args.capital_multiplier == 1.0 else f"-x{args.capital_multiplier:g}") + \
        ("-follow" if args.capital == "follow" else "")
    tsuffix = "-all" if args.triggers == "all" else ""
    suffix = ("-daily" if args.schedule == "daily" else "") + tsuffix + csuffix
    sched, trig = args.schedule, args.triggers
    stub_start = stub_start_day(args.span_start if not live else None)
    start = model_start(model, args) if live else stub_start
    span_name = model if live else (args.model if args.model not in (None, "stub") else "stub")
    bstart = start if live else (model_start(span_name, args) if span_name != "stub" else stub_start)
    if args.common_window:
        span_name, bstart = "common", C.COMMON_START
    bsuf = f"@{span_name}"
    groups = select_groups(args.groups)
    workers = args.workers or (len(groups) if live else 4)
    t_start = time.time()
    log(f"{args.cmd} {'LIVE' if live else 'dry run'} model={model} label={label}{suffix} span from "
        f"{C.day_iso(start)} schedule={sched} triggers={trig} repeats={args.repeats} workers={workers} "
        f"inputs={C.INPUTS}")

    if args.cmd == "estimate":
        estimate(args.model if args.model not in (None, "stub") else None)
        return
    if live and args.cmd in ("decide", "ablate"):
        key = "daily" if sched == "daily" else trig
        rows = [r for r in estimate(model, write=False) if r["triggers"] == key and r["tools"] == "max_tools"]
        confirm_live(args, (rows[0]["usd"] or 0.0) * args.repeats if rows else 0.0)
    init = (label, model, live, target, args.cutoff_rule)
    if args.cmd in ("observe", "all"):
        import observe as O
        t = time.time()
        metas = run_jobs(job_observe, [(g.key,) for g in groups], init, workers)
        idx = O.write_index()
        log(f"observation streams: {len(metas)} built; index {idx['groups']} groups, {idx['records']:,} records, "
            f"{idx['days_with_event']:,} with an event; absent inputs: {len(idx['inputs_absent'])} "
            f"({time.time() - t:,.0f} s)")
        if args.cmd == "observe":
            return

    if args.cmd == "reaccount":
        import evaluate as E
        if not args.source:
            sys.exit("reaccount needs --source LABEL ...")
        t = time.time()
        res = run_jobs(job_reaccount, [(g.key, args.source, E.shortfall_times()) for g in groups], init,
                       min(workers, len(groups)) or 1)
        flat = [x for r in res for x in r]
        log(f"reaccount: {len(flat)} runs, {sum(x['interpreted'] for x in flat)} answers read as meant "
            f"({time.time() - t:,.0f} s)")
        return

    if args.cmd == "reprice":
        import evaluate as E
        if not args.source:
            sys.exit("reprice needs --source LABEL ...")
        t = time.time()
        res = run_jobs(job_reprice, [(g.key, args.source, E.shortfall_times()) for g in groups], init,
                       min(workers, len(groups)) or 1)
        flat = [x for r in res for x in r]
        log(f"reprice: {len(flat)} runs repriced over {len(groups)} groups ({time.time() - t:,.0f} s)")
        return

    specs = defaultdict(list)
    if args.cmd in ("decide", "baselines", "ablate", "all"):
        import decide as D
        import observe as O
        vals = e4_values()
        cap, fixed = {}, {}

        def capital(g, s):
            key = (g.key, s)
            if key not in cap:
                w = g.window(s)
                if not w:
                    cap[key] = (None, None)
                else:
                    if g.key not in fixed:
                        fixed[g.key] = D.capital_usd(g, vals, O.load_records(g.key))[0]
                    cap[key] = (fixed[g.key], D.aum_path(g, w[0], w[1], vals) if args.capital == "follow" else None)
            return cap[key]
        for g in groups:
            if args.cmd in ("decide", "all"):
                for rep in range(1, args.repeats + 1):
                    specs[g.key].append(("autocurator", label + suffix, rep, start, capital(g, start),
                                         args.capital_multiplier, True, False, sched, trig))
            if args.cmd in ("baselines", "all"):
                for b in ("max_apy", "equal", "rule_based"):
                    specs[g.key].append((b, f"baseline-{b}{suffix}{bsuf}", 1, bstart, capital(g, bstart),
                                         args.capital_multiplier, True, False, sched, trig))
            if args.cmd in ("ablate", "all"):
                want = set(args.variants or ("no_llm", "cheap_nlp", "no_docs", "daily"))
                for v in ("no_llm", "cheap_nlp"):
                    if v in want:
                        specs[g.key].append((v, f"ablate-{v}{suffix}{bsuf}", 1, bstart, capital(g, bstart),
                                             args.capital_multiplier, True, False, sched, trig))
                if "no_docs" in want:
                    specs[g.key].append(("autocurator", f"{label}-ablate-no_docs{suffix}", 1, start,
                                         capital(g, start), args.capital_multiplier, False, False, sched, trig))
                if sched == "triggered" and "daily" in want:
                    specs[g.key].append(("autocurator", f"{label}-ablate-daily{tsuffix}{csuffix}", 1, start,
                                         capital(g, start), args.capital_multiplier, True, False, "daily", trig))
                if args.leakage_diagnostic and live:
                    specs[g.key].append(("autocurator", f"{label}-ablate-leakage{suffix}", 1, None,
                                         capital(g, None), args.capital_multiplier, True, True, sched, trig))
        import evaluate as E
        shortfall = E.shortfall_times()
        jobs = [(g.key, specs[g.key], shortfall) for g in groups if specs[g.key]]
        spans = [w for g in groups for x in specs[g.key] if x[4][0] is not None and (w := g.window(x[3]))]
        t = time.time()
        with Progress(sum(b - a + 1 for a, b in spans), len(spans), label + suffix):
            res = run_jobs(job_group, jobs, init, min(workers, len(jobs)) or 1)
        flat = [x for r in res for x in r]
        log(f"runs: {sum(1 for x in flat if 'skipped' not in x)} runs, "
            f"{sum(x.get('decisions', 0) for x in flat):,} decisions, "
            f"{sum(x.get('rebalances', 0) for x in flat):,} rebalances, {sum(1 for x in flat if 'skipped' in x)} "
            f"skipped, over {len(jobs)} groups ({time.time() - t:,.0f} s)")
        C.write_json(C.OUT / "results" / f"run_summary_{args.cmd}{suffix}.json", flat)

    if args.variants is not None:
        return
    if args.cmd in ("evaluate", "all", "ablate"):
        import evaluate as E
        models = [model] if live and model not in M.PROPOSED else None
        E.evaluate(stub_start, models=(M.PROPOSED + models) if models else None, log=log, rule=args.cutoff_rule)
    if args.cmd in ("ablate", "all", "evaluate"):
        import ablate as A
        abl_model, _d = A.ablation_model(args.cutoff_rule)
        variants = [f"ablate-no_llm{suffix}{bsuf}", f"ablate-cheap_nlp{suffix}{bsuf}", f"{label}-ablate-no_docs{suffix}"]
        if sched == "triggered":
            variants.append(f"{label}-ablate-daily{tsuffix}{csuffix}")
        if args.leakage_diagnostic and live:
            variants.append(f"{label}-ablate-leakage{suffix}")
        span = "stub" if not live else model
        A.table7(label + suffix, variants, span, model if live else abl_model,
                 other_labels=[f"baseline-{b}{suffix}{bsuf}" for b in ("max_apy", "equal", "rule_based")])
        log(f"table7 written (span {span}, priced as {model if live else abl_model})")
    if args.cmd == "all":
        estimate(None)
    log(f"{args.cmd} finished in {time.time() - t_start:,.0f} s")


if __name__ == "__main__":
    main()
