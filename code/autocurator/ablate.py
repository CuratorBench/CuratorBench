# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31", "pandas>=2.2", "numpy>=1.26", "pycryptodome", "matplotlib>=3.8"]
# ///
"""E9 ablations and cost."""
import bisect
import csv
import gzip
import json
import re
from collections import defaultdict

import baselines as B
import context as C
import decide as D
import evaluate as E
import models as M
import tools as T

NEGATIVE = re.compile(r"\b(depeg\w*|exploit\w*|hack\w*|insolven\w*|bankrupt\w*|frozen|freez\w*|paus\w*|"
                      r"halt\w*|suspend\w*|delist\w*|lawsuit\w*|fraud\w*|bad debt|shortfall\w*|"
                      r"collaps\w*|rug\w*|unbacked|undercollateral\w*|impair\w*|haircut\w*|"
                      r"withdrawals? (?:are )?(?:disabled|closed|limited))\b", re.I)
POSITIVE = re.compile(r"\b(audit\w*|proof of reserves?|attestation\w*|fully backed|overcollateral\w*|"
                      r"insur\w*|transparen\w*)\b", re.I)
NATIVE = {1: (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
          42161: (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
          8453: (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
          59144: (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
          56: (56, "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"),
          43114: (43114, "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"),
          146: (146, "0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38"),
          9745: (9745, "0x6100e367285b01f48d07953803a2d8dca5d19873"),
          999: (999, "0x5555555555555555555555555555555555555555"),
          747474: (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")}


def ablation_model(rule="stated"):
    """The proposed model whose span starts first (ties."""
    best, d_best = None, None
    for m in M.PROPOSED:
        d = C.span_start_day(m, rule)
        if d_best is None or d < d_best:
            best, d_best = m, d
    return best, d_best


def rate_max_weights(nav, obs, markets, apy_after, steps=100):
    """Weights that maximize the vault's yield at current rates."""
    if nav <= 0 or not markets:
        return {}
    chunk = nav / steps

    def total(m, amt):
        return amt * apy_after(m, amt - obs[m]["position"])
    x = {m: 0.0 for m in markets}
    got = {m: 0.0 for m in markets}
    gain = {m: total(m, chunk) for m in markets}
    for _ in range(steps):
        best = max(gain, key=gain.get)
        if gain[best] <= 0:
            break
        x[best] += chunk
        got[best] += gain[best]
        gain[best] = total(best, x[best] + chunk) - got[best]
    return {m: v / nav for m, v in x.items() if v > 0}


class NoLLM(D.Policy):
    name = "no_llm"

    def decide(self, dc):
        keep = B.rule_filter(dc)
        return rate_max_weights(dc["nav"], dc["obs"], keep, dc["apy_after"])


class CheapNLP(D.Policy):
    """Keyword classifier over the filtered corpus (module docstring)."""

    name = "cheap_nlp"

    def __init__(self, corpus):
        self.corpus = corpus
        self.docs = {}
        self.memo = {}

    def _family(self, fam):
        """[(visible at, published, negative, positive)] of the family's documents the cutoff filter can ever expose, by visibility time."""
        if fam not in self.docs:
            rows = []
            for vis, doc_id in self.corpus.by_asset.get(fam, []):
                d = self.corpus.by_id[doc_id]
                txt = d["title"] + " " + d["text"][:4000]
                rows.append((vis, d["published_ts"], bool(NEGATIVE.search(txt)), bool(POSITIVE.search(txt))))
            rows.sort()
            self.docs[fam] = (rows, [r[0] for r in rows])
        return self.docs[fam]

    def judge(self, fam, ts):
        """(not flagged, counts); documents visible strictly before ts only."""
        key = (fam, ts)
        if key in self.memo:
            return self.memo[key]
        rows, vis = self._family(fam)
        hi = bisect.bisect_left(vis, ts)
        lo = bisect.bisect_left(vis, ts - 90 * C.DAY)
        latest = vis[hi - 1] if hi else None
        if latest is not None and latest >= ts:
            raise AssertionError(f"cheap_nlp read a document visible at {latest} for a decision at {ts}")
        r30 = [r for r in rows[lo:hi] if ts - r[1] <= 30 * C.DAY]
        r90 = [r for r in rows[lo:hi] if ts - r[1] <= 90 * C.DAY]
        neg30 = sum(r[2] for r in r30)
        neg90 = sum(r[2] for r in r90)
        pos90 = sum(r[3] for r in r90)
        bad = neg30 >= 2 or (bool(r90) and neg90 / len(r90) >= 0.3 and pos90 < neg90)
        out = (not bad, {"docs": hi, "neg30": neg30, "neg90": neg90, "pos90": pos90, "latest_visible_ts": latest})
        self.memo[key] = out
        return out

    def decide(self, dc):
        gc, ts = dc["gc"], dc["ts"]
        keep, flagged = [], 0
        for m in B.open_markets(dc):
            ok = True
            for fam, w, *_ in gc.families(m, ts):
                good, _info = self.judge(fam, ts)
                if not good and w >= 0.02:
                    ok = False
            if ok:
                keep.append(m)
            else:
                flagged += 1
        dc["record"]["flagged"] = flagged
        return B.equal_weights(keep)


def native_usd(prices, cid, day):
    k = NATIVE.get(int(cid))
    if not k:
        return None
    return prices.usd(k[0], k[1], day)


def rebalances(run):
    """[(ts, markets moved)] of a run."""
    out = [(int(d["ts"]), int(d["markets_moved"])) for d in run["decisions"] + run.get("retries", [])
           if d.get("rebalance") and d.get("markets_moved")]
    return sorted(out)


def run_cost(run, gm, prices):
    """Per-run aggregates for cost_rows."""
    meta = run["meta"]
    agg = {"days": meta["last_day"] - meta["first_day"] + 1, "decisions": len(run["decisions"]),
           "tokens_in": 0, "tokens_out": 0, "cache_read": 0, "cache_write": 0, "calls": 0, "rebalances": 0,
           "markets": 0, "unpriced": 0,
           "gas_usd": 0.0, "model": meta.get("model"), "label": meta["label"]}
    for d in run["decisions"]:
        agg["tokens_in"] += d.get("tokens_in", 0)
        agg["tokens_out"] += d.get("tokens_out", 0)
        agg["cache_read"] += d.get("cache_read", 0)
        agg["cache_write"] += d.get("cache_write", 0)
        agg["calls"] += d.get("calls", 0)
    for ts, k in rebalances(run):
        agg["rebalances"] += 1
        agg["markets"] += k
        fee = gm.fee_native(meta["protocol"], meta["chain_id"], k, ts)
        px = native_usd(prices, meta["chain_id"], C.day_of_ts(ts))
        if fee is None or px is None:
            agg["unpriced"] += 1
            continue
        agg["gas_usd"] += fee * px
    return agg


def cost_rows(aggs_by_label, target_model):
    """Per label: tokens and dollars per decision, inference and gas per vault-year."""
    spec = M.MODELS[target_model]
    think = M.THINKING_TOKENS if spec.get("thinking") else 0
    out = []
    for label, aggs in sorted(aggs_by_label.items()):
        n_dec = sum(a["decisions"] for a in aggs)
        t_in = sum(a["tokens_in"] for a in aggs)
        t_out = sum(a["tokens_out"] for a in aggs)
        c_rd = sum(a["cache_read"] for a in aggs)
        c_wr = sum(a["cache_write"] for a in aggs)
        calls = sum(a["calls"] for a in aggs)
        n_reb = sum(a["rebalances"] for a in aggs)
        k_sum = sum(a["markets"] for a in aggs)
        unpriced = sum(a["unpriced"] for a in aggs)
        gas_usd = sum(a["gas_usd"] for a in aggs)
        years = sum(a["days"] for a in aggs) / 365.0
        models = {a["model"] for a in aggs}
        row = {"label": label, "priced_as": target_model, "runs": len(aggs), "decisions": n_dec,
               "calls_per_vault_year": n_dec / years if years else None, "requests": calls,
               "vault_years": years, "rebalances": n_reb,
               "rebalances_per_vault_year": n_reb / years if years else None,
               "markets_per_rebalance": k_sum / n_reb if n_reb else None,
               "rebalances_unpriced": unpriced,
               "gas_usd_per_rebalance": gas_usd / (n_reb - unpriced) if n_reb > unpriced else None,
               "gas_usd_per_vault_year": gas_usd / years if years else None}
        if models <= {None}:
            row.update({"tokens_in_per_decision": 0, "cached_tokens_per_decision": 0, "tokens_out_per_decision": 0,
                        "usd_per_decision": 0.0, "inference_usd_per_vault_year": 0.0})
        elif "stub" in models:
            row.update({"tokens_in_per_decision": None, "cached_tokens_per_decision": None,
                        "tokens_out_per_decision": None, "usd_per_decision": None, "inference_usd_per_vault_year": None})
        else:
            dec_cost = M.cost_usd(target_model, t_in, t_out + think * calls, cache_read=c_rd, cache_write=c_wr)
            row.update({"tokens_in_per_decision": t_in / n_dec if n_dec else 0,
                        "cached_tokens_per_decision": c_rd / n_dec if n_dec else 0,
                        "tokens_out_per_decision": t_out / n_dec if n_dec else 0,
                        "usd_per_decision": dec_cost / n_dec if n_dec else 0.0,
                        "inference_usd_per_vault_year": dec_cost / years if years else None})
        out.append(row)
    return out


def label_costs(labels, target_model):
    """cost_rows over every run of the labels (streamed."""
    import gas as G
    gm = G.GasModel()
    prices = C.Prices(C.Tokens(), set(NATIVE.values()))
    aggs = defaultdict(list)
    for lab in labels:
        for p in sorted((C.OUT / "runs" / lab).glob("*.json.gz")):
            aggs[lab].append(run_cost(D.load_run(p), gm, prices))
    return {r["label"]: r for r in cost_rows({k: v for k, v in aggs.items() if v}, target_model)}


T7_COLS = ("span", "cutoff", "span_days", "groups", "groups_compared", "events_in_span", "mean_yield_gap_pp",
           "mean_tail_loss_gap_pp", "median_yield_gap_pp", "median_tail_loss_gap_pp", "mean_pct_yield", "mean_pct_tail_loss", "events_zero_exposure_t0",
           "mean_loss_per_dollar", "mean_pct_loss")


def table7(full_label, variants, span, target_model, other_labels=()):
    """Rows: the full system and each variant, with Table 6's columns on the full system's span and the cost per vault-year."""
    t6 = {}
    if (E.RES / "table6.csv").exists():
        with open(E.RES / "table6.csv") as fh:
            t6 = {(r["span"], r["label"]): r for r in csv.DictReader(fh)}
    labels = [full_label] + [v for v in variants if v != full_label]
    costs = label_costs(labels + [x for x in other_labels if x not in labels], target_model)
    rows = []
    for lab in labels:
        a, c = t6.get((span, lab), {}), costs.get(lab, {})
        inf, gas = c.get("inference_usd_per_vault_year"), c.get("gas_usd_per_vault_year")
        row = {"variant": "full system" if lab == full_label else lab.split("ablate-")[-1], "label": lab}
        row.update({k: a.get(k) for k in T7_COLS})
        row.update({"calls_per_vault_year": c.get("calls_per_vault_year"),
                    "rebalances_per_vault_year": c.get("rebalances_per_vault_year"),
                    "markets_per_rebalance": c.get("markets_per_rebalance"),
                    "gas_usd_per_rebalance": c.get("gas_usd_per_rebalance"),
                    "gas_usd_per_vault_year": gas, "inference_usd_per_vault_year": inf,
                    "cost_usd_per_vault_year": (inf + (gas or 0.0)) if inf is not None else None,
                    "usd_per_decision": c.get("usd_per_decision"),
                    "tokens_in_per_decision": c.get("tokens_in_per_decision"),
                    "cached_tokens_per_decision": c.get("cached_tokens_per_decision"),
                    "decisions": c.get("decisions"), "priced_as": target_model})
        rows.append(row)
    note = ("gas: measured per rebalance (code/autocurator/gas.py, data/autocurator/gas/); token and inference "
            "columns stay empty for stub runs and are filled from the live runs' API usage")
    E.write_csv(E.RES / "table7.csv", rows, {"full_label": full_label, "span": span, "priced_as": target_model,
                                             "note": note})
    E.write_csv(E.RES / "costs.csv", list(costs.values()), {"priced_as": target_model, "note": note})
    return rows


if __name__ == "__main__":
    print(__doc__)
