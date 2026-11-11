# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Table 1, CuratorBench by protocol (E1, E3, E2)."""
from __future__ import annotations

import csv
import hashlib
from collections import Counter, defaultdict
from pathlib import Path

import tabfmt as T

GENERATOR = "code/tables/table2_components.py"
PANEL = T.DATA / "panel"
VAULTS = T.DATA / "vaults"
REG = T.DATA / "registry"
CURATORS = REG / "vault_curators.jsonl"
VAULT_DAILY = T.DATA / "aum/vault_daily.csv"
LENDING_DAILY = T.DATA / "aum/lending_daily.csv"
HEADS = T.DATA / "aum/vault_daily.csv.meta.json"
SUMMARY = T.DATA / "aum/summary.json"
TABLE3 = T.DATA / "events/table3.csv"
COVERAGE = T.DATA / "corpus/coverage.json"

KINDS = {"morpho": [("morpho_v1", "V1"), ("morpho_v2", "V2")],
         "euler_v2": [("euler_earn", "Earn"), ("euler_lending", "Lending vaults")],
         "moolah": [("moolah_vault", None)],
         "silo_v2": [("silo_vault", None)]}
ORACLE_COLS = [("tracks_market", "Tracks"), ("exchange_rate", "Rate"), ("fixed_price", "Fixed"),
               ("min_of_two", "Min"), ("other", "Other"), ("spam", "Spam"), ("unresolved", "Unres.")]
SPAM_SKELETONS = {("morpho", 8453): "dcb6e345ab138f07"}
ORACLE_FOLD = {"none": "other"}
NOTE_WIDTH = 120
SOURCE_LABEL = [("governance_forum", "forum posts"), ("docs", "documentation pages"), ("audit", "audits"),
                ("news", "news items"), ("web_archive", "web-archive captures"),
                ("curator_announcement", "curator announcements"),
                ("issuer_announcement", "issuer announcements"), ("other", "other")]


def rows_in(path: Path) -> int:
    """Data rows of a CSV with a header (newline count minus one)."""
    n, last = 0, b"\n"
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            n += chunk.count(b"\n")
            last = chunk[-1:]
    if last != b"\n":
        n += 1
    return max(n - 1, 0)


def listing_digest(files) -> str:
    h = hashlib.sha256()
    for f in sorted(files):
        h.update(f"{T.rel(f)}\t{f.stat().st_size}\n".encode())
    return h.hexdigest()


def content_digest(files) -> str:
    h = hashlib.sha256()
    for f in sorted(files):
        h.update(f"{T.rel(f)}\t{T.sha256(f)}\n".encode())
    return h.hexdigest()


def pairs():
    return {p: sorted(int(d.name) for d in (PANEL / p).iterdir() if d.is_dir()) for p in T.PROTOCOL_ORDER}


def market_layer(scope, used):
    out = {}
    for p, cids in scope.items():
        n_mk, modes = 0, Counter()
        for cid in cids:
            f = REG / "markets" / p / f"{cid}.jsonl"
            used["registry"].append(f)
            n_mk += sum(1 for r in T.read_jsonl(f) if p != "euler_v2" or r.get("type") == "vault")
            f = REG / "oracles" / p / f"{cid}.jsonl"
            used["registry"].append(f)
            spam_skel, skel = SPAM_SKELETONS.get((p, cid)), {}
            if spam_skel:
                f_code = REG / "oracles" / "_cache" / str(cid) / "code.jsonl"
                used["registry"].append(f_code)
                skel = {c["address"]: c.get("skeleton") for c in T.read_jsonl(f_code)}
            for r in T.read_jsonl(f):
                if r["type"] == "unresolved" and spam_skel and skel.get(r["oracle"]) == spam_skel:
                    continue
                modes[oracle_mode(r["type"], r.get("subtype"))] += 1
        out[p] = {"chains": len(cids), "markets": n_mk, "modes": modes}
    return out


def oracle_mode(otype, subtype):
    """As code/analysis/e4_metrics.py."""
    if otype in ("tracks_market", "min_of_two"):
        return "track"
    if otype == "exchange_rate" and subtype == "underlying_at_own_feed":
        return "track"
    if otype in ("exchange_rate", "fixed_price"):
        return "hard"
    return "worst"


def fixed_share(modes):
    n = sum(modes.values())
    return (modes["hard"] + modes["worst"]) / n if n else None


def vault_layer(scope, used):
    kind_of, enumerated = {}, Counter()
    for p, cids in scope.items():
        for cid in cids:
            f = REG / "vaults" / p / f"{cid}.jsonl"
            used["registry"].append(f)
            for r in T.read_jsonl(f):
                kind_of[(cid, r["vault"].lower())] = r["kind"]
                enumerated[r["kind"]] += 1
    lending = set()
    for cid in scope["euler_v2"]:
        for r in T.read_jsonl(REG / "markets/euler_v2" / f"{cid}.jsonl"):
            if r.get("type") == "vault":
                lending.add((cid, r["vault"].lower()))
    enumerated["euler_lending"] = len(lending)
    funded, first = defaultdict(set), {}
    for path, fixed in ((VAULT_DAILY, None), (LENDING_DAILY, "euler_lending")):
        with open(path, newline="") as fh:
            for r in csv.DictReader(fh):
                usd = T.number(r["aum_usd"])
                if not usd or usd <= 0:
                    continue
                k = fixed or r["kind"]
                funded[k].add((int(r["chain_id"]), r["vault"].lower()))
                if r["date"] < first.get(k, "9999"):
                    first[k] = r["date"]
    cur = defaultdict(set)
    for r in T.read_jsonl(CURATORS):
        key = (int(r["chain_id"]), r["vault"].lower())
        if r["entity_id"] and r["confidence"] != "unattributed" and key in funded.get(r["kind"], ()):
            cur[r["kind"]].add(r["entity_id"])
    return {"funded": {k: len(v) for k, v in funded.items()}, "curators": cur, "first": first,
            "all_curators": set().union(*cur.values())}


def protocol_events():
    n = Counter()
    rows = T.read_csv(TABLE3)
    for r in rows:
        for p in set(r["protocols"].split(";")):
            n[p] += 1
    return n, len(rows)


def main():
    scope = pairs()
    used = {"registry": []}
    mk = market_layer(scope, used)
    vl = vault_layer(scope, used)
    ev, n_events = protocol_events()
    summ = T.read_json(SUMMARY)["protocols"]

    def usd_b(x):
        return f"{x / 1e9:.2f}"

    def kinds(p):
        return [k for k, _ in KINDS[p]]

    body = [r"\begin{tabular}{@{}lrrrrrrr@{}}", r"\toprule",
            T.row(["", "", "", "Fixed-", "Funded", "", "Peak", ""]),
            T.row(["Protocol", "Chains", "Markets", r"price (\%)", "vaults", "Curators", r"AUM (\$B)", "Events"]),
            r"\midrule"]
    for p in T.PROTOCOL_ORDER:
        m = mk[p]
        cur = set().union(*(vl["curators"].get(k, set()) for k in kinds(p)))
        body.append(T.row([T.PROTOCOL_LABEL[p], T.count(m["chains"]), T.count(m["markets"]),
                           f"{100 * fixed_share(m['modes']):.0f}",
                           T.count(sum(vl["funded"].get(k, 0) for k in kinds(p))), T.count(len(cur)),
                           usd_b(summ[p]["peak_usd"]), T.count(ev.get(p, 0))]))
    modes_all = sum((m["modes"] for m in mk.values()), Counter())
    kinds_all = [k for p in T.PROTOCOL_ORDER for k in kinds(p)]
    body += [r"\midrule",
             T.row(["All", T.count(len({c for cids in scope.values() for c in cids})),
                    T.count(sum(m["markets"] for m in mk.values())),
                    f"{100 * fixed_share(modes_all):.0f}",
                    T.count(sum(vl["funded"].get(k, 0) for k in kinds_all)),
                    T.count(len(vl["all_curators"])), usd_b(summ["all"]["peak_usd"]), T.count(n_events)]),
             r"\bottomrule", r"\end{tabular}"]
    sources = [(f"data/registry/{{markets,oracles,vaults}}/ ({len(set(used['registry']))} files, contents)",
                content_digest(set(used["registry"]))),
               CURATORS, VAULT_DAILY, LENDING_DAILY, SUMMARY, TABLE3]
    print(T.rel(T.write("table2_components", GENERATOR, sources, body)))


if __name__ == "__main__":
    main()
