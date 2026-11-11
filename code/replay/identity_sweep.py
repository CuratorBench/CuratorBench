# /// script
# requires-python = ">=3.11"
# dependencies = ["pycryptodome"]
# ///
"""Identity test over ALL curator vaults (paper claim."""

import json
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "data"))
import engine
from ts_cache import TsCache

DATA = HERE.parents[1] / "data"
OUT = DATA / "replay" / "identity-sweep.jsonl"


def estimated_cost(meta_path):
    """Proxy: sum of touched markets' panel file sizes."""
    m = json.load(open(meta_path))
    cid = m["chain_id"]
    total = 0
    for mid in m["final_shares"]:
        p = DATA / "markets" / f"{cid}-{mid}.csv"
        if p.exists():
            total += p.stat().st_size
    return total


def run_vault(key, caches):
    cid, addr = key.split("-", 1)
    cid = int(cid)
    meta = json.load(open(DATA / "trajectories" / f"{key}.meta.json"))
    b0, b1 = meta["creation_block"], meta["head_block"]

    missing = [mid for mid in meta["final_shares"]
               if not (DATA / "markets" / f"{cid}-{mid}.csv").exists()]
    if missing:
        return {"vault": key, "name": meta["name"],
                "skipped": f"{len(missing)} markets not in panel"}
    if meta["n_events"] == 0:
        return {"vault": key, "name": meta["name"], "skipped": "no events"}

    blocks = engine.prepare_timestamp_blocks(cid, addr, b0, b1,
                                             accruals_only=True)
    ts = caches.setdefault(cid, TsCache(cid)).ensure(blocks, quiet=True)

    rp = engine.Replay(cid, addr, b0, b1, ts, convention="recorded")
    failures = []

    def audit(eng, block, li, mid, hist_post):
        m = eng.markets[mid]
        if (m.tsa, m.tss, m.tba, m.tbs) != hist_post and len(failures) < 3:
            failures.append({"block": block, "market": mid[:14]})

    rp.run(engine.IdentityPolicy(), audit_hook=audit)

    last_panel = {}
    for mid in meta["final_shares"]:
        rows = engine.load_market_rows(cid, mid)
        last_panel[mid] = rows[-1][0] if rows else 0
    expected = {mid: 0 for mid in meta["final_shares"]}
    tail_events = 0
    for e in engine.load_vault_events(cid, addr):
        if e[0] <= last_panel.get(e[2], 0):
            expected[e[2]] = expected.get(e[2], 0) + e[5]
        else:
            tail_events += 1
    share_bad = sum(1 for mid, exp in expected.items()
                    if rp.shares.get(mid, 0) != exp)
    n_events = sum(len(v) for v in rp.hist.values())
    return {"vault": key, "name": meta["name"], "events": n_events,
            "markets": meta["n_markets"], "post_panel_events": tail_events,
            "state_mismatches": failures, "share_mismatches": share_bad,
            "pass": not failures and share_bad == 0}


def main():
    done = set()
    if OUT.exists():
        with open(OUT) as f:
            done = {json.loads(line)["vault"] for line in f if line.strip()}

    metas = sorted((DATA / "trajectories").glob("*.meta.json"),
                   key=estimated_cost)
    keys = [p.stem.removesuffix(".meta") for p in metas]
    todo = [k for k in keys if k not in done]
    print(f"{len(keys)} vaults, {len(done)} done, {len(todo)} to run")

    caches = {}
    OUT.parent.mkdir(exist_ok=True)
    npass = nfail = 0
    for i, key in enumerate(todo):
        try:
            r = run_vault(key, caches)
        except Exception as e:
            r = {"vault": key, "error": str(e)[:200]}
        with open(OUT, "a") as f:
            f.write(json.dumps(r) + "\n")
        tag = ("skip" if "skipped" in r else
               "ERR " if "error" in r else
               "PASS" if r["pass"] else "FAIL")
        if tag == "PASS":
            npass += 1
        elif tag == "FAIL" or tag == "ERR ":
            nfail += 1
        print(f"[{i + 1}/{len(todo)}] {tag} {key} "
              f"({r.get('events', '-')} events)")

    print(f"\nSWEEP DONE: {npass} pass, {nfail} fail/err "
          f"(see {OUT})")


if __name__ == "__main__":
    main()
