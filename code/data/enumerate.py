# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""E1, step 1: enumerate every market and every curator vault of the four protocols on every chain in config/deployments.json."""
import argparse
import json
import pathlib
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
from common import io_utils, logs
from protocols import euler, moolah, morpho, silo

CONFIG = HERE.parent / "config"
MODULES = {"morpho": morpho, "euler_v2": euler, "silo_v2": silo,
           "moolah": moolah}
SUMMARY = "registry/summary.json"


def targets(only_proto=None, only_cid=None, kinds=("markets", "vaults")):
    reg = json.loads((CONFIG / "deployments.json").read_text())["protocols"]
    chains = logs.load_chains()
    for proto, mod in MODULES.items():
        if only_proto and proto != only_proto:
            continue
        for cid, entry in reg[proto]["chains"].items():
            if only_cid and cid != only_cid:
                continue
            for kind in kinds:
                yield proto, cid, kind, chains.get(cid), entry, getattr(mod, kind)


def run_one(proto, cid, kind, ccfg, entry, fn, allow_rpc=False, force=False):
    tid = f"{kind}:{proto}:{cid}"
    if ccfg is None:
        return tid, {"status": "no_chain_cfg"}
    if not ccfg.get("hypersync"):
        if not (allow_rpc and logs.rpc_target(ccfg)):
            return tid, {"status": "skipped_no_hypersync",
                         "chain": ccfg.get("name")}
    out = io_utils.DATA / f"registry/{kind}/{proto}/{cid}.jsonl"
    meta = out.with_suffix(".jsonl.meta.json")
    if out.exists() and meta.exists() and not force:
        m = json.loads(meta.read_text())
        return tid, {"status": "ok", "rows": m["rows"], "chain": ccfg["name"],
                     "scanned_to_block": m.get("scanned_to_block"),
                     "cached": True}
    t0 = time.time()
    try:
        rows, upto = fn(ccfg, entry)
        for r in rows:
            r["protocol"], r["chain_id"] = proto, int(cid)
        io_utils.write_jsonl(
            f"registry/{kind}/{proto}/{cid}.jsonl", rows,
            generator="code/data/enumerate.py",
            meta={"protocol": proto, "chain_id": int(cid),
                  "chain": ccfg["name"], "scanned_to_block": upto,
                  "source": "hypersync" if ccfg.get("hypersync") else "rpc",
                  "registry_entry": entry})
        return tid, {"status": "ok", "rows": len(rows), "chain": ccfg["name"],
                     "scanned_to_block": upto,
                     "seconds": round(time.time() - t0, 1)}
    except Exception as exc:
        traceback.print_exc()
        return tid, {"status": "failed", "chain": ccfg.get("name"),
                     "error": str(exc)[:300]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="proto or proto:chain_id")
    ap.add_argument("--kind", choices=["markets", "vaults"])
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--allow-rpc", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    proto_f, _, cid_f = (args.only or "").partition(":")
    kinds = (args.kind,) if args.kind else ("markets", "vaults")

    path = io_utils.DATA / SUMMARY
    summary = json.loads(path.read_text())["targets"] if path.exists() else {}
    jobs = list(targets(proto_f or None, cid_f or None, kinds))
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(run_one, *j, allow_rpc=args.allow_rpc,
                          force=args.force) for j in jobs]
        for fut in as_completed(futs):
            tid, res = fut.result()
            summary[tid] = res
            print(f"{tid:<32} {res['status']:<22} "
                  f"{res.get('rows', ''):>7} {res.get('seconds', '')}",
                  flush=True)
    io_utils.write_json(SUMMARY, {"targets": dict(sorted(summary.items()))},
                        generator="code/data/enumerate.py", meta={})
    ok = sum(1 for v in summary.values() if v["status"] == "ok")
    print(f"\n{ok}/{len(summary)} targets ok")


if __name__ == "__main__":
    main()
