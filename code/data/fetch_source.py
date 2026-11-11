# /// script
# requires-python = ">=3.11"
# ///
"""Fetch Etherscan-verified Solidity source for a contract and store it under data/contracts/{chain}-{address}/ — one file per source unit."""

import json
import pathlib
import sys
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from legacy_rpc import ENV

DATA = pathlib.Path(__file__).parents[2] / "data"


def fetch_source(chain_id, address, label=None):
    key = ENV.get("ETHERSCAN_API_KEY", "")
    url = (f"https://api.etherscan.io/v2/api?chainid={chain_id}"
           f"&module=contract&action=getsourcecode&address={address}"
           f"&apikey={key}")
    with urllib.request.urlopen(url, timeout=60) as resp:
        out = json.loads(resp.read())
    if out.get("status") != "1":
        raise RuntimeError(f"etherscan: {out.get('result')}")
    item = out["result"][0]
    name = item["ContractName"]
    if not item["SourceCode"]:
        raise RuntimeError(f"{address} on chain {chain_id}: not verified")

    outdir = DATA / "contracts" / f"{chain_id}-{address.lower()}"
    outdir.mkdir(parents=True, exist_ok=True)

    src = item["SourceCode"]
    files = {}
    if src.startswith("{{"):
        parsed = json.loads(src[1:-1])
        for path, unit in parsed["sources"].items():
            files[path] = unit["content"]
    elif src.startswith("{"):
        parsed = json.loads(src)
        for path, unit in parsed.get("sources", parsed).items():
            files[path] = unit["content"] if isinstance(unit, dict) else unit
    else:
        files[f"{name}.sol"] = src

    for path, content in files.items():
        dest = outdir / path.replace("/", "__")
        dest.write_text(content)
    meta = {"contract_name": name, "compiler": item["CompilerVersion"],
            "label": label or name, "n_files": len(files)}
    (outdir / "_meta.json").write_text(json.dumps(meta, indent=1))
    print(f"{chain_id}-{address}: {name} ({item['CompilerVersion']}), "
          f"{len(files)} source files -> {outdir}")
    return outdir


if __name__ == "__main__":
    cid = int(sys.argv[1])
    addr = sys.argv[2]
    fetch_source(cid, addr, sys.argv[3] if len(sys.argv) > 3 else None)
