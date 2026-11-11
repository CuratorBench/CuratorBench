# /// script
# requires-python = ">=3.11"
# dependencies = ["pycryptodome"]
# ///
"""Derive event topics / function selectors used by the pipeline."""
import json
import pathlib
import sys
import urllib.request

from Crypto.Hash import keccak

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from legacy_rpc import ENV, MORPHO_BLUE


def k(s):
    h = keccak.new(digest_bits=256)
    h.update(s.encode())
    return h.hexdigest()


def canonical(inputs):
    parts = []
    for i in inputs:
        t = i["type"]
        if t == "tuple":
            t = "(" + ",".join(c["type"] for c in i["components"]) + ")"
        parts.append(t)
    return ",".join(parts)


def fetch_blue_abi():
    key = ENV.get("ETHERSCAN_API_KEY", "")
    url = (f"https://api.etherscan.io/v2/api?chainid=1&module=contract"
           f"&action=getabi&address={MORPHO_BLUE}&apikey={key}")
    with urllib.request.urlopen(url, timeout=30) as r:
        out = json.loads(r.read())
    if out.get("status") != "1":
        raise RuntimeError(f"etherscan: {out.get('result')}")
    return json.loads(out["result"])


FUNCTIONS = {
    "SEL_POSITION": "position(bytes32,address)",
    "SEL_MARKET": "market(bytes32)",
    "SEL_TOTAL_ASSETS": "totalAssets()",
    "SEL_ID_TO_MARKET_PARAMS": "idToMarketParams(bytes32)",
}
EXTRA_EVENTS = {
    "TOPIC_CREATE_METAMORPHO":
        "CreateMetaMorpho(address,address,address,uint256,address,string,string,bytes32)",
}

if __name__ == "__main__":
    try:
        abi = fetch_blue_abi()
        print("# Morpho Blue events (from Etherscan-verified ABI):")
        for item in abi:
            if item.get("type") == "event":
                sig = f"{item['name']}({canonical(item['inputs'])})"
                indexed = [i["name"] for i in item["inputs"] if i.get("indexed")]
                name = f"TOPIC_{item['name'].upper()}"
                print(f'{name} = "0x{k(sig)}"')
                print(f"#   {sig}  indexed={indexed}")
    except Exception as e:
        print(f"# Etherscan unavailable ({e}); no Blue ABI derivation")
    print("\n# Non-Blue events:")
    for name, sig in EXTRA_EVENTS.items():
        print(f'{name} = "0x{k(sig)}"  # {sig}')
    print("\n# Function selectors:")
    for name, sig in FUNCTIONS.items():
        print(f'{name} = "0x{k(sig)[:8]}"  # {sig}')
