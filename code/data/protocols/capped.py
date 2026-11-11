"""Vaults found by their SetCap events rather than by a factory."""
from common import evmabi, logs, multicall

T_CAP_ID = evmabi.topic0("SetCap(address,bytes32,uint256)")
T_CAP_4626 = evmabi.topic0("SetCap(address,address,uint256)")
_DEPLOY = [evmabi.topic0(s) for s in (
    "OwnershipTransferred(address,address)",
    "Upgraded(address)",
    "Initialized(uint64)", "Initialized(uint8)")]


def vaults(ccfg, cap_topic, view, core, known, kind):
    """Rows for the vaults that emitted `cap_topic`, answer `view` with `core`, and are not in `known` (the factory-created set)."""
    raw, upto = logs.scan(ccfg, topics=[[cap_topic]])
    first_cap = {}
    for lg in raw:
        a = lg["address"].lower()
        first_cap[a] = min(first_cap.get(a, lg["block_number"]),
                           lg["block_number"])
    cand = sorted(set(first_cap) - set(known))
    target = logs.rpc_target(ccfg)
    if not cand or not target:
        return [], upto
    cores = multicall.try_view(target, cand, view, ["address"])
    found = [a for a, c in zip(cand, cores)
             if c and c[0].lower() == core.lower()]
    if not found:
        return [], upto
    assets = multicall.try_view(target, found, "asset()", ["address"])
    names = multicall.try_view(target, found, "name()", ["string"])
    symbols = multicall.try_view(target, found, "symbol()", ["string"])
    deploy, _ = logs.scan(ccfg, addresses=found, topics=[_DEPLOY])
    born = {}
    for lg in deploy:
        a = lg["address"].lower()
        if a not in born or lg["block_number"] < born[a]["block_number"]:
            born[a] = lg
    rows = []
    for a, asset, n, s in zip(found, assets, names, symbols):
        b = born.get(a)
        rows.append({
            "kind": kind, "vault": a,
            "asset": asset[0].lower() if asset else None,
            "name": n[0] if n else None, "symbol": s[0] if s else None,
            "factory": None, "official_factory": False,
            "found_by": "set_cap", "first_cap_block": first_cap[a],
            "created_block": b["block_number"] if b else first_cap[a],
            "created_ts": b.get("timestamp") if b else None,
            "created_tx": b["tx_hash"] if b else None,
        })
    return rows, upto
