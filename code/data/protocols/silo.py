"""Silo v2. Market layer."""
from common import evmabi, logs, multicall

T_NS = evmabi.topic0(
    "NewSilo(address,address,address,address,address,address)")
T_SHARE = evmabi.topic0("NewSiloShareTokens(address,address,address)")
T_VAULT = evmabi.topic0(
    "CreateSiloVault(address,address,address,uint256,address,string,string)")


def markets(ccfg, entry):
    raw, upto = logs.scan(ccfg, topics=[[T_NS, T_SHARE]])
    silos = {}

    def add(addr_, lg):
        silos.setdefault(addr_.lower(), {
            "silo": addr_.lower(),
            "event_factory": lg["address"].lower(),
            "created_block": lg["block_number"],
            "created_ts": lg.get("timestamp"),
            "created_tx": lg["tx_hash"],
        })

    for lg in raw:
        if lg["topics"][0] == T_NS:
            silo0, silo1, _cfg = evmabi.decode(
                ["address", "address", "address"], lg["data"])
            add(silo0, lg)
            add(silo1, lg)
        else:
            add(evmabi.addr(lg["topics"][2]), lg)

    target = logs.rpc_target(ccfg)
    rows = list(silos.values())
    if target and rows:
        addrs = [r["silo"] for r in rows]
        assets = multicall.try_view(target, addrs, "asset()", ["address"])
        configs = multicall.try_view(target, addrs, "config()", ["address"])
        factories = multicall.try_view(target, addrs, "factory()", ["address"])
        kept = []
        for r, a, c, f in zip(rows, assets, configs, factories):
            if a is None:
                continue
            r["token"] = a[0].lower()
            r["silo_config"] = c[0].lower() if c else None
            r["factory"] = f[0].lower() if f else None
            kept.append(r)
        rows = kept
    return rows, upto


def vaults(ccfg, entry):
    registry = (entry.get("silo_vaults_factory") or "").lower()
    raw, upto = logs.scan(ccfg, topics=[[T_VAULT]])
    rows = []
    for lg in raw:
        owner, timelock, name, symbol = evmabi.decode(
            ["address", "uint256", "string", "string"], lg["data"])
        rows.append({
            "kind": "silo_vault",
            "vault": evmabi.addr(lg["topics"][1]),
            "caller": evmabi.addr(lg["topics"][2]),
            "asset": evmabi.addr(lg["topics"][3]),
            "initial_owner": owner.lower(), "initial_timelock": timelock,
            "name": name, "symbol": symbol,
            "factory": lg["address"].lower(),
            "official_factory": lg["address"].lower() == registry,
            "found_by": "factory_event",
            "created_block": lg["block_number"],
            "created_ts": lg.get("timestamp"), "created_tx": lg["tx_hash"],
        })
    return rows, upto
