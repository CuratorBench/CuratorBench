"""Morpho: markets from CreateMarket on the singleton; vaults from the creation events of the MetaMorpho (V1, V1.1) and Vault V2 factories."""
from common import evmabi, logs, multicall

from . import capped

T_MARKET = evmabi.topic0(
    "CreateMarket(bytes32,(address,address,address,address,uint256))")
T_V1 = evmabi.topic0(
    "CreateMetaMorpho(address,address,address,uint256,address,string,string,bytes32)")
T_V2 = evmabi.topic0("CreateVaultV2(address,address,bytes32,address)")


def markets(ccfg, entry, singleton_key="morpho"):
    raw, upto = logs.scan(ccfg, addresses=[entry[singleton_key]],
                          topics=[[T_MARKET]])
    rows = []
    for lg in raw:
        loan, coll, oracle, irm, lltv = evmabi.decode(
            ["address", "address", "address", "address", "uint256"],
            lg["data"])
        rows.append({
            "market_id": lg["topics"][1],
            "loan_token": loan.lower(),
            "collateral_token": coll.lower(),
            "oracle": oracle.lower(),
            "irm": irm.lower(),
            "lltv": str(lltv),
            "created_block": lg["block_number"],
            "created_ts": lg.get("timestamp"),
            "created_tx": lg["tx_hash"],
        })
    return rows, upto


def vaults(ccfg, entry):
    v1 = {a.lower() for a in entry.get("vault_v1_factories", [])}
    v2 = {a.lower() for a in entry.get("vault_v2_factories", [])}
    raw, upto = logs.scan(ccfg, topics=[[T_V1, T_V2]])
    rows = []
    for lg in raw:
        factory = lg["address"].lower()
        base = {"factory": factory, "found_by": "factory_event",
                "created_block": lg["block_number"],
                "created_ts": lg.get("timestamp"), "created_tx": lg["tx_hash"]}
        if lg["topics"][0] == T_V1:
            try:
                owner, timelock, name, symbol, _salt = evmabi.decode(
                    ["address", "uint256", "string", "string", "bytes32"],
                    lg["data"])
            except Exception:
                continue
            rows.append({**base, "kind": "morpho_v1",
                         "vault": evmabi.addr(lg["topics"][1]),
                         "caller": evmabi.addr(lg["topics"][2]),
                         "asset": evmabi.addr(lg["topics"][3]),
                         "initial_owner": owner.lower(),
                         "initial_timelock": timelock,
                         "name": name, "symbol": symbol,
                         "official_factory": factory in v1})
        else:
            rows.append({**base, "kind": "morpho_v2",
                         "initial_owner": evmabi.addr(lg["topics"][1]),
                         "asset": evmabi.addr(lg["topics"][2]),
                         "vault": evmabi.addr(lg["topics"][3]),
                         "name": None, "symbol": None,
                         "official_factory": factory in v2})
    target = logs.rpc_target(ccfg)
    v2rows = [r for r in rows if r["kind"] == "morpho_v2"]
    if target and v2rows:
        addrs = [r["vault"] for r in v2rows]
        names = multicall.try_view(target, addrs, "name()", ["string"])
        symbols = multicall.try_view(target, addrs, "symbol()", ["string"])
        for r, n, s in zip(v2rows, names, symbols):
            r["name"] = n[0] if n else None
            r["symbol"] = s[0] if s else None
    extra, _ = capped.vaults(ccfg, capped.T_CAP_ID, "MORPHO()",
                             entry["morpho"], {r["vault"] for r in rows},
                             "morpho_v1")
    return rows + extra, upto
