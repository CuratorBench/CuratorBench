"""Moolah (Lista Lending)."""
from common import evmabi, logs

from . import capped, morpho

T_VAULT = evmabi.topic0(
    "CreateMoolahVault(address,address,address,address,uint256,address,"
    "address,address,address,address,string,string)")


def markets(ccfg, entry):
    return morpho.markets(ccfg, entry, singleton_key="moolah")


def vaults(ccfg, entry):
    factory = entry["vault_factory"]
    raw, upto = logs.scan(ccfg, addresses=[factory], topics=[[T_VAULT]])
    rows = []
    for lg in raw:
        (_impl, _mgr_tl, _cur_tl, delay, manager, curator, guardian, name,
         symbol) = evmabi.decode(
            ["address", "address", "address", "uint256", "address",
             "address", "address", "string", "string"], lg["data"])
        rows.append({
            "kind": "moolah_vault",
            "vault": evmabi.addr(lg["topics"][1]),
            "caller": evmabi.addr(lg["topics"][2]),
            "asset": evmabi.addr(lg["topics"][3]),
            "manager": manager.lower(), "curator": curator.lower(),
            "guardian": guardian.lower(), "initial_timelock": delay,
            "name": name, "symbol": symbol,
            "factory": lg["address"].lower(), "official_factory": True,
            "found_by": "factory_event",
            "created_block": lg["block_number"],
            "created_ts": lg.get("timestamp"), "created_tx": lg["tx_hash"],
        })
    extra, _ = capped.vaults(ccfg, capped.T_CAP_ID, "MOOLAH()",
                             entry["moolah"], {r["vault"] for r in rows},
                             "moolah_vault")
    return rows + extra, upto
