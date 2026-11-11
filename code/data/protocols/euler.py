"""Euler v2. Market layer."""
from common import evmabi, logs

from . import capped

T_PROXY = evmabi.topic0("ProxyCreated(address,bool,address,bytes)")
T_LTV = evmabi.topic0("GovSetLTV(address,uint16,uint16,uint16,uint48,uint32)")
T_EARN = evmabi.topic0(
    "CreateEulerEarn(address,address,address,uint256,address,string,string,bytes32)")
ZERO = "0x" + "0" * 40


def markets(ccfg, entry):
    raw, upto = logs.scan(ccfg, addresses=[entry["evault_factory"]],
                          topics=[[T_PROXY]])
    vaults_ = {}
    for lg in raw:
        proxy = evmabi.addr(lg["topics"][1])
        upgradeable, _impl, trailing = evmabi.decode(
            ["bool", "address", "bytes"], lg["data"])
        t = trailing.hex()
        vaults_[proxy] = {
            "type": "vault", "vault": proxy,
            "asset": "0x" + t[0:40] if len(t) >= 40 else None,
            "oracle": "0x" + t[40:80] if len(t) >= 80 else None,
            "unit_of_account": "0x" + t[80:120] if len(t) >= 120 else None,
            "upgradeable": upgradeable,
            "created_block": lg["block_number"],
            "created_ts": lg.get("timestamp"), "created_tx": lg["tx_hash"],
        }
    ltv_raw, _ = logs.scan(ccfg, topics=[[T_LTV]])
    links = []
    for lg in ltv_raw:
        vault = lg["address"].lower()
        if vault not in vaults_:
            continue
        borrow_ltv, liq_ltv, _init, _target_ts, _ramp = evmabi.decode(
            ["uint16", "uint16", "uint16", "uint48", "uint32"], lg["data"])
        links.append({
            "type": "ltv", "vault": vault,
            "collateral_vault": evmabi.addr(lg["topics"][1]),
            "borrow_ltv": borrow_ltv, "liquidation_ltv": liq_ltv,
            "block": lg["block_number"], "ts": lg.get("timestamp"),
        })
    return list(vaults_.values()) + links, upto


def vaults(ccfg, entry):
    factory = entry.get("euler_earn_factory")
    raw, upto = [], None
    if factory and factory.lower() != ZERO:
        raw, upto = logs.scan(ccfg, addresses=[factory], topics=[[T_EARN]])
    rows = []
    for lg in raw:
        owner, timelock, name, symbol, _salt = evmabi.decode(
            ["address", "uint256", "string", "string", "bytes32"], lg["data"])
        rows.append({
            "kind": "euler_earn",
            "vault": evmabi.addr(lg["topics"][1]),
            "caller": evmabi.addr(lg["topics"][2]),
            "asset": evmabi.addr(lg["topics"][3]),
            "initial_owner": owner.lower(), "initial_timelock": timelock,
            "name": name, "symbol": symbol,
            "factory": lg["address"].lower(), "official_factory": True,
            "found_by": "factory_event",
            "created_block": lg["block_number"],
            "created_ts": lg.get("timestamp"), "created_tx": lg["tx_hash"],
        })
    extra, cap_upto = capped.vaults(ccfg, capped.T_CAP_4626, "EVC()",
                                    entry["evc"], {r["vault"] for r in rows},
                                    "euler_earn")
    return rows + extra, upto or cap_upto
