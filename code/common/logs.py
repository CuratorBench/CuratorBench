"""Unified log scanning."""
import json
from pathlib import Path

from . import hypersync_client, rpc

CONFIG = Path(__file__).resolve().parents[1] / "config"


def load_chains() -> dict:
    return json.loads((CONFIG / "chains.json").read_text())


def rpc_target(cfg: dict):
    """dRPC slug or raw URL for state reads on this chain (None if neither)."""
    return cfg.get("drpc") or cfg.get("rpc_url")


def _normalize_rpc_log(lg: dict) -> dict:
    return {
        "address": lg["address"],
        "topics": lg.get("topics") or [],
        "data": lg.get("data"),
        "block_number": int(lg["blockNumber"], 16),
        "log_index": int(lg["logIndex"], 16),
        "tx_hash": lg["transactionHash"],
    }


def scan(cfg: dict, addresses=None, topics=None, from_block=0,
         rpc_chunk=50_000):
    """Return (logs, scanned_to_block)."""
    if cfg.get("hypersync"):
        return hypersync_client.scan_logs(
            cfg["hypersync"], addresses=addresses, topics=topics,
            from_block=from_block)
    target = rpc_target(cfg)
    if not target:
        raise RuntimeError(f"chain {cfg.get('name')}: no scan route")
    head = rpc.block_number(target)
    raw = rpc.get_logs(target, address=addresses, topics=topics,
                       from_block=from_block, to_block=head, chunk=rpc_chunk)
    return [_normalize_rpc_log(lg) for lg in raw], head + 1
