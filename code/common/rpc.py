"""JSON-RPC access to dRPC archive nodes."""
import json
import threading
import time

import requests

from . import env

_TIMEOUT = 30
_RETRIES = 6
_sent = 0
_lock = threading.Lock()


def calls_made() -> int:
    """Requests sent by this process so far (dRPC bills per request)."""
    return _sent


def url(chain: str) -> str:
    """`chain` is a dRPC slug, or a full https:// URL for chains dRPC does not carry (public endpoints recorded in config/chains.json)."""
    if chain.startswith("http"):
        return chain
    return f"https://lb.drpc.live/{chain}/{env.require('DRPC_API_KEY')}"


def call(chain: str, method: str, params: list):
    global _sent
    last = None
    for attempt in range(_RETRIES):
        with _lock:
            _sent += 1
        try:
            resp = requests.post(url(chain), timeout=_TIMEOUT, json={
                "jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            body = resp.json()
            if "result" in body:
                return body["result"]
            last = body.get("error")
            if isinstance(last, dict) and ("rate" in str(last).lower()
                                           or last.get("code") in (-32005, 429)):
                time.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"{chain} {method}: {last}")
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last = exc
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"{chain} {method}: giving up after {_RETRIES} tries: {last}")


def chain_id(chain: str) -> int:
    return int(call(chain, "eth_chainId", []), 16)


def block_number(chain: str) -> int:
    return int(call(chain, "eth_blockNumber", []), 16)


def get_block(chain: str, number, hydrate=False) -> dict:
    tag = hex(number) if isinstance(number, int) else number
    return call(chain, "eth_getBlockByNumber", [tag, hydrate])


def eth_call(chain: str, to: str, data: str, block="latest") -> str:
    tag = hex(block) if isinstance(block, int) else block
    return call(chain, "eth_call", [{"to": to, "data": data}, tag])


def get_code(chain: str, address: str, block="latest") -> str:
    tag = hex(block) if isinstance(block, int) else block
    return call(chain, "eth_getCode", [address, tag])


_RANGE_ERR = ("distance", "range", "limit", "max")


def _advertised_range(err: str):
    """Providers state their cap in the error ('max ..."""
    import re as _re
    nums = [int(n) for n in _re.findall(r"\d+", err) if 10 <= int(n) <= 10_000_000]
    return min(nums) if nums else None


def get_logs(chain: str, address=None, topics=None, from_block=0,
             to_block="latest", chunk=1_000_000, min_chunk=100,
             max_requests=25_000):
    """Fallback log scan for chains not indexed by HyperSync."""
    if to_block == "latest":
        to_block = block_number(chain)
    out, frm = [], from_block
    requests_made = 0
    while frm <= to_block:
        requests_made += 1
        if (requests_made > 10 and
                (to_block - frm) // chunk + requests_made > max_requests):
            raise RuntimeError(
                f"{chain}: getLogs cap {chunk} blocks would need "
                f">{max_requests} requests; not viable")
        to = min(frm + chunk - 1, to_block)
        flt = {"fromBlock": hex(frm), "toBlock": hex(to)}
        if address:
            flt["address"] = address
        if topics:
            flt["topics"] = topics
        try:
            out.extend(call(chain, "eth_getLogs", [flt]))
        except RuntimeError as exc:
            msg = str(exc).lower()
            if any(k in msg for k in ("unknown network", "parse request",
                                      "unauthorized", "not found")):
                raise
            if chunk > min_chunk:
                cap = _advertised_range(msg) if any(
                    k in msg for k in _RANGE_ERR) else None
                chunk = max(min_chunk, min(cap or chunk // 2, chunk // 2))
                continue
            raise
        frm = to + 1
    return out
