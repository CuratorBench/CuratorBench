"""dRPC helpers of the first-pass Morpho V1 pipeline."""

import json
import pathlib
import time
import urllib.request

MORPHO_BLUE = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"
MORPHO_API = "https://api.morpho.org/graphql"

ENV_PATH = pathlib.Path(__file__).parents[1] / ".env"


def _load_env(path=ENV_PATH):
    env = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


ENV = _load_env()

RPC_BY_CHAIN = {
    1: ENV.get("RPC_URL"),
    8453: ENV.get("RPC_URL_BASE"),
}

DEFAULT_CHUNK = {1: 1_000_000, 8453: 1_000_000}
MIN_CHUNK = 5_000

TOPIC_SUPPLY = "0xedf8870433c83823eb071d3df1caa8d008f12f6440918c20d75a3602cda30fe0"
TOPIC_WITHDRAW = "0xa56fc0ad5702ec05ce63666221f796fb62437c32db1aa1aa075fc6484cf58fbf"

TOPIC_BORROW = "0x570954540bed6b1304a87dfe815a5eda4a648f7097a16240dcd85c9b5fd42a43"
TOPIC_REPAY = "0x52acb05cebbd3cd39715469f22afbf5a17496295ef3bc9bb5944056c63ccaa09"
TOPIC_ACCRUE_INTEREST = "0x9d9bd501d0657d7dfe415f779a620a62b78bc508ddc0891fbbd8b7ac0f8fce87"
TOPIC_LIQUIDATE = "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41"
TOPIC_CREATE_MARKET = "0xac4b2400f169220b0c0afdde7a0b32e775ba727ea1cb30b35f935cdaab8683ac"

SEL_POSITION = "0x93c52062"
SEL_MARKET = "0x5c60e39a"
SEL_TOTAL_ASSETS = "0x01e1d114"


class RpcError(Exception):
    pass


def _post_json(url, payload, timeout=90):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def rpc_call(method, params, chain_id=1, retries=4):
    """JSON-RPC call with retry/backoff on network errors."""
    url = RPC_BY_CHAIN.get(chain_id)
    if not url:
        raise RpcError(f"no RPC endpoint configured for chain {chain_id} "
                       f"(check code/.env)")
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    last_err = None
    for attempt in range(retries):
        try:
            out = _post_json(url, payload)
            if "error" in out:
                raise RpcError(out["error"])
            return out["result"]
        except RpcError:
            raise
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RpcError(f"chain {chain_id}: {last_err}")


def get_logs(address, topics, from_block, to_block, chain_id=1):
    """Chunked eth_getLogs with call-local adaptive chunk size."""
    logs = []
    start = from_block
    chunk = DEFAULT_CHUNK[chain_id]
    streak = 0
    while start <= to_block:
        end = min(start + chunk - 1, to_block)
        try:
            res = rpc_call(
                "eth_getLogs",
                [{
                    "address": address,
                    "fromBlock": hex(start),
                    "toBlock": hex(end),
                    "topics": topics,
                }],
                chain_id=chain_id,
            )
        except RpcError:
            if chunk <= MIN_CHUNK:
                raise
            chunk = max(chunk // 2, MIN_CHUNK)
            streak = 0
            continue
        if len(res) >= 10_000 and end > start:
            chunk = max(chunk // 2, 1)
            streak = 0
            continue
        logs.extend(res)
        start = end + 1
        streak += 1
        if streak >= 4 and chunk < DEFAULT_CHUNK[chain_id]:
            chunk = min(chunk * 2, DEFAULT_CHUNK[chain_id])
            streak = 0
        time.sleep(0.05)
    return logs


def eth_call(to, data, block, chain_id=1):
    """Archive eth_call. block."""
    tag = hex(block) if isinstance(block, int) else block
    return rpc_call("eth_call", [{"to": to, "data": data}, tag], chain_id=chain_id)


def block_number(chain_id=1):
    return int(rpc_call("eth_blockNumber", [], chain_id=chain_id), 16)


def block_timestamp(block, chain_id=1, _cache={}):
    key = (chain_id, block)
    if key not in _cache:
        res = rpc_call("eth_getBlockByNumber", [hex(block), False], chain_id=chain_id)
        _cache[key] = int(res["timestamp"], 16)
    return _cache[key]


def batch_timestamps(chain_id, blocks, chunk=500, quiet=False):
    """Exact timestamps for many blocks, one JSON-RPC batch per `chunk`."""
    url = RPC_BY_CHAIN[chain_id]
    out = {}
    blocks = sorted(set(blocks))
    for i in range(0, len(blocks), chunk):
        part = blocks[i:i + chunk]
        payload = [{"jsonrpc": "2.0", "id": j,
                    "method": "eth_getBlockByNumber",
                    "params": [hex(b), False]}
                   for j, b in enumerate(part)]
        for attempt in range(4):
            try:
                res = _post_json(url, payload)
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(1.5 * (attempt + 1))
        failed = []
        for r in res:
            if "result" not in r or r["result"] is None:
                failed.append(part[r["id"]])
            else:
                out[part[r["id"]]] = int(r["result"]["timestamp"], 16)
        for b in failed:
            for attempt in range(6):
                try:
                    one = _post_json(url, {"jsonrpc": "2.0", "id": 1,
                                           "method": "eth_getBlockByNumber",
                                           "params": [hex(b), False]})
                    if one.get("result"):
                        out[b] = int(one["result"]["timestamp"], 16)
                        break
                except Exception:
                    pass
                time.sleep(2.0 * (attempt + 1))
            else:
                raise RpcError(f"batch ts: block {b} failed after retries")
        if not quiet:
            print(f"    ts {min(i + chunk, len(blocks))}/{len(blocks)}",
                  end="\r")
    if not quiet:
        print()
    return out


def block_at_timestamp(ts, lo, hi, chain_id=1):
    """Largest block with timestamp <= ts, via bisection on [lo, hi]."""
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if block_timestamp(mid, chain_id) <= ts:
            lo = mid
        else:
            hi = mid - 1
        time.sleep(0.05)
    return lo


def words(hexdata):
    """Split 0x-prefixed ABI data into 32-byte ints."""
    raw = hexdata[2:]
    return [int(raw[i:i + 64], 16) for i in range(0, len(raw), 64)]


def topic_address(topic):
    """Decode an address from a 32-byte topic."""
    return "0x" + topic[-40:]


def morpho_gql(query, variables=None, retries=5):
    last_err = None
    for attempt in range(retries):
        try:
            out = _post_json(MORPHO_API,
                             {"query": query, "variables": variables or {}})
            if "errors" in out:
                raise RuntimeError(json.dumps(out["errors"])[:500])
            return out["data"]
        except RuntimeError:
            raise
        except Exception as e:
            last_err = e
            time.sleep(2.0 * (attempt + 1))
    raise RuntimeError(f"morpho_gql failed after {retries} tries: {last_err}")
