# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""E1, oracle type: classify every market's oracle by what one price call reads, so the tail-loss metric knows whether a market can see a depeg of its collateral."""
import argparse
import hashlib
import json
import os
import pathlib
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from eth_abi.exceptions import DecodingError

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common import env, evmabi, io_utils, logs, multicall, rpc

DATA = io_utils.DATA
CACHE = DATA / "registry" / "oracles" / "_cache"
SOURCES = DATA / "contracts"
ZERO = "0x" + "0" * 40

PROBES = [
    ("price", "price()", ["uint256"]),
    ("decimals", "decimals()", ["uint8"]),
    ("description", "description()", ["string"]),
    ("name", "name()", ["string"]),
    ("symbol", "symbol()", ["string"]),
    ("latest_round", "latestRoundData()",
     ["uint80", "int256", "uint256", "uint256", "uint80"]),
    ("aggregator", "aggregator()", ["address"]),
    ("type_and_version", "typeAndVersion()", ["string"]),
    ("base_feed_1", "BASE_FEED_1()", ["address"]),
    ("base_feed_2", "BASE_FEED_2()", ["address"]),
    ("quote_feed_1", "QUOTE_FEED_1()", ["address"]),
    ("quote_feed_2", "QUOTE_FEED_2()", ["address"]),
    ("base_vault", "BASE_VAULT()", ["address"]),
    ("quote_vault", "QUOTE_VAULT()", ["address"]),
    ("vault", "VAULT()", ["address"]),
    ("base_sample", "BASE_VAULT_CONVERSION_SAMPLE()", ["uint256"]),
    ("quote_sample", "QUOTE_VAULT_CONVERSION_SAMPLE()", ["uint256"]),
    ("vault_sample", "VAULT_CONVERSION_SAMPLE()", ["uint256"]),
    ("scale_factor", "SCALE_FACTOR()", ["uint256"]),
    ("asset", "asset()", ["address"]),
    ("data_feed_id", "getDataFeedId()", ["bytes32"]),
    ("price_id", "priceId()", ["bytes32"]),
    ("wat", "wat()", ["bytes32"]),
]


def nz(a):
    return bool(a) and int(a, 16) != 0


def _jsonable(v):
    if isinstance(v, bytes):
        return "0x" + v.hex()
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, str) and v.startswith("0x") and len(v) == 42:
        return v.lower()
    if isinstance(v, int) and abs(v) >= 2 ** 53:
        return str(v)
    return v


class AppendCache:
    """Append-only JSONL keyed by `key`."""

    def __init__(self, path, key="address"):
        self.path, self.key, self.rows = path, key, {}
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            for line in path.read_text().splitlines():
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                self.rows[row[key]] = row

    def __contains__(self, k):
        return k in self.rows

    def get(self, k):
        return self.rows.get(k)

    def add_many(self, rows):
        with self.path.open("a") as fh:
            for row in rows:
                fh.write(json.dumps(row, sort_keys=True) + "\n")
                self.rows[row[self.key]] = row
            fh.flush()


def probe(target, cid, addrs):
    """Getter battery for each address, cached in probes.jsonl."""
    cache = AppendCache(CACHE / str(cid) / "probes.jsonl")
    todo = sorted({a for a in addrs if nz(a) and a not in cache})

    def run(batch):
        calls = [(a, evmabi.selector(sig)) for a in batch
                 for _, sig, _ in PROBES]
        try:
            res = multicall.aggregate3(target, calls)
        except Exception as exc:
            if len(batch) == 1:
                return [{"address": batch[0], "probe_error": str(exc)[:150]}]
            mid = len(batch) // 2
            return run(batch[:mid]) + run(batch[mid:])
        out = []
        for j, a in enumerate(batch):
            row = {"address": a}
            for k, (pname, _sig, rets) in enumerate(PROBES):
                ok, ret = res[j * len(PROBES) + k]
                val = None
                if ok and ret not in ("0x", ""):
                    try:
                        val = evmabi.decode(rets, ret)
                        val = _jsonable(val if len(rets) > 1 else val[0])
                    except Exception:
                        val = None
                row[pname] = val
            out.append(row)
        return out

    for i in range(0, len(todo), 25):
        cache.add_many(run(todo[i:i + 25]))
    if todo:
        print(f"  probed {len(todo)} new nodes", flush=True)
    return cache


def skeleton(code_hex):
    """Runtime code with PUSH32 immediates and CBOR metadata masked."""
    code = bytes.fromhex(code_hex.removeprefix("0x"))
    if len(code) >= 2:
        meta_len = int.from_bytes(code[-2:], "big")
        if 0 < meta_len < len(code) and code[-2 - meta_len] in (0xa1, 0xa2):
            code = code[:-2 - meta_len]
    out, i = bytearray(), 0
    while i < len(code):
        op = code[i]
        out.append(op)
        if 0x60 <= op <= 0x7f:
            n = op - 0x5f
            out.extend(b"\0" * n if op == 0x7f else code[i + 1:i + 1 + n])
            i += n
        i += 1
    return hashlib.sha256(bytes(out)).hexdigest()[:16]


def code_info(target, cid, addrs):
    """address -> {skeleton, size, eip1167 implementation}, cached."""
    cache = AppendCache(CACHE / str(cid) / "code.jsonl")
    todo = sorted({a for a in addrs if nz(a) and a not in cache})

    def one(a):
        try:
            code = rpc.get_code(target, a)
        except Exception as exc:
            return {"address": a, "error": str(exc)[:120]}
        row = {"address": a, "size": (len(code) - 2) // 2,
               "skeleton": skeleton(code) if len(code) > 2 else None}
        h = code.removeprefix("0x")
        if h.startswith("363d3d373d3d3d363d73") and len(h) == 90:
            row["eip1167"] = "0x" + h[20:60]
        return row

    with ThreadPoolExecutor(max_workers=12) as pool:
        for i in range(0, len(todo), 200):
            cache.add_many(list(pool.map(one, todo[i:i + 200])))
            print(f"  code {min(i + 200, len(todo))}/{len(todo)}", flush=True)
    return cache


def opcode_flags(target, cid, addr):
    """Whether addr's runtime code can call another contract and whether it reads storage, from its opcodes (cached)."""
    cache = AppendCache(CACHE / str(cid) / "opcodes.jsonl")
    if addr in cache:
        return cache.get(addr)
    code = bytes.fromhex(rpc.get_code(target, addr).removeprefix("0x"))
    ops, i = set(), 0
    while i < len(code):
        op = code[i]
        ops.add(op)
        i += 1 + (op - 0x5f if 0x60 <= op <= 0x7f else 0)
    row = {"address": addr, "calls": bool(ops & {0xf1, 0xf2, 0xf4, 0xfa}),
           "sload": 0x54 in ops, "size": len(code)}
    cache.add_many([row])
    return row


PRICE_SEL = "0xa035b1fe"
_TRACE_BLOCK = {}


def _slim_frame(f):
    """A callTracer frame without gas and the chain-specific transfer lists (Arbitrum and Robinhood add before/afterEVMTransfers)."""
    out = {k: f[k] for k in ("type", "from", "to", "input", "output",
                             "error", "revertReason", "gasUsed") if k in f}
    for k in ("from", "to"):
        if out.get(k):
            out[k] = out[k].lower()
    if f.get("calls"):
        out["calls"] = [_slim_frame(c) for c in f["calls"]]
    return out


def trace_call(target, cid, addr, data=PRICE_SEL, tracer="callTracer",
               time=None, block=None, frm=None):
    """debug_traceCall of addr with calldata data at one block per chain and run (the head when the first new trace of the run."""
    cache = AppendCache(CACHE / str(cid) / "trace.jsonl", key="key")
    key = f"{addr}:{data}:{tracer}" + (f"@time={time}" if time else "") \
        + (f"@block={block}" if block else "") + (f"@from={frm}" if frm
                                                  else "")
    if key not in cache:
        if block is None:
            if cid not in _TRACE_BLOCK:
                _TRACE_BLOCK[cid] = rpc.block_number(target)
            blk = _TRACE_BLOCK[cid]
        else:
            blk = block
        row = {"key": key, "address": addr, "data": data, "tracer": tracer,
               "block": blk}
        cfg = {"tracer": tracer}
        call = {"to": addr, "data": data}
        if frm:
            row["from"] = call["from"] = frm
        if block is not None:
            row["historical"] = True
        if time:
            row["time"] = time
            cfg["blockOverrides"] = {"time": hex(time)}
        try:
            res = rpc.call(target, "debug_traceCall", [call, hex(blk), cfg])
            if tracer == "callTracer":
                row["result"] = _slim_frame(res)
            else:
                row["result"] = {a.lower(): sorted((v.get("storage") or {}))
                                 for a, v in res.items()}
        except RuntimeError as exc:
            row["rpc_error"] = str(exc)[:200]
        cache.add_many([row])
    return cache.get(key)


def core_address(protocol, cid):
    """The protocol's singleton on a chain (Morpho, Moolah) from config/deployments.json, lower case; None for others."""
    dep = json.loads((HERE.parent / "config" / "deployments.json").read_text())
    ch = dep["protocols"].get(protocol, {}).get("chains", {}).get(str(cid), {})
    a = ch.get("morpho") or ch.get("moolah")
    return a.lower() if a else None


def set_trace_context(rs, protocol, key, callers):
    """Per market: the block and callers retry_trace uses (historical mode only)."""
    if rs.historical:
        rs.hist_block = panel_last_block(protocol, rs.cid, key)
        rs.trace_from = [c for c in callers if nz(c)]


def call_from(rs, to, data, frm, block="latest"):
    """eth_call of to with data from frm (an access-controlled reader), cached in calls.jsonl; the return data, or None if it reverts."""
    cache = AppendCache(CACHE / str(rs.cid) / "calls.jsonl", key="key")
    key = f"{to}:{data}@{block}#from={frm}"
    if key not in cache:
        tag = hex(block) if isinstance(block, int) else block
        try:
            ret = rpc.call(rs.target, "eth_call",
                           [{"to": to, "data": data, "from": frm}, tag])
            row = {"key": key, "ok": True, "ret": ret}
        except RuntimeError as exc:
            row = {"key": key, "ok": False, "ret": "0x",
                   "error": str(exc)[:120]}
        cache.add_many([row])
    got = cache.get(key)
    return got["ret"] if got["ok"] and got["ret"] not in ("0x", "") \
        else None


def panel_last_block(protocol, cid, key):
    """Block of the last event logged for a market (Morpho and Moolah market id, Silo silo) in data/panel/<protocol>/<cid>/<key>.csv."""
    path = DATA / "panel" / protocol / str(cid) / f"{key}.csv"
    if not key or not path.exists():
        return None
    last = None
    with path.open() as fh:
        next(fh, None)
        for line in fh:
            if line.strip():
                last = line
    return int(last.split(",", 1)[0]) if last else None


ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"
ROUTESCAN = ("https://api.routescan.io/v2/network/mainnet/evm/{cid}/"
             "etherscan/api")
_last_es = [0.0]
ES_CALLS = [0]
RS_CALLS = [0]
RS_UNSUPPORTED = {}


def _getsourcecode(url, params, counter, spacing):
    """First result item of an Etherscan-compatible getsourcecode call; {"api_error"."""
    wait = spacing - (time.time() - _last_es[0])
    if wait > 0:
        time.sleep(wait)
    _last_es[0] = time.time()
    for attempt in range(4):
        try:
            counter[0] += 1
            body = requests.get(url, timeout=60, params={
                "module": "contract", "action": "getsourcecode",
                **params}).json()
            if body.get("status") == "1":
                return body["result"][0]
            if "rate" in str(body.get("result", "")).lower():
                time.sleep(1.0 + attempt)
                continue
            return {"api_error": str(body.get("message") or
                                     body.get("result"))[:120]}
        except (requests.RequestException, ValueError):
            time.sleep(1.0 + attempt)
    return None


def etherscan_source(cid, addr):
    """Verified source of one address, cached under data/contracts/."""
    meta_path = SOURCES / f"{cid}-{addr}" / "_meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        if "contract_name" in meta:
            return meta
    item = _getsourcecode(ETHERSCAN_V2, {
        "chainid": cid, "address": addr,
        "apikey": env.require("ETHERSCAN_API_KEY")}, ES_CALLS, 0.22)
    if not item or "api_error" in item:
        return {"contract_name": None, "verified": False, "fetch_error": True}
    return _save_source(cid, addr, item, ETHERSCAN_V2)


def routescan_source(cid, addr):
    """Verified source from Routescan for an address whose cached record says Etherscan v2 has none."""
    meta_path = SOURCES / f"{cid}-{addr}" / "_meta.json"
    meta = json.loads(meta_path.read_text())
    if meta.get("contract_name") or "routescan_verified" in meta:
        return meta
    url = ROUTESCAN.format(cid=cid)
    item = {"api_error": RS_UNSUPPORTED[cid]} if cid in RS_UNSUPPORTED \
        else _getsourcecode(url, {"address": addr}, RS_CALLS, 0.5)
    if item is None:
        return meta
    if item.get("SourceCode"):
        return _save_source(cid, addr, item, url,
                            {"etherscan_v2_verified": False})
    answer = item.get("api_error") or "unverified"
    if "not supported" in answer:
        RS_UNSUPPORTED[cid] = answer
    meta.update(routescan_verified=False, routescan_source=url,
                routescan_answer=answer)
    tmp = meta_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=1))
    tmp.replace(meta_path)
    return meta


def _save_source(cid, addr, item, url, extra=None):
    """Write one getsourcecode item under data/contracts/<cid>-<addr>/."""
    d = SOURCES / f"{cid}-{addr}"
    meta_path = d / "_meta.json"
    src = item.get("SourceCode") or ""
    files = {}
    if src.startswith("{{"):
        parsed = json.loads(src[1:-1])
        files = {p: u["content"] for p, u in parsed["sources"].items()}
    elif src.startswith("{"):
        parsed = json.loads(src)
        files = {p: (u["content"] if isinstance(u, dict) else u)
                 for p, u in parsed.get("sources", parsed).items()}
    elif src:
        files = {f"{item['ContractName']}.sol": src}
    d.mkdir(parents=True, exist_ok=True)
    for p, content in files.items():
        (d / p.replace("/", "__")).write_text(content)
    meta = {"contract_name": item.get("ContractName") or None,
            "verified": bool(src), "compiler": item.get("CompilerVersion"),
            "implementation": (item.get("Implementation") or "").lower() or None,
            "n_files": len(files), "label": item.get("ContractName"),
            "source": url, "chain_id": cid, "address": addr, **(extra or {})}
    tmp = meta_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=1))
    tmp.replace(meta_path)
    return meta


def main_source(cid, addr, name):
    """Text of the file that defines contract `name` (not its libraries or interfaces), or '' when the source is not cached."""
    d = SOURCES / f"{cid}-{addr}"
    if not name or not d.exists():
        return ""
    pat = re.compile(rf"\bcontract\s+{re.escape(name)}\b")
    for f in sorted(d.iterdir()):
        if f.suffix in (".sol", ".vy") and f.name != "_meta.json":
            text = f.read_text(errors="ignore")
            if pat.search(text):
                return text
    return ""


def source_name(cid, addr, text=True):
    """Contract name of a saved source of addr (any package's layout), or None; with text, only when its main file is saved too."""
    p = SOURCES / f"{cid}-{addr}" / "_meta.json"
    if not p.exists():
        return None
    name = json.loads(p.read_text()).get("contract_name")
    if not name or (text and not main_source(cid, addr, name)):
        return None
    return name


SIGNALS = {
    "share_rate": r"\.(convertToAssets|previewRedeem|getPooledEthByShares|"
                  r"stEthPerToken|tokensPerStEth|ratio|lpPrice|"
                  r"getVirtualPrice|getFloorPrice|getPriceForRedemption|"
                  r"getPriceForIssuance|"
                  r"\w*([Pp]ricePerShare|[Ee]xchangeRate|SharePrice|"
                  r"sharePrice|ExitAssets|[Nn]av|NAV|[Rr]ate)\w*)\(",
    "reads_feed": r"\.(latestRoundData|latestAnswer|getPrice|price|read|"
                  r"getPriceUnsafe|getPriceNoOlderThan|readWithAge|peek|"
                  r"getValue)\(",
    "pt_rate": r"\.(getPtToAssetRate|getPtToSyRate)\(|PendlePYOracleLib",
    "dex": r"\.(observe|consult|getTimepoints|getLpToAssetRate|"
           r"getYtToAssetRate|price_oracle|ema_price|getQuoteAtTick|"
           r"getTwap)\(|PendleLpOracleLib",
    "min": r"Math\.min\(|\bmin\(|_min\(",
    "linear_discount": r"[Dd]iscount|maturity",
}
NAME_KIND = {
    "NakedUSD0ppAdapter": "rate",
    "ChainlinkRTOracle": "rate",
    "USPChainlinkAdapter": "rate",
    "CHFUSDChainLinkPriceAdapter": "market",
    "PPSOracle": "stored",
    "MockOracle": "stored",
    "ClampFeed": "min",
    "OjoPTFeed": "min",
    "SpectraPriceOracle": "rate",
    "CurveStableswapOracle": "min",
    "StableUsdtPriceFeed": "clamp",
    "IdleOracle": "constant",
    "AtlasOracleAdaptor": "market",
    "OracleAdaptor": "rate_then_market",
    "USRPriceAggregatorV3Interface": "rate",
    "RlpPriceAggregatorV3Interface": "rate",
    "MellowPriceFeed": "rate",
    "GemUsdcOracle": "rate",
    "OneWeiPriceFeed": "constant",
    "lisUSDPriceFeed": "constant",
    "USDXLiquidationPriceFeed": "stored",
    "KCMorphoOracle": "stored",
    "YieldTokenAggregator": "rate",
    "ACSPriceFeed": "stored",
}
PENDLE_PT = {"PendleChainlinkOracle", "PendleChainlinkOracleWithQuote"}
LISTA_EXPAND = {
    "WstETHPriceFeed": ("wstETH_ETH_PriceFeed()", "ETH_TOKEN_ADDR()"),
    "sUSDePriceFeed": ("sUSDe_USDe_PriceFeed()", "USDe_TOKEN_ADDR()"),
    "SlisBnbPriceFeed": (None, "WBNB_TOKEN_ADDR()"),
}
ORACLE_NETWORKS = ("redstone", "pyth", "chronicle", "api3", "dia",
                   "chainlink", "oval", "stork", "atlas")
RATE_NAME = re.compile(r"(?i)rateprovider|exchangerate|sharerate|navoracle")
FIAT = {"USD", "EUR", "CHF", "JPY", "GBP", "BRL", "MXN", "XAU"}
FORWARDERS = {
    "MetaOracleDeviationTimelock": ("currentOracle()", "primaryOracle()",
                                    "backupOracle()"),
    "ProxyOracle": ("delegate()",),
}


EORACLE_FEEDS = {
    54: "market",
    85: "market",
    132: "rate",
    146: "rate",
    162: "rate",
    163: "rate",
}
EORACLE_TRACE_BLOCKS = {
    43114: {"2025-10-28": 71035144, "2025-11-01": 71245524,
            "2025-11-02": 71295398, "2025-11-03": 71347737,
            "2025-11-04": 71402507, "2025-11-05": 71458069,
            "2025-11-06": 71513151, "2025-11-07": 71569863,
            "2025-11-08": 71626350, "2025-11-10": 71734123,
            "2025-11-20": 72277819, "2025-12-15": 73783578},
}


def signals(text):
    return sorted(k for k, p in SIGNALS.items() if re.search(p, text))


MORPHO_COMPOSITES = {"MorphoChainlinkOracleV2", "MorphoChainlinkOracle",
                     "ChainlinkOracle"}
IMPL_SLOT = ("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca50"
             "5d382bbc")
BEACON_SLOT = ("0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b"
               "35133d50")
ZOS_SLOT = ("0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048"
            "ed3f8c3")
CHAINLINK_PROXY = {"EACAggregatorProxy", "AggregatorProxy",
                   "PriceFeedProxy"}
RATE_WORDS = ("exchange rate", "exchangerate", "exchange-rate", " nav",
              "fundamental", "redemption", "/ rate", "rate provider",
              "conversion", "share price", "_fundamental")
DESC_PAIR = re.compile(r"([A-Za-z0-9.+_\-]+)\s*/\s*([A-Za-z0-9.+_\-]+)")


def norm_sym(s):
    s = (s or "").upper().replace(" ", "")
    s = s.removeprefix("SY-")
    return {"WETH": "ETH", "WBNB": "BNB"}.get(s, s)


WRAPPED_NATIVE = {
    14: ("0x1d80c49bbbcd1c0911346656b529df9e5c2f783d", "WFLR", "FLR"),
    50: ("0x951857744785e80e2de051c32ee7b25f9c458c42", "WXDC", "XDC"),
    137: ("0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270", "WPOL",
          "POL"),
    143: ("0x3bd359c1119da7da1d913d1c4d2b7c461115433a", "WMON",
          "MON"),
    146: ("0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38", "WS",
          "S"),
    252: ("0xfc00000000000000000000000000000000000002", "WFRAX",
          "FRAX"),
    999: ("0x5555555555555555555555555555555555555555", "WHYPE",
          "HYPE"),
    1329: ("0xe30fedd158a2e3b13e9badaeabafc5516e95e8c7", "WSEI",
           "SEI"),
    1776: ("0x0000000088827d2d103ee2d9a6b781773ae03ffb", "WINJ", "INJ"),
    5000: ("0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8", "WMNT",
           "MNT"),
    9745: ("0x6100e367285b01f48d07953803a2d8dca5d19873", "WXPL",
           "XPL"),
    42793: ("0xc9b53ab2679f573e480d01e0f49e2b5cfb7a3eab", "WXTZ", "XTZ"),
    43114: ("0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7", "WAVAX",
            "AVAX"),
    80094: ("0x6969696969696969696969696969696969696969", "WBERA",
            "BERA"),
    98866: ("0xea237441c92cae6fc17caaf9a7acb3f953be4bd1", "WPLUME",
            "PLUME"),
}


def pt_underlying(sym):
    """'USR' from 'PT-USR-4SEP2025'."""
    m = re.match(r"^PT-(.+?)-\d{1,2}[A-Z]{3}\d{4}$", sym or "")
    return norm_sym(m.group(1)) if m else None


def desc_pair(desc):
    """('BASE', 'QUOTE') from a feed description such as 'USDe / USD'."""
    if not desc:
        return None
    m = DESC_PAIR.search(desc)
    if not m:
        return None
    return norm_sym(m.group(1)), norm_sym(m.group(2))


def bytes32_text(v):
    if not v:
        return None
    try:
        t = bytes.fromhex(v.removeprefix("0x")).rstrip(b"\0").decode()
        return t if t.isprintable() and t else None
    except (ValueError, UnicodeDecodeError):
        return None


class Resolver:
    """Kinds of nodes on one chain, with the evidence that decides them."""

    def __init__(self, cid, target):
        self.cid, self.target = cid, target
        self.probes = AppendCache(CACHE / str(cid) / "probes.jsonl")
        self.code = AppendCache(CACHE / str(cid) / "code.jsonl")
        self.skel_rep = {}
        self.symbols = {}
        self.impls = AppendCache(CACHE / str(cid) / "impl.jsonl")
        self.via = {}
        self.wrapped = WRAPPED_NATIVE.get(cid)
        self._fallback = {}
        self._ref = None
        self.historical = False
        self.hist_block = None
        self.trace_from = []

    def prepare(self, addrs):
        """Probe, code-hash and fetch sources for addrs and every component they name, down to the leaves."""
        frontier = sorted({a for a in addrs if nz(a)})
        seen = set()
        while frontier:
            seen.update(frontier)
            self.probes = probe(self.target, self.cid, frontier)
            nxt = set()
            for a in frontier:
                p = self.probes.get(a) or {}
                for k in ("base_feed_1", "base_feed_2", "quote_feed_1",
                          "quote_feed_2", "aggregator"):
                    if nz(p.get(k)) and p[k] not in seen:
                        nxt.add(p[k])
            frontier = sorted(nxt)
        self.code = code_info(self.target, self.cid, seen)
        self.fetch_sources(seen)
        return seen

    def fetch_sources(self, addrs):
        reps = {}
        for a in sorted(addrs):
            c = self.code.get(a) or {}
            sk = c.get("skeleton")
            if not sk:
                continue
            reps.setdefault(sk, a)
            if (SOURCES / f"{self.cid}-{a}" / "_meta.json").exists():
                reps[sk] = a
        self.skel_rep.update(reps)
        for i, a in enumerate(sorted(set(reps.values()))):
            meta = etherscan_source(self.cid, a)
            if meta.get("implementation"):
                etherscan_source(self.cid, meta["implementation"])
            if (i + 1) % 100 == 0:
                print(f"  sources {i + 1}/{len(reps)}", flush=True)

    def source(self, addr):
        """(contract name, verified, main-file text) for addr, through the representative of its code skeleton."""
        c = self.code.get(addr) or {}
        rep = self.skel_rep.get(c.get("skeleton"), addr)
        meta_path = SOURCES / f"{self.cid}-{rep}" / "_meta.json"
        if not meta_path.exists():
            return None, None, "", rep
        meta = json.loads(meta_path.read_text())
        loc = (self.cid, rep)
        if not meta.get("contract_name"):
            if c.get("eip1167"):
                meta = {**meta, "implementation": c["eip1167"]}
            elif not meta.get("implementation"):
                loc, meta = self.fallback(rep, addr) or (loc, meta)
        impl = self.impl_of(addr) if meta.get("implementation") else None
        if impl:
            im = etherscan_source(self.cid, impl)
            iloc = (self.cid, impl)
            if not im.get("contract_name") and not im.get("fetch_error"):
                iloc, im = self.fallback(impl, addr) or (iloc, im)
            name = im.get("contract_name")
            return name, im.get("verified"), main_source(*iloc, name), impl
        name = meta.get("contract_name")
        return name, meta.get("verified"), main_source(*loc, name), rep

    def fallback(self, addr, node):
        """((chain, address), meta) of a source for addr, which the explorer has not verified."""
        if addr not in self._fallback:
            if addr not in self.code:
                self.code = code_info(self.target, self.cid, [addr])
            sk = (self.code.get(addr) or {}).get("skeleton")
            hits = self.bytecode_matches(sk) if sk else []
            out = None
            if hits:
                names = {}
                for _c, _a, n in hits:
                    names[n] = names.get(n, 0) + 1
                name = min(names, key=lambda n: (-names[n], n))
                c, a, _ = min((h for h in hits if h[2] == name),
                              key=lambda h: (h[0] != self.cid, h[0], h[1]))
                meta = json.loads((SOURCES / f"{c}-{a}" / "_meta.json")
                                  .read_text())
                out = ((c, a), {**meta, "verified": False}, {
                    "method": "bytecode_match", "chain_id": c, "address": a,
                    "contract_name": name, "masked_bytecode_hash": sk,
                    "candidate_names": sorted(names),
                    "candidates": [[c2, a2] for c2, a2, _ in hits]})
            else:
                meta = routescan_source(self.cid, addr)
                if meta.get("contract_name"):
                    out = ((self.cid, addr), meta,
                           {"method": "routescan", "address": addr,
                            "source": meta.get("source")})
            self._fallback[addr] = out
        out = self._fallback[addr]
        if out is None:
            return None
        self.via[node] = out[2]
        return out[0], out[1]

    def bytecode_matches(self, sk):
        """Verified contracts [(chain, address, name)] whose code skeleton is sk, on this chain and on every chain with a finished."""
        if self._ref is None:
            chains = logs.load_chains()
            done = {int(f.stem) for f in
                    (DATA / "registry" / "oracles").glob("*/*.jsonl")}
            self._ref = []
            for c in sorted(done | {self.cid}):
                code = self.code if c == self.cid else \
                    AppendCache(CACHE / str(c) / "code.jsonl")
                impls = {r["eip1167"] for r in code.rows.values()
                         if r.get("eip1167")}
                ipath = CACHE / str(c) / "impl.jsonl"
                if ipath.exists():
                    impls |= {r["implementation"] for r in
                              AppendCache(ipath).rows.values()
                              if nz(r.get("implementation"))}
                todo = [a for a in impls if a not in code and
                        source_name(c, a, text=False)]
                if c == self.cid:
                    if todo:
                        self.code = code_info(self.target, c, todo)
                    continue
                if todo:
                    code = code_info(logs.rpc_target(chains[str(c)]), c, todo)
                self._ref.append((c, code))
        out = []
        for c, code in [(self.cid, self.code)] + self._ref:
            for a, r in sorted(code.rows.items()):
                name = source_name(c, a) if r.get("skeleton") == sk else None
                if name:
                    out.append((c, a, name))
        return out

    def impl_of(self, addr):
        """Logic contract of a proxy."""
        c = self.code.get(addr) or {}
        if c.get("eip1167"):
            return c["eip1167"]
        row = self.impls.get(addr)
        if row is None or (row["implementation"] is None
                           and not row.get("all_slots")):
            impl = None
            for slot in (IMPL_SLOT, BEACON_SLOT, ZOS_SLOT):
                word = "0x" + rpc.call(self.target, "eth_getStorageAt",
                                       [addr, slot, "latest"])[-40:]
                if not nz(word):
                    continue
                impl = _addr_call(self.target, [
                    (word, evmabi.selector("implementation()"))])[0] \
                    if slot == BEACON_SLOT else word
                if impl:
                    break
            row = {"address": addr, "implementation": impl, "all_slots": True}
            self.impls.add_many([row])
        return row["implementation"]

    def can_call(self, addr):
        c = self.code.get(addr) or {}
        if not c.get("size"):
            return False
        rep = self.skel_rep.get(c.get("skeleton"), addr)
        return opcode_flags(self.target, self.cid, rep)["calls"]

    def match_sym(self, token, sym):
        """Symbol a token is compared under."""
        if self.wrapped and token == self.wrapped[0]:
            return self.wrapped[2]
        return sym

    def native_pair(self, pair):
        """A pair read from a feed's text, with this chain's wrapper symbol read as the native asset."""
        if not self.wrapped:
            return pair
        return [self.wrapped[2] if x == self.wrapped[1] else x for x in pair]

    def token_symbols(self, tokens):
        todo = sorted({t for t in tokens if nz(t) and t not in self.symbols})
        if todo:
            res = multicall.try_view(self.target, todo, "symbol()", ["string"])
            res32 = multicall.try_view(self.target, todo, "symbol()",
                                       ["bytes32"])
            for t, r, r32 in zip(todo, res, res32):
                self.symbols[t] = (r[0] if r else
                                   bytes32_text("0x" + r32[0].hex())
                                   if r32 else None)
        return self.symbols


    def feed(self, addr, depth=0, trace=True):
        """Describe one feed: kind in {market, rate, constant, composite, min, dex, unknown}, the asset pair it prices where known, and its components."""
        node = self._feed(addr, depth)
        if node.get("contract") in CHAINLINK_AGGREGATORS and \
                node.get("kind") != "market":
            node = aggregator_feed(self, node)
        if node.get("kind") == "market" and not node.get("pair"):
            node = feed_asset(self, node)
        if trace and depth < 3 and not node.get("calculated") and (
                node.get("kind") in ("unknown", None) or
                (node.get("kind") == "market" and not node.get("pair")
                 and not node.get("calculated"))):
            node = feed_trace(self, addr, node, depth)
        return node

    def _feed(self, addr, depth=0):
        p = self.probes.get(addr) or {}
        name, verified, text, rep = self.source(addr)
        desc = p.get("description")
        node = {"address": addr, "contract": name, "verified": verified,
                "description": desc}
        if p.get("type_and_version"):
            node["type_and_version"] = p["type_and_version"]
        sig = signals(text) if text else []
        node["source_signals"] = sig
        if rep != addr:
            node["source_of"] = rep
        if addr in self.via:
            node["source_via"] = self.via[addr]
        tv = (p.get("type_and_version") or "")
        dlow = (desc or "").lower()
        is_cl = (name in CHAINLINK_PROXY or tv.startswith("EACAggregatorProxy")
                 or (nz(p.get("aggregator")) and desc))
        if self.is_morpho_composite(p, name) and depth < 3 and \
                (name or self.can_call(addr)):
            node["kind"] = "composite"
            node["legs"] = self.morpho_legs(addr, p, depth + 1)
            return node
        if name in PENDLE_PT:
            pt = {**node, "kind": "pt", "kind_basis": "Pendle oracle source",
                  **self.pendle_underlying(addr)}
            if name == "PendleChainlinkOracle":
                return pt
            q = _addr_call(self.target,
                           [(addr, evmabi.selector("quoteOracle()"))])[0]
            legs = [{"side": "base", "role": "pendle_rate", **pt}]
            if q:
                self.prepare([q])
                legs.append({"side": "base", "role": "quote_oracle",
                             **self.feed(q, depth + 1)})
            return {**node, "kind": "composite", "legs": legs}
        if name in LISTA_EXPAND and depth < 3:
            legs = self.lista_legs(addr, name, depth)
            if legs:
                return {**node, "kind": "composite", "legs": legs}
        if name == "WstETHPriceFeed" and depth < 3:
            f, w = _addr_call(self.target, [
                (addr, evmabi.selector("stETHtoETHPriceFeed()")),
                (addr, evmabi.selector("wstETH()"))])
            if f:
                self.prepare([f])
                return {**node, "kind": "composite", "legs": [
                    {"side": "base", "role": "wsteth_rate", "address": w,
                     "contract": "wstETH.tokensPerStEth", "kind": "rate",
                     "pair": ["WSTETH", "STETH"],
                     "description": "wstETH to stETH share rate"},
                    {"side": "base", "role": "steth_feed",
                     **self.feed(f, depth + 1)}]}
        ident = (bytes32_text(p.get("data_feed_id")) or bytes32_text(p.get("wat")))
        if ident:
            node["feed_id"] = ident
        pair = desc_pair(desc) or desc_pair(ident) if (desc or ident) else None
        if ident and not pair:
            pair = (norm_sym(ident.split("_")[0].split("/")[0]), None)
        if pair:
            node["pair"] = self.native_pair(list(pair))
        if name == "EOFeedAdapter":
            return self.eoracle_feed(addr, node)
        if is_cl or "redstone" in dlow or "chronicle" in dlow or \
                "pyth" in dlow:
            node["kind"] = "rate" if any(w in dlow for w in RATE_WORDS) or \
                RATE_NAME.search(name or "") else "market"
            node["kind_basis"] = "feed description"
            m = re.search(r"for ([A-Za-z0-9.+_\-]+)", desc or "")
            if not pair and m:
                node["pair"] = self.native_pair(
                    [norm_sym(m.group(1).split("_")[0]), None])
            return node
        if any(w in dlow for w in RATE_WORDS) or RATE_NAME.search(name or "") \
                or (ident and "FUNDAMENTAL" in ident.upper()):
            node["kind"] = "rate"
            node["kind_basis"] = "feed description, name or id"
            return node
        if not text:
            ops = opcode_flags(self.target, self.cid, rep) if \
                (self.code.get(addr) or {}).get("size") else None
            if ops and not ops["calls"]:
                node["kind"] = "stored" if ops["sload"] else "constant"
                node["kind_basis"] = "bytecode: no external call"
                node["bytecode"] = ops
                return node
            node["kind"] = "unknown"
            node["reason"] = "unverified" if verified is False else "no source"
            if ops:
                node["bytecode"] = ops
            return node
        node["kind"] = self.kind_from_source(name, text, sig, p)
        node["kind_basis"] = ("reviewed contract name" if name in NAME_KIND
                              else "source scan")
        if node["kind"] == "stored" and name not in NAME_KIND:
            ops = opcode_flags(self.target, self.cid, rep)
            node["bytecode"] = ops
            if ops["calls"]:
                node["kind"] = "unknown"
                node["reason"] = ("no external read in the main source file, "
                                  "but the bytecode makes calls")
        if node["kind"] == "pt":
            hint = "sy" if "getPtToSyRate" in text and \
                "getPtToAssetRate" not in text else "asset"
            node.update(self.pendle_underlying(addr, hint))
        return node

    def eoracle_feed(self, addr, node):
        """An eOracle feed: its feed id (getFeedId()), its feed manager (_feedManager, storage slot 0 of the adapter, whose source."""
        ok, ret = multicall.aggregate3(self.target, [
            (addr, evmabi.selector("getFeedId()"))])[0]
        fid = int(ret, 16) if ok and len(ret) == 66 else None
        manager = "0x" + rpc.call(self.target, "eth_getStorageAt",
                                  [addr, "0x0", "latest"])[-40:]
        meta = etherscan_source(self.cid, manager)
        if meta.get("implementation"):
            meta = etherscan_source(self.cid, meta["implementation"])
        node.update(eoracle_feed_id=fid, feed_manager=manager,
                    feed_manager_contract=meta.get("contract_name"))
        kind = EORACLE_FEEDS.get(fid)
        if kind:
            node.update(kind=kind, kind_basis=f"reviewed eOracle feed id {fid}")
        else:
            node.update(kind="unknown",
                        reason=f"eOracle feed id {fid} not reviewed")
        blocks = EORACLE_TRACE_BLOCKS.get(self.cid)
        if blocks:
            cache = AppendCache(CACHE / str(self.cid) / "eoracle_trace.jsonl",
                                key="key")
            sel = evmabi.selector("latestRoundData()")
            todo = [b for b in blocks.values() if f"{addr}@{b}" not in cache]
            for b in todo:
                ok, ret = multicall.aggregate3(self.target, [(addr, sel)],
                                               block=b)[0]
                got = evmabi.decode(["uint80", "int256", "uint256", "uint256",
                                     "uint80"], ret) if ok and ret != "0x" \
                    else None
                cache.add_many([{"key": f"{addr}@{b}", "block": b,
                                 "answer": str(got[1]) if got else None,
                                 "updated_at": got[3] if got else None}])
            node["trace"] = [{"date": d, **{k: v for k, v in
                                            cache.get(f"{addr}@{b}").items()
                                            if k != "key"}}
                             for d, b in blocks.items()]
        return node

    def pendle_underlying(self, addr, hint="asset", market=None,
                          otype=None):
        """The PT's underlying for a Pendle Chainlink-compatible oracle."""
        cache = AppendCache(CACHE / str(self.cid) / "pendle.jsonl")
        if addr not in cache:
            row = {"address": addr}
            if market is None:
                res = multicall.aggregate3(self.target, [
                    (addr, evmabi.selector("market()")),
                    (addr, evmabi.selector("baseOracleType()"))])
                market = "0x" + res[0][1][-40:] if res[0][0] and \
                    len(res[0][1]) >= 66 else None
                otype = int(res[1][1], 16) if res[1][0] and \
                    len(res[1][1]) >= 66 else None
            row.update(market=market, oracle_type=otype)
            if market:
                ok, ret = multicall.aggregate3(self.target, [
                    (market, evmabi.selector("readTokens()"))])[0]
                if ok and len(ret) >= 66 * 3 - 4:
                    sy = "0x" + ret[2:66][-40:]
                    got = multicall.aggregate3(self.target, [
                        (sy, evmabi.selector("yieldToken()")),
                        (sy, evmabi.selector("assetInfo()"))])
                    yt = "0x" + got[0][1][-40:] if got[0][0] and \
                        len(got[0][1]) >= 66 else None
                    asset = "0x" + got[1][1][2 + 64:2 + 128][-40:] \
                        if got[1][0] and len(got[1][1]) >= 2 + 192 else None
                    row.update(sy=sy, yield_token=yt, sy_asset=asset)
            cache.add_many([row])
        row = cache.get(addr)
        if otype not in (0, 1):
            otype = row.get("oracle_type")
        if otype not in (0, 1):
            otype = 0 if hint == "sy" else 1
        yt, asset = row.get("yield_token"), row.get("sy_asset")
        under = yt if otype == 0 else asset
        syms = self.token_symbols([x for x in (yt, asset) if nz(x)])
        out = {"pendle_market": row.get("market"),
               "pendle_oracle_type": {0: "PT_TO_SY", 1: "PT_TO_ASSET"}[otype],
               "sy": row.get("sy"), "underlying": under,
               "underlying_symbol": (self.match_sym(under, syms.get(under))
                                     if nz(under) else None)}
        if otype == 1 and nz(yt) and nz(asset) and yt != asset:
            out["yield_token_symbol"] = self.match_sym(yt, syms.get(yt))
            out["implicit_share_rate"] = True
        return out

    def lista_legs(self, addr, name, depth):
        """Legs of a Lista feed from LISTA_EXPAND."""
        feed_g, under_g = LISTA_EXPAND[name]
        getters = ["resilientOracle()", under_g] + \
            ([feed_g] if feed_g else ["stakeManager()"])
        res_oracle, under, first = _addr_call(
            self.target, [(addr, evmabi.selector(g)) for g in getters])
        if not (res_oracle and under):
            return None
        sym = self.match_sym(under, self.token_symbols([under]).get(under))
        legs = []
        if feed_g and first:
            self.prepare([first])
            legs.append({"side": "base", "role": "rate_or_price_feed",
                         **self.feed(first, depth + 1)})
        else:
            legs.append({"side": "base", "role": "stake_manager_rate",
                         "address": first, "contract": "stakeManager",
                         "kind": "rate", "pair": [None, norm_sym(sym)],
                         "description": "convertSnBnbToBnb"})
        main = None
        if res_oracle and under:
            ok, ret = multicall.aggregate3(self.target, [(res_oracle,
                evmabi.encode_call("getTokenConfig(address)", ["address"],
                                   [under]))])[0]
            if ok and ret not in ("0x", ""):
                main = evmabi.decode(TOKEN_CONFIG, ret)[0][1][0].lower()
        if nz(main):
            self.prepare([main])
            leg = {"side": "base", "role": "underlying_main_feed",
                   **self.feed(main, depth + 1)}
            if not leg.get("pair"):
                leg["pair"] = [norm_sym(sym), "USD"]
            legs.append(leg)
        else:
            legs.append({"side": "base", "role": "underlying_main_feed",
                         "address": None, "kind": "unknown",
                         "reason": "ResilientOracle config unreadable"})
        return legs

    @staticmethod
    def kind_from_source(name, text, sig, p):
        """Kind of a contract from its reviewed name or, failing that, from what its main source file calls."""
        if name in NAME_KIND:
            return NAME_KIND[name]
        n = (name or "").lower()
        if "lineardiscount" in n:
            return "rate"
        reads = "reads_feed" in sig or "share_rate" in sig
        if not reads and ("fixed" in n or "constant" in n or
                          "hardcoded" in n or "static" in n or
                          "oneto" in n or "pegged" in n):
            return "constant"
        if "mock" in n or "test" in n:
            return "stored"
        if RATE_NAME.search(name or "") or any(
                w in n for w in ("customaggregator", "netassetvalue",
                                 "chroniclevao")):
            return "rate"
        if "min" in sig and ("reads_feed" in sig or "pt_rate" in sig):
            return "min"
        if "pt_rate" in sig:
            return "pt"
        if "dex" in sig:
            return "dex"
        if "linear_discount" in sig and "reads_feed" not in sig:
            return "rate"
        if "share_rate" in sig and "reads_feed" not in sig:
            return "rate"
        if "share_rate" in sig and "reads_feed" in sig:
            return "composite_rate"
        if any(w in n for w in ORACLE_NETWORKS) and "reads_feed" not in sig:
            return "market"
        if not sig and "proxy" not in n:
            return "stored"
        return "unknown"


    @staticmethod
    def is_morpho_composite(p, name=None):
        """Whether price() is Morpho's composite of feeds and vaults."""
        if not name:
            return p.get("base_feed_1") is not None
        if name not in MORPHO_COMPOSITES:
            return False
        return any(k in p and p.get(k) is not None for k in
                   ("base_feed_1", "base_vault", "vault"))

    def morpho_legs(self, addr, p, depth=0):
        """Legs of MorphoChainlinkOracle (V1."""
        legs = []
        bv = p.get("base_vault") if p.get("base_vault") is not None \
            else p.get("vault")
        if nz(bv):
            legs.append({"side": "base", "role": "vault_conversion",
                         "address": bv})
        for k in ("base_feed_1", "base_feed_2"):
            if nz(p.get(k)):
                legs.append({"side": "base", "role": k,
                             **self.feed(p[k], depth)})
        if nz(p.get("quote_vault")):
            legs.append({"side": "quote", "role": "vault_conversion",
                         "address": p["quote_vault"]})
        for k in ("quote_feed_1", "quote_feed_2"):
            if nz(p.get(k)):
                legs.append({"side": "quote", "role": k,
                             **self.feed(p[k], depth)})
        return legs


TRACE_SEL = {
    "0xfeaf968c": ("feed", "latestRoundData()"),
    "0x50d25bcd": ("feed", "latestAnswer()"),
    "0x79458ace": ("feed", "latestQuote()"),
    "0xa035b1fe": ("feed", "price()"),
    "0xae68676c": ("feed", "getQuote(uint256,address,address)"),
    "0x13b0be33": ("feed", "quote(uint256,address)"),
    "0xacefafae": ("token_oracle", "peek(address)"),
    "0x57de26a4": ("feed", "read()"),
    "0x0fdb11cf": ("feed", "fetchPrice()"),
    "0x98d5fdca": ("feed", "getPrice()"),
    "0x960384a0": ("dia", "getValue(string)"),
    "0x44e02982": ("redstone", "getValueForDataFeed(bytes32)"),
    "0xa4ae35e0": ("pyth", "getPriceNoOlderThan(bytes32,uint256)"),
    "0x96834ad3": ("pyth", "getPriceUnsafe(bytes32)"),
    "0xf69058c1": ("stork", "getTemporalNumericValueUnsafeV1(bytes32)"),
    "0x472c22f1": ("api3", "dapiNameHashToDataFeedId(bytes32)"),
    "0x351fc251": ("token_oracle", "getPriceFromOracle(address,uint256)"),
    "0x41976e09": ("token_oracle", "getPrice(address)"),
    "0xaea91078": ("token_oracle", "price(address)"),
    "0xd02641a0": ("token_oracle", "getTokenPrice(address)"),
    "0x67a7cfb7": ("api3", "dataFeeds(bytes32)"),
    "0x91526c36": ("api3", "oevDataFeed(uint256,bytes32)"),
    "0x07a2d13a": ("rate", "convertToAssets(uint256)"),
    "0x4cdad506": ("rate", "previewRedeem(uint256)"),
    "0x01e1d114": ("rate", "totalAssets()"),
    "0x99530b06": ("rate", "pricePerShare()"),
    "0x77c7b8fc": ("rate", "getPricePerFullShare()"),
    "0x679aefce": ("rate", "getRate()"),
    "0x3ba0b9a9": ("rate", "exchangeRate()"),
    "0x035faf82": ("rate", "stEthPerToken()"),
    "0x7a28fb88": ("rate", "getPooledEthByShares(uint256)"),
    "0x05b2bfb0": ("rate", "totalManagedAssets()"),
    "0x265f3e0e": ("rate", "getNlpByWnlp(uint256)"),
    "0x9290d427": ("rate", "virtualPrice(address)"),
    "0xa2e9ca62": ("rate", "estimatedAumUsd(address)"),
    "0xbb7b8b80": ("rate", "get_virtual_price()"),
    "0xf51dd485": ("rate", "preview_withdraw(uint256)"),
    "0x34109b33": ("rate", "convertToAsset()"),
    "0x0df1ff24": ("rate", "EGGStoSONIC(uint256)"),
    "0x0b906835": ("rate", "previewRedeemSyb(uint256)"),
    "0x044e69f4": ("rate", "previewRedeemXbtc(uint256)"),
    "0x9e65741e": ("rate", "exchangePrice()"),
    "0x10821ff9": ("rate", "exchangeRate(uint32)"),
    "0x85906256": ("rate", "convertToTokens(uint256)"),
    "0xda9a6cf8": ("rate", "getUnderlyingByWrapped(uint256)"),
    "0xa34ff137": ("rate", "0xa34ff137() (reported total value)"),
    "0xa60bf13d": ("multiplier", "uiMultiplier()"),
    "0x1b3ed722": ("multiplier", "multiplier()"),
    "0x2b63c300": ("multiplier", "getCurrentMultiplier()"),
    "0x883bdbfd": ("dex", "observe(uint32[])"),
    "0x3850c7bd": ("dex", "slot0()"),
    "0x0902f1ac": ("dex", "getReserves()"),
    "0x1e2eaeaf": ("dex", "extsload(bytes32)"),
    "0x86fc88d3": ("dex", "price_oracle()"),
    "0x68727653": ("dex", "price_oracle(uint256)"),
    "0xcc2b27d7": ("dex", "calc_withdraw_one_coin(uint256,int128)"),
    "0x4fb08c5e": ("dex", "calc_withdraw_one_coin(uint256,uint256)"),
    "0x4d44ca89": ("dex", "getLpToSyRate(address,uint32)"),
    "0x252c09d7": ("dex", "observations(uint256)"),
    "0xf140a35a": ("dex", "getAmountOut(uint256,address)"),
    "0x32cd935a": ("dex", "previewSwapExactPtForToken(address,uint256,address)"),
    "0xabca0eab": ("pt", "getPtToAssetRate(address,uint32)"),
    "0xa31426d1": ("pt", "getPtToSyRate(address,uint32)"),
    "0x18160ddd": ("state", "totalSupply()"),
    "0x70a08231": ("state", "balanceOf(address)"),
    "0x722713f7": ("state", "balanceOf()"),
    "0x1a686502": ("state", "liquidity()"),
    "0x0dfe1681": ("state", "token0()"),
    "0xd21220a7": ("state", "token1()"),
    "0x38d52e0f": ("state", "asset()"),
    "0x313ce567": ("state", "decimals()"),
    "0x5c60da1b": ("check", "implementation()"),
    "0x7706ba52": ("check", "oraclePaused()"),
    "0xb187bd26": ("check", "isPaused()"),
    "0xbc61e733": ("check", "isPaused(uint8)"),
    "0xc7ff1584": ("check", "isHalted()"),
    "0x13993bb7": ("check", "isFresh(uint8,uint8)"),
    "0x8b1b925f": ("check", "isValid(address)"),
    "0xfb2eeb3b": ("check", "getLastUpdateDetails(bytes32)"),
    "0xecd8e907": ("check", "sessionAt(uint256)"),
    "0x80b39083": ("check", "closedFor(uint256)"),
    "0x0d345c62": ("check", "isSynced()"),
    "0x97a4064f": ("check", "effectiveAt()"),
    "0x8f775839": ("check", "settled()"),
    "0x53a0ea31": ("check", "lastClassifiedAt()"),
    "0x3403c2fc": ("check", "emergencyShutdown()"),
    "0x9485aa88": ("check", "wfracPriceUpdatedAt()"),
    "0xcf6eefb7": ("check", "pendingDefaultAdmin()"),
    "0x6190bfb0": ("check", "getAllSupplyControllerAddresses()"),
    "0x45035edd": ("check", "withdraw_hook()"),
    "0xfc7b9c18": ("check", "totalDebt()"),
    "0x3813c35a": ("check", "managementFeeBps()"),
    "0xbc0bdf50": ("check", "lastFeeAccrual()"),
    "0x324d8438": ("check", "EXIT_FEE_BPS()"),
    "0xd4197e82": ("check", "getOracleParams(address)"),
    "0xc3f909d4": ("check", "getConfig()"),
    "0x93d9d94c": ("check", "CRYPTOPOOL()"),
    "0x2a943945": ("check", "amm()"),
}
TRACE_STORED_INPUT = {
    "UsdcToCrncOracle": ("0x47d13fb0803409c7fe169a210d4e906165f1a432",
                         "the CRNC side is wethToCrncAverage, a Uniswap V2 "
                         "TWAP that update() stores"),
}
TRACE_STORED_SKELETON = {"fb7eaba3e2565f0c": "UsdcToCrncOracle"}
TRACE_SKIP = {"dcb6e345ab138f07"}
PROXY_SLOTS = {IMPL_SLOT, BEACON_SLOT, ZOS_SLOT,
               "0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a7178"
               "50b5d6103"}
PYTH_SYMBOLS_URL = "https://history.pyth-lazer.dourolabs.app/history/v1/symbols"
_PYTH = {}


def pyth_symbols():
    """Pyth price id (Hermes id) -> symbol, description and asset type, from Pyth's symbol list, which keeps inactive feeds that Hermes no longer lists."""
    if not _PYTH:
        path = CACHE / "pyth_symbols.json"
        if not path.exists():
            rows = requests.get(PYTH_SYMBOLS_URL, timeout=120).json()
            feeds = {r["hermes_id"].lower(): {
                k: r.get(k) for k in ("symbol", "description", "asset_type",
                                      "state")}
                for r in rows if r.get("hermes_id")}
            path.write_text(json.dumps({
                "source": PYTH_SYMBOLS_URL,
                "fetched_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                             time.gmtime()),
                "feeds": feeds}, indent=0, sort_keys=True))
        _PYTH.update(json.loads(path.read_text())["feeds"])
    return _PYTH


def _pow10(a, b):
    """Whether a and b differ by a power of ten (1 included)."""
    hi, lo = max(a, b), min(a, b)
    if hi % lo:
        return False
    q = hi // lo
    while q % 10 == 0:
        q //= 10
    return q == 1


def cached_calls(target, cid, calls, block="latest", storage=False):
    """eth_call results for (to, calldata) pairs through multicall, cached in _cache/<cid>/calls.jsonl under to:calldata@block."""
    cache = AppendCache(CACHE / str(cid) / "calls.jsonl", key="key")
    keys = [f"{t}:{d}@{block}" for t, d in calls]
    todo = sorted({(t, d) for (t, d), k in zip(calls, keys) if k not in cache})
    if todo and storage:
        tag = hex(block) if isinstance(block, int) else block
        cache.add_many([{"key": f"{t}:{d}@{block}", "ok": True,
                         "ret": rpc.call(target, "eth_getStorageAt", [
                             t, d.split(":", 1)[1], tag])} for t, d in todo])
        todo = []
    if todo:
        res = multicall.aggregate3(target, todo, block=block)
        cache.add_many([{"key": f"{t}:{d}@{block}", "ok": ok, "ret": ret}
                        for (t, d), (ok, ret) in zip(todo, res)])
    return [cache.get(k) for k in keys]


def _frame_reads(frame):
    """External calls a contract makes in one frame."""
    out = []
    for c in frame.get("calls") or []:
        if c["type"] == "DELEGATECALL":
            out.extend(_frame_reads(c))
        elif c["input"][:10] != "0x5c60da1b":
            out.append(c)
    return out


def _sel_name(sel):
    return TRACE_SEL.get(sel, (None, sel))[1]


def _summary(frame, depth=0):
    """Call tree as [depth, type, to, selector name, error] lines."""
    out = []
    for c in frame.get("calls") or []:
        out.append([depth, c["type"], c["to"], _sel_name(c["input"][:10])]
                   + ([c["error"]] if c.get("error") else []))
        out.extend(_summary(c, depth + 1))
    return out


def price_trace(rs, o):
    """callTracer traces of o.price(), the head trace first."""
    tr = trace_call(rs.target, rs.cid, o)
    taken, seen, t_prev = [tr], {}, None
    for _ in range(3):
        f = tr.get("result") or {}
        if not f.get("error"):
            break
        stale = set()

        def walk(x):
            for c in x.get("calls") or []:
                if c["input"][:10] == "0xa4ae35e0":
                    stale.add((c["to"], "0x" + c["input"][10:74]))
                walk(c)
        walk(f)
        new = sorted(k for k in stale if k not in seen)
        if not new:
            break
        got = cached_calls(rs.target, rs.cid, [
            (p, "0x96834ad3" + i[2:]) for p, i in new])
        for k, g in zip(new, got):
            seen[k] = int(g["ret"][2 + 64 * 3:2 + 64 * 4], 16) \
                if g["ok"] and len(g["ret"]) >= 2 + 64 * 4 else None
        pts = [v for v in seen.values() if v]
        if not pts or max(pts) + 1 == t_prev:
            break
        t_prev = max(pts) + 1
        tr = trace_call(rs.target, rs.cid, o, time=t_prev)
        taken.append(tr)
    return taken


def retry_trace(rs, addr, data, feed=False):
    """With rs.historical, the traces of a call that reverts at the head."""
    if not rs.historical:
        return None
    if feed:
        tries = [(None, f) for f in rs.trace_from]
        if rs.hist_block and rs.trace_from:
            tries.append((rs.hist_block, rs.trace_from[0]))
    else:
        tries = [(rs.hist_block, None)] if rs.hist_block else []
    for blk, frm in tries:
        tr = trace_call(rs.target, rs.cid, addr, data=data, block=blk,
                        frm=frm)
        f = tr.get("result") or {}
        if not tr.get("rpc_error") and f and not f.get("error"):
            return tr
    return None


def _retry_note(tr, fn):
    bits = []
    if tr.get("historical"):
        bits.append(f"at block {tr['block']}, the last block with a log "
                    "of this market")
    if tr.get("from"):
        bits.append(f"from {tr['from']}")
    return (f"{fn} reverts at the head when called from the zero address; "
            f"classified from a trace " + " and ".join(bits))


def _decode_string_arg(inp):
    try:
        return evmabi.decode(["string"], "0x" + inp[10:])[0]
    except Exception:
        return None


def _id_leg(rs, c, role, owner):
    """Leg of a feed keyed by an identifier in the calldata."""
    leg = {"address": c["to"], "selector": _sel_name(c["input"][:10]),
           "kind_basis": "trace: feed identifier"}
    if role == "dia":
        key = _decode_string_arg(c["input"])
        pair = desc_pair(key)
        leg.update(contract="DIA oracle", description=f"DIA {key}",
                   feed_id=key, kind="market" if pair else "unknown")
        if pair:
            leg["pair"] = rs.native_pair(list(pair))
        else:
            leg["reason"] = f"DIA key {key!r} names no pair"
        return leg
    if role == "redstone":
        ident = bytes32_text("0x" + c["input"][10:74])
        leg.update(contract="RedStone adapter", feed_id=ident,
                   description=f"RedStone {ident}")
        if not ident:
            return {**leg, "kind": "unknown", "reason": "unreadable feed id"}
        rate = "FUNDAMENTAL" in ident.upper() or any(
            w in ident.lower() for w in RATE_WORDS)
        pair = desc_pair(ident) or (norm_sym(ident.split("_")[0]), None)
        return {**leg, "kind": "rate" if rate else "market",
                "pair": rs.native_pair(list(pair))}
    if role == "stork":
        sid = "0x" + c["input"][10:74].lower()
        hit = stork_ids().get(sid)
        leg.update(contract="Stork", feed_id=sid)
        if not hit:
            return {**leg, "kind": "unknown",
                    "reason": f"Stork id {sid[:12]}... matches no asset name"}
        return {**leg, "kind": "market", "description":
                f"Stork {hit[0]}{hit[1]}",
                "pair": rs.native_pair([norm_sym(hit[0]), norm_sym(hit[1])])}
    if role == "pyth":
        pid = c["input"][10:74].lower()
        meta = pyth_symbols().get(pid)
        leg.update(contract="Pyth", feed_id="0x" + pid)
        if not meta:
            return {**leg, "kind": "unknown",
                    "reason": f"Pyth id 0x{pid[:12]}... not in Pyth's list"}
        sym = meta["symbol"] or ""
        pair = desc_pair(sym.split(".", 1)[-1].removesuffix(".RR")
                         .split(".")[-1])
        rate = sym.endswith(".RR") or "rate" in (meta["asset_type"] or "") \
            .lower() or "nav" in (meta["asset_type"] or "").lower()
        leg.update(description=f"Pyth {sym}", pyth_state=meta.get("state"))
        if not pair:
            return {**leg, "kind": "unknown",
                    "reason": f"Pyth symbol {sym!r} names no pair"}
        return {**leg, "kind": "rate" if rate else "market",
                "pair": rs.native_pair(list(pair))}
    got = cached_calls(rs.target, rs.cid,
                       [(owner, evmabi.selector("dapiName()"))])[0]
    name = bytes32_text(got["ret"][:66]) if got["ok"] and \
        len(got["ret"]) >= 66 else None
    leg.update(address=owner, contract="Api3ReaderProxyV1",
               description=f"Api3 {name}", feed_id=name)
    pair = desc_pair(name)
    if not pair:
        return {**leg, "kind": "unknown",
                "reason": f"Api3 dAPI name {name!r} names no pair"}
    return {**leg, "kind": "market", "pair": rs.native_pair(list(pair))}


def _storage_reads(rs, o, addr, data=PRICE_SEL):
    """Storage slots addr reads during the call of o with data (price() by default), proxy slots excluded, from a prestateTracer trace (cached)."""
    pre = trace_call(rs.target, rs.cid, o, data=data, tracer="prestateTracer")
    slots = (pre.get("result") or {}).get(addr, [])
    return [s for s in slots if s not in PROXY_SLOTS]


def trace_legs(rs, o, addr, frame, depth=0):
    """(legs, checks, notes) of addr's frame in o.price()'s trace."""
    legs, checks, notes, state = [], [], [], []
    seen = set()
    reads = _frame_reads(frame)
    times = {}
    for c in reads:
        k = (c["to"], c["input"])
        times[k] = times.get(k, 0) + 1
    for c in reads:
        to, sel = c["to"], c["input"][:10]
        key = (to, sel, c["input"][10:74])
        if key in seen:
            continue
        seen.add(key)
        role, name = TRACE_SEL.get(sel, (None, sel))
        if times[(to, c["input"])] == 2 and role in (
                "feed", "dia", "redstone", "pyth", "stork", "token_oracle"):
            notes.append(f"{name} on {to} is read twice, on both sides of "
                         "the price, and cancels")
            continue
        if int(to, 16) <= 0xffff:
            legs.append({"address": to, "kind": "unknown", "selector": name,
                         "reason": f"call to precompile {to}, which the "
                                   "trace does not execute"})
            continue
        if role == "check":
            checks.append(f"{name} on {to}")
            continue
        if role == "state":
            state.append((to, name))
            continue
        if role == "token_oracle":
            tok = "0x" + c["input"][34:74]
            ok, ret = multicall.aggregate3(rs.target, [(to, evmabi.encode_call(
                "getTokenConfig(address)", ["address"], [tok]))])[0]
            main = None
            if ok and ret not in ("0x", ""):
                try:
                    main = evmabi.decode(TOKEN_CONFIG, ret)[0][1][0].lower()
                except Exception:
                    main = None
            if nz(main):
                rs.prepare([main])
                sym = rs.token_symbols([tok]).get(tok)
                leg = {"selector": name, "token_oracle": to, "token": tok,
                       **rs.feed(main, depth + 1)}
                if not leg.get("pair") and leg.get("kind") == "market":
                    leg["pair"] = [norm_sym(rs.match_sym(tok, sym)), "USD"]
                legs.append(leg)
                continue
            role = "feed"
        if role in ("dia", "redstone", "pyth", "api3", "stork"):
            leg = _id_leg(rs, c, role, addr)
            if role == "api3" and any(x.get("contract") == "Api3ReaderProxyV1"
                                      for x in legs):
                continue
            legs.append(leg)
            continue
        if role == "feed":
            reps = dict(rs.skel_rep)
            rs.prepare([to])
            rs.skel_rep.update(reps)
            node = rs.feed(to, depth + 1, trace=False)
            desc = (node.get("description") or "").lower()
            if "sequencer" in desc:
                checks.append(f"{name} on {to} ('{node.get('description')}')")
                continue
            if node.get("kind_basis") == "bytecode: no external call" and \
                    not _frame_reads(c):
                st = _storage_reads(rs, o, to)
                node = {**node, "kind": "stored" if st else "constant",
                        "kind_basis": "trace: no external call, "
                                      + ("reads storage" if st
                                         else "reads no storage")}
            if node.get("kind") in (None, "unknown") and depth < 4 and \
                    not node.get("calculated") and \
                    node.get("contract") not in CHAINLINK_AGGREGATORS:
                sub, sch, sno = trace_legs(rs, o, to, c, depth + 1)
                checks += sch
                notes += sno
                if not sub and not c.get("error"):
                    st = _storage_reads(rs, o, to)
                    sub = [{"address": to, "selector": name,
                            "kind": "stored" if st else "constant",
                            "kind_basis": "trace: no external call, "
                                          + ("reads storage" if st
                                             else "reads no storage"),
                            "description": f"{name} of {to}"}]
                notes.append(f"{name} on {to} (unidentified) expanded "
                             "from its own calls")
                if depth == 0:
                    for x in sub:
                        x["_via"] = to
                legs.extend(sub)
                continue
            if node.get("contract") == "Api3ReaderProxyV1" and \
                    not node.get("pair"):
                api3 = _id_leg(rs, c, "api3", to)
                node = {**node, **{k: api3[k] for k in (
                    "kind", "pair", "description", "feed_id", "reason")
                    if k in api3}}
            leg = {"selector": name, **node}
            if node.get("kind") == "composite":
                if not node["legs"]:
                    notes.append(f"{name} on {to} ({node.get('contract')}) "
                                 "reads no feed and no vault, so it cancels "
                                 "to its scale factor")
                for x in node["legs"]:
                    legs.append({**x, "fixed_side": True})
                continue
            legs.append(leg)
            continue
        if role == "rate" and not nz((rs.probes.get(to) or {}).get("asset")):
            inner = [x for x in _frame_reads(c) if x["input"][:10] == sel
                     and nz((rs.probes.get(x["to"]) or {}).get("asset"))]
            if inner:
                notes.append(f"{name} on {to} calls {name} on vault "
                             f"{inner[0]['to']}")
                c, to = inner[0], inner[0]["to"]
                key = (to, sel, c["input"][10:74])
                if key in seen:
                    continue
                seen.add(key)
        if role in ("rate", "multiplier"):
            p = rs.probes.get(to) or {}
            if role == "rate" and nz(p.get("asset")) and \
                    sel in ("0x07a2d13a", "0x4cdad506", "0x01e1d114"):
                if any(x.get("role") == "vault_conversion" and
                       x["address"] == to for x in legs):
                    continue
                legs.append({"role": "vault_conversion", "address": to,
                             "kind": "rate", "selector": name,
                             "vault_asset": p["asset"]})
            else:
                sym = rs.token_symbols([to]).get(to)
                legs.append({"role": role, "address": to, "kind": "rate",
                             "selector": name, "token_symbol": sym,
                             "pair": [norm_sym(rs.match_sym(to, sym)) or None,
                                      None],
                             "description": f"{name} of {sym or to}"})
            continue
        if role == "dex":
            legs.append(_dex_leg(rs, c, name))
            continue
        if role == "pt":
            mkt = "0x" + c["input"][34:74]
            otype = 1 if sel == "0xabca0eab" else 0
            info = rs.pendle_underlying(to + ":" + mkt, market=mkt,
                                        otype=otype)
            legs.append({"address": to, "kind": "pt", "selector": name,
                         "role": "pendle_rate", **info})
            continue
        node = rs.feed(to, depth + 1, trace=False) if rs.code.get(to) \
            else {}
        if node.get("verified") and node.get("kind") in ("stored",
                                                          "constant"):
            legs.append({"selector": name, **node})
            continue
        legs.append({"address": to, "kind": "unknown", "selector": name,
                     "contract": node.get("contract"),
                     "reason": f"unidentified read {name} on {to}"})
    if state and not legs:
        legs.append({"address": state[0][0], "kind": "unknown",
                     "selector": state[0][1],
                     "reason": "reads only token or pool state ("
                               + ", ".join(sorted({f"{n} on {t}"
                                                   for t, n in state}))
                               + "), no feed, rate or pool price"})
    elif state:
        notes.append("state reads beside the price inputs: " + ", ".join(
            sorted({f"{n} on {t}" for t, n in state})))
    return legs, checks, notes


def _dex_leg(rs, c, name):
    """A DEX read: the pool's two tokens (token0/token1, cached), when the pool exposes them."""
    pool = c["to"]
    leg = {"address": pool, "kind": "dex", "selector": name,
           "contract": rs.source(pool)[0] if rs.code.get(pool) else None,
           "description": f"{name} on pool {pool}"}
    if c["input"][:10] == "0x1e2eaeaf":
        leg["note"] = "Uniswap V4 PoolManager slot read; pool tokens unread"
        return leg
    got = cached_calls(rs.target, rs.cid, [
        (pool, evmabi.selector("token0()")),
        (pool, evmabi.selector("token1()"))])
    toks = ["0x" + g["ret"][-40:] if g["ok"] and len(g["ret"]) >= 66
            else None for g in got]
    if not all(toks):
        got = cached_calls(rs.target, rs.cid, [
            (pool, evmabi.encode_call("coins(uint256)", ["uint256"], [i]))
            for i in (0, 1)])
        toks = ["0x" + g["ret"][-40:] if g["ok"] and len(g["ret"]) >= 66
                else None for g in got]
    if all(toks):
        syms = rs.token_symbols(toks)
        leg["pool_tokens"] = toks
        leg["pool_symbols"] = [norm_sym(rs.match_sym(t, syms.get(t)))
                               for t in toks]
    return leg


def _order_legs(legs, coll):
    """Base legs in conversion order from the collateral."""
    rest, out, cur = list(legs), [], coll
    while rest:
        nxt = None
        for x in rest:
            inp = x.get("_in")
            if inp is None and x.get("kind") == "rate" and \
                    x.get("role") not in ("vault_conversion",):
                inp = cur
            if inp is not None and inp == cur:
                nxt = x
                break
        if nxt is None:
            break
        rest = [y for y in rest if y is not nxt]
        out.append(nxt)
        if nxt.get("kind") in ("rate", "composite_rate") and nxt.get("_out"):
            cur = nxt["_out"]
        elif nxt.get("kind") not in ("rate", "composite_rate", "pt"):
            break
    return out + rest


def _pyth_stale(f):
    return (f.get("output") or "").startswith("0x19abf40e")


def _twin_reads(rs, o):
    """Number of price inputs (reads that are not checks) in the trace of another oracle with o's code skeleton whose price() d."""
    sk = (rs.code.get(o) or {}).get("skeleton")
    cache = AppendCache(CACHE / str(rs.cid) / "trace.jsonl", key="key")
    for r in sorted(cache.rows.values(), key=lambda r: r["key"]):
        a = r["address"]
        if a == o or r.get("tracer") != "callTracer" or \
                (rs.code.get(a) or {}).get("skeleton") != sk:
            continue
        f = r.get("result") or {}
        if f and not f.get("error"):
            return len([c for c in _frame_reads(f) if TRACE_SEL.get(
                c["input"][:10], ("",))[0] != "check"]), a
    return None, None


def _loose(s):
    """Symbol for telling which leg prices the loan token."""
    s = norm_sym(s)
    s = {"USD₮0": "USDT", "USDT0": "USDT"}.get(s, s)
    return s[2:] if s.startswith("RH") and len(s) > 3 else s


def trace_classify(rs, o, row, coll):
    """(type, subtype, blind_to, reasons, evidence) of an oracle with no source, from its price() trace; None for a skipped skeleton."""
    code = rs.code.get(o) or {}
    ev = {"kind_basis": "trace"}
    if not code.get("size"):
        return ("unresolved", "no_code", None,
                [f"oracle {o}: no contract code at the head, so price() "
                 "returns no data and every borrow reverts"], ev)
    if code.get("skeleton") in TRACE_SKIP:
        return None
    taken = price_trace(rs, o)
    tr = taken[-1]
    ev["trace"] = {"block": tr.get("block"), "tracer": "callTracer"}
    if tr.get("time"):
        ev["trace"]["time_override"] = tr["time"]
    if tr.get("rpc_error"):
        return ("unresolved", "trace_failed", None,
                [f"debug_traceCall of {o}.price() failed: {tr['rpc_error']}"],
                ev)
    f = tr["result"]
    ev["trace"]["calls"] = _summary(f)
    head = [f"price() trace at block {tr['block']}"
            + (f" with the block time set to {tr['time']}" if tr.get("time")
               else "")]
    if f.get("error"):
        ev["trace"]["error"] = f["error"]
        ev["trace"]["revert_data"] = (f.get("output") or "")[:10]
        reads = sorted({f"{_sel_name(c['input'][:10])} on {c['to']}"
                        for t in taken for c in _frame_reads(t["result"])})
        n_twin, twin = _twin_reads(rs, o)
        merged = {}
        for t in taken:
            for c in _frame_reads(t["result"]):
                k = (c["to"], c["input"])
                if k not in merged or merged[k].get("error"):
                    merged[k] = c
        hist = None
        if not (all(_pyth_stale(t["result"]) for t in taken) and n_twin
                and len(merged) >= n_twin):
            hist = retry_trace(rs, o, PRICE_SEL)
        if hist:
            ev["trace"] = {"block": hist["block"], "tracer": "callTracer",
                           "historical": True, "head_block": tr.get("block"),
                           "head_error": f["error"],
                           "calls": _summary(hist["result"])}
            head = [_retry_note(hist, "price()")]
            tr, f = hist, hist["result"]
        elif not (all(_pyth_stale(t["result"]) for t in taken) and n_twin
                  and len(merged) >= n_twin):
            if rs.historical and rs.hist_block:
                ev["trace"]["historical_block_tried"] = rs.hist_block
            return ("unresolved", "trace_reverts", None,
                    [f"price() of {o} reverts at block {tr['block']}"
                     + (f" and at block {rs.hist_block}, the last block with "
                        "a log of this market" if rs.historical and
                        rs.hist_block else "") + " "
                     f"({f['error']}, {(f.get('output') or '')[:10]}); "
                     "reads before the revert: "
                     + (", ".join(reads) or "none")], ev)
        else:
            f = {"calls": list(merged.values())}
            ev["trace"]["partial"] = {"twin": twin, "twin_reads": n_twin,
                                      "reads_seen": len(merged)}
            head = [f"price() of {o} reverts on a stale Pyth price at block "
                    f"{tr['block']}; its {len(merged)} reads before the "
                    f"revert cover the {n_twin} price inputs of same-code "
                    f"oracle {twin}, whose price() succeeds"]
    sname = rs.source(o)[0] or TRACE_STORED_SKELETON.get(code.get("skeleton"))
    stored = TRACE_STORED_INPUT.get(sname)
    if stored and stored[0] == row["collateral_token"]:
        return ("unresolved", "stored_input_unseen", None, head + [
            f"{sname}: {stored[1]}; the trace shows only the "
            "other inputs"], ev)
    return trace_verdict(rs, o, f, ev, head, coll, row["loan_token"],
                         row["loan_symbol"], row["collateral_symbol"],
                         tr["block"])


def trace_verdict(rs, o, f, ev, head, coll, loan_token, loan_symbol,
                  coll_display, block, data=PRICE_SEL):
    """Type of a price from the call frame f of o (called with data)."""
    fn = "price()" if data == PRICE_SEL else _sel_name(data[:10])
    row = {"loan_token": loan_token, "loan_symbol": loan_symbol,
           "collateral_symbol": coll_display}
    tr = {"block": block}
    legs, checks, notes = trace_legs(rs, o, o, f)
    if checks:
        ev["trace"]["checks"] = sorted(set(checks))
    if notes:
        ev["trace"]["notes"] = notes
    if not legs and notes_cancel(ev):
        ev["legs"] = []
        t, sub, blind, why = classify_no_base([], coll)
        return t, sub, blind, head + notes_cancel(ev) + why, ev
    if not legs:
        st = _storage_reads(rs, o, o, data)
        ev["trace"]["storage_read"] = st
        if st:
            out = int(f.get("output") or "0x0", 16)
            vals = {}
            for slot in st:
                got = cached_calls(rs.target, rs.cid, [(
                    o, f"eth_getStorageAt:{slot}")], block=tr["block"],
                    storage=True)[0]
                vals[slot] = got["ret"]
            ev["trace"]["storage_values"] = vals
            hit = [k for k, v in vals.items() if out and int(v, 16) and (
                out % int(v, 16) == 0 or int(v, 16) % out == 0) and
                _pow10(out, int(v, 16))]
            return ("other", "stored_price",
                    f"any move of {row['collateral_symbol']} the setter does "
                    "not post",
                    [f"{fn} of {o} makes no external call and reads its "
                     f"own storage ({len(st)} slot(s)) at block "
                     f"{tr['block']}: a value an owner or keeper writes"
                     + (f"; the price is slot {hit[0]} up to a power of ten"
                        if hit else "")], ev)
        return ("fixed_price", "constant_contract",
                f"any move in the price of {row['collateral_symbol']}",
                [f"{fn} of {o} makes no external call and reads no "
                 f"storage at block {tr['block']}: it returns a constant"],
                ev)
    c = norm_sym(coll)
    loan = _loose(rs.match_sym(row["loan_token"], row["loan_symbol"]))
    vsym = {}
    for x in legs:
        x.setdefault("role", x.get("selector") or "read")
        if x.get("role") == "vault_conversion":
            a = x.get("vault_asset")
            vsym[x["address"]] = rs.match_sym(
                a, rs.token_symbols([a]).get(a)) if a else None
            vs = rs.token_symbols([x["address"]]).get(x["address"])
            x["_in"] = norm_sym(rs.match_sym(x["address"], vs))
            x["_out"] = norm_sym(vsym[x["address"]])
        elif x.get("kind") == "dex" and x.get("pool_symbols"):
            ps = x["pool_symbols"]
            if c in ps:
                x["pair"] = [c, ps[1 - ps.index(c)]]
            elif loan in [_loose(p) for p in ps]:
                i = [_loose(p) for p in ps].index(loan)
                x["pair"] = [ps[i], ps[1 - i]]
            else:
                x["pair"] = list(ps)
            x["_in"] = x["pair"][0]
        else:
            pr = x.get("pair") or [None, None]
            x["_in"], x["_out"] = pr[0], pr[1]
    for x in legs:
        x.pop("fixed_side", None)
    ordered = _order_legs(legs, c)
    chain = []
    cur = c
    for x in ordered:
        inp = x.get("_in")
        if inp is None and x.get("kind") == "rate" and \
                x.get("role") != "vault_conversion":
            inp = cur
        if inp != cur:
            break
        chain.append(x)
        if x.get("kind") in ("rate", "composite_rate") and x.get("_out"):
            cur = x["_out"]
        else:
            break
    for x in legs:
        off = all(x is not y for y in chain)
        x["side"] = "quote" if off and x.get("kind") != "rate" and (
            (x.get("_in") and _loose(x["_in"]) == loan and loan != c) or
            (x.get("_out") and x.get("_out") == c and x.get("_in") != c)) \
            else "base"
    groups = {}
    for x in legs:
        if x.get("_via"):
            groups.setdefault(x["_via"], set()).add(x["side"])
    quotes = sorted(v for v, sd in groups.items() if sd == {"base", "quote"})
    if len(quotes) >= 2:
        for x in legs:
            for k in ("_in", "_out", "_via"):
                x.pop(k, None)
        ev["legs"] = legs
        return ("unresolved", "trace_aggregate", None,
                head + [f"{fn} combines {len(quotes)} quotes of the "
                        "collateral in the loan token, from "
                        + ", ".join(quotes)
                        + "; the trace does not show how (median, minimum "
                        "or other)"], ev)
    base = [x for x in ordered if x["side"] == "base"]
    quote = [x for x in legs if x["side"] == "quote"]
    leaf = lambda x: (x.get("address"), x.get("feed_id"),
                      x.get("description"), x.get("kind"))
    for b in list(base):
        q = next((y for y in quote if leaf(y) == leaf(b)), None)
        if q is not None and b.get("kind") in ("market", "rate", "dex"):
            base = [y for y in base if y is not b]
            quote = [y for y in quote if y is not q]
            notes.append(f"{b.get('description') or b.get('address')} is "
                         "read on both sides of the price and cancels")
            ev["trace"]["notes"] = notes
    legs = base + quote
    for x in legs:
        for k in ("_in", "_out", "_via"):
            x.pop(k, None)
    ev["legs"] = legs
    if not legs:
        t, sub, blind, why = classify_no_base([], coll)
        return t, sub, blind, head + notes_cancel(ev) + why, ev
    if not base:
        t, sub, blind, why = classify_no_base(legs, coll)
    else:
        t, sub, blind, why = classify_side(base, coll, vsym)
    if any(x.get("role") == "multiplier" for x in base) and blind and \
            blind == f"a discount of {c} against {c}":
        blind = f"a discount of {c} against the {c} shares it represents"
    return t, sub, blind, head + notes_cancel(ev) + why, ev


def notes_cancel(ev):
    return [n for n in ev.get("trace", {}).get("notes", [])
            if "cancels" in n]


LRD_SEL = "0xfeaf968c"
LOOSE_QUOTES = {"USD", "ETH", "BTC", "SOL", "EUR", "USDC", "USDT", "BNB"}
_STORK = {}


def stork_ids():
    """keccak256(name) -> name for Stork asset ids."""
    if not _STORK:
        from eth_utils import keccak
        bases = set()
        for line in (DATA / "prices" / "tokens.jsonl").open():
            sym = (json.loads(line).get("symbol") or "").upper()
            if sym.isascii() and sym:
                bases.add(sym.replace(" ", ""))
        for m in pyth_symbols().values():
            pr = desc_pair((m.get("symbol") or "").split(".", 1)[-1]
                           .removesuffix(".RR").split(".")[-1])
            if pr:
                bases.add(pr[0])
        for b in bases:
            for q in LOOSE_QUOTES:
                _STORK["0x" + keccak(text=b + q).hex()] = (b, q)
    return _STORK


def loose_pair(desc):
    """(BASE, QUOTE) from a description DESC_PAIR cannot split."""
    d = (desc or "").strip()
    for pre in ("Robinhood ", "Coinbase "):
        if d.startswith(pre):
            rest = d[len(pre):].strip()
            if pre == "Coinbase " and re.fullmatch(r"[A-Z0-9.]{1,8}", rest):
                return norm_sym(rest), None
            d = rest
    m = re.fullmatch(r"(.+)-([A-Za-z]{3,4})", d)
    if m and m.group(2).upper() in LOOSE_QUOTES:
        return norm_sym(m.group(1)), norm_sym(m.group(2))
    return None


CHAINLINK_AGGREGATORS = {"DualAggregator", "AccessControlledOCR2Aggregator",
                         "AccessControlledOffchainAggregator",
                         "OffchainAggregator", "OCR2Aggregator"}


def aggregator_feed(rs, node):
    """A Chainlink aggregator."""
    addr = node["address"]
    desc = node.get("description")
    if not desc:
        for p in sorted(rs.probes.rows.values(), key=lambda p: p["address"]):
            if p.get("aggregator") == addr and p.get("description"):
                desc = p["description"]
                break
    out = {**node, "kind": "market", "kind_basis": "Chainlink aggregator",
           "description": desc}
    pair = desc_pair(desc) or loose_pair(desc)
    if pair:
        out["pair"] = rs.native_pair(list(pair))
    elif not desc:
        out.update(kind="unknown", reason="Chainlink aggregator read "
                   "directly; no cached proxy names its asset")
    return out


def feed_asset(rs, node):
    """Name the asset of a market feed whose description names none, from its identifier getters."""
    addr = node["address"]
    p = rs.probes.get(addr) or {}
    desc = node.get("description") or ""
    name = node.get("contract") or ""
    out = dict(node)
    if "calculated" in desc.lower():
        out.update(kind="unknown", calculated=True,
                   reason=f"Chainlink calculated feed '{desc}': a derived "
                          "value whose inputs are not on chain")
        return out
    pair, basis, kind = None, None, "market"
    if "Api3ReaderProxy" in name:
        got = cached_calls(rs.target, rs.cid,
                           [(addr, evmabi.selector("dapiName()"))])[0]
        dn = bytes32_text(got["ret"][:66]) if got["ok"] and \
            len(got["ret"]) >= 66 else None
        if desc_pair(dn):
            out.update(kind="market", description=f"Api3 {dn}", feed_id=dn,
                       pair=rs.native_pair(list(desc_pair(dn))))
        return out
    elif p.get("price_id") and "pyth" in desc.lower():
        m = pyth_symbols().get(p["price_id"][2:].lower())
        if m:
            sym = m["symbol"] or ""
            pair = desc_pair(sym.split(".", 1)[-1].removesuffix(".RR")
                             .split(".")[-1])
            basis = f"Pyth price id {p['price_id'][:10]}... = {sym}"
            if sym.endswith(".RR") or any(
                    w in (m["asset_type"] or "").lower()
                    for w in ("rate", "nav")):
                kind = "rate"
    elif p.get("price_id") and "stork" in desc.lower():
        hit = stork_ids().get(p["price_id"].lower())
        if hit:
            pair, basis = (norm_sym(hit[0]), norm_sym(hit[1])), \
                f"Stork asset id {p['price_id'][:10]}... = " \
                f"keccak('{hit[0]}{hit[1]}')"
    else:
        pair = loose_pair(desc)
        basis = "description" if pair else None
    if pair:
        out.update(pair=rs.native_pair(list(pair)), kind=kind,
                   asset_basis=basis)
    return out


def feed_trace(rs, addr, node, depth=0):
    """A feed no getter identifies, classified from what one latestRoundData() call reads (cached trace)."""
    tr = trace_call(rs.target, rs.cid, addr, data=LRD_SEL)
    f = tr.get("result") or {}
    base = {k: v for k, v in node.items() if k not in ("kind", "reason")}
    base["trace"] = {"block": tr.get("block"), "data": LRD_SEL,
                     "calls": _summary(f)}
    if tr.get("rpc_error") or f.get("error"):
        hist = retry_trace(rs, addr, LRD_SEL, feed=True)
        if not hist:
            return {**node, "trace": base["trace"], "kind": "unknown",
                    "reason": (node.get("reason") or "names no asset")
                    + "; latestRoundData() reverts in a trace at block "
                    + str(tr.get("block"))}
        tr, f = hist, hist["result"]
        base["trace"] = {"block": tr["block"], "data": LRD_SEL,
                         "from": tr.get("from"),
                         "historical": bool(tr.get("historical")),
                         "calls": _summary(f),
                         "note": _retry_note(tr, "latestRoundData()")}
        reads = _frame_reads(f)
        if len(reads) == 1 and reads[0]["input"][:10] == LRD_SEL:
            got = call_from(rs, addr, evmabi.selector("description()"),
                            tr.get("from"), tr["block"] if
                            tr.get("historical") else "latest")
            try:
                desc = evmabi.decode(["string"], got)[0] if got else None
            except Exception:
                desc = None
            pair = desc_pair(desc) or loose_pair(desc)
            if desc and "calculated" in desc.lower():
                pair = None
            if pair:
                rate = any(w in desc.lower() for w in RATE_WORDS)
                return {**base, "kind": "rate" if rate else "market",
                        "description": desc,
                        "pair": rs.native_pair(list(pair)),
                        "kind_basis": "feed description, read from "
                                      f"{tr.get('from')}"}
            return {**node, "trace": base["trace"], "kind": "unknown",
                    "reason": f"Chainlink proxy readable only by its "
                              f"consumer; description {desc!r} names no "
                              "asset"}
    legs, checks, notes = trace_legs(rs, addr, addr, f, depth + 1)
    if notes:
        base["trace"]["notes"] = notes
    if checks:
        base["trace"]["checks"] = sorted(set(checks))
    if not legs:
        st = _storage_reads(rs, addr, addr, LRD_SEL)
        return {**base, "kind": "stored" if st else "constant",
                "kind_basis": "trace: no external call, "
                              + ("reads storage" if st else
                                 "reads no storage")}
    unknown = [x for x in legs if x.get("kind") in ("unknown", None)]
    if unknown:
        return {**base, "kind": "unknown", "kind_basis": "trace",
                "legs": legs,
                "reason": "; ".join(x.get("reason") or "kind not identified"
                                    for x in unknown)}
    for x in legs:
        x.setdefault("role", x.get("selector") or "read")
        x["side"] = "base"
        if x.get("role") == "vault_conversion":
            a = x.get("vault_asset")
            vs = rs.token_symbols([x["address"], a] if a else
                                  [x["address"]])
            x["pair"] = [norm_sym(rs.match_sym(x["address"],
                                               vs.get(x["address"]))),
                         norm_sym(rs.match_sym(a, vs.get(a))) if a else None]
            x["kind"], x["role"] = "rate", "vault_rate"
            x["description"] = f"vault conversion {x['address']}"
    if len(legs) == 1:
        x = legs[0]
        return {**base, **{k: x[k] for k in ("kind", "pair", "feed_id",
                                             "pyth_state") if k in x},
                "description": x.get("description") or node.get(
                    "description"),
                "kind_basis": f"trace: reads only {x.get('selector')} on "
                              f"{x.get('address')}"}
    return {**base, "kind": "composite", "kind_basis": "trace", "legs": legs}


def _order_from(legs, coll):
    """Legs of a traced composite feed in conversion order from coll."""
    tagged = []
    for x in legs:
        pr = x.get("pair") or [None, None]
        tagged.append({**x, "_in": pr[0], "_out": pr[1]})
    out = []
    for x in _order_legs(tagged, coll):
        x.pop("_in", None)
        x.pop("_out", None)
        out.append(x)
    return out


def vault_assets(target, vaults):
    res = multicall.try_view(target, sorted(vaults), "asset()", ["address"])
    return {v: (r[0].lower() if r else None)
            for v, r in zip(sorted(vaults), res)}


def classify_no_base(legs, coll_sym):
    """No base feed and no base vault."""
    quote = [x for x in legs if x["side"] == "quote"
             and x.get("role") != "vault_conversion"]
    num = (quote[-1].get("pair") or [None, None])[1] if quote else None
    if num and norm_sym(coll_sym) == num:
        return ("tracks_market", "collateral_is_numeraire", None,
                [f"no base input; the collateral {coll_sym} is the numeraire "
                 f"of quote feed {quote[-1]['address']} "
                 f"'{quote[-1].get('description')}', so the loan is priced "
                 "in the collateral by a market feed"])
    peg = num or "one unit of the loan token"
    return ("fixed_price", "no_collateral_input",
            f"a depeg of {coll_sym} against {peg}",
            [f"no base feed and no base vault: one {coll_sym} is taken as "
             f"{peg} (SCALE_FACTOR"
             + (f" / quote feed {quote[-1]['address']})" if quote else ")")])


def classify_side(legs, coll_sym, vault_asset_sym):
    """Type of the collateral side from its ordered legs."""
    flat = []
    for lg in legs:
        if lg.get("kind") == "composite":
            sub = [x for x in lg["legs"] if x["side"] == "base"]
            if lg.get("kind_basis") == "trace":
                sub = _order_from(sub, norm_sym(coll_sym))
            flat.extend(sub)
            continue
        flat.append(lg)
    if not flat:
        return ("fixed_price", "no_collateral_input",
                f"any move in the price of {coll_sym}",
                ["no base feed and no base vault: the collateral side of "
                 "price() is the constant SCALE_FACTOR"])
    unknown = [x for x in flat if x.get("kind") in ("unknown", None)
               and x.get("role") != "vault_conversion"]
    if unknown:
        return ("unresolved", "unidentified_component", None,
                [f"{x['role']} {x['address']} ({x.get('contract')}): "
                 f"{x.get('reason') or 'kind not identified'}"
                 for x in unknown])
    kinds = [("rate" if x.get("role") == "vault_conversion" else x["kind"])
             for x in flat]
    if "stored" in kinds:
        x = flat[kinds.index("stored")]
        return ("other", "stored_price",
                f"any move of {coll_sym} the setter does not post",
                [f"{x.get('contract')} {x['address']} returns a value from "
                 "its own storage, written by an owner or keeper: constant "
                 "unless someone updates it"])
    if all(k == "constant" for k in kinds):
        return ("fixed_price", "constant_feed",
                f"any move in the price of {coll_sym}",
                ["every collateral-side feed is a constant: "
                 + ", ".join(f"{x.get('contract')} {x['address']}"
                             for x in flat)])
    if "min" in kinds:
        return ("min_of_two", None, None,
                ["a collateral-side feed takes the lower of two sources"])
    if "clamp" in kinds:
        x = flat[kinds.index("clamp")]
        return ("other", "clamped_market_feed",
                f"a move of {coll_sym} outside the clamp bounds",
                [f"{x.get('contract')} {x['address']} clamps a market price "
                 "between constant bounds"])
    coll = norm_sym(coll_sym)
    current = {coll}
    convs, reasons, pegged = [], [], None
    for x, k in zip(flat, kinds):
        cur = next(iter(current)) if current else None
        if k == "rate_then_market":
            return ("exchange_rate", "underlying_at_own_feed",
                    f"a discount of {coll_sym} against its underlying",
                    reasons + [f"{x.get('contract')} {x['address']}: share "
                               "rate times the underlying's price from "
                               "ResilientOracle.peek(underlying)"])
        if k in ("rate", "composite_rate"):
            if x.get("role") == "vault_conversion":
                out = norm_sym(vault_asset_sym.get(x["address"])) or None
                reasons.append(f"vault conversion {x['address']} "
                               f"({cur} -> {out})")
            else:
                pr = x.get("pair") or [None, None]
                out = pr[1] or (pt_underlying(cur) if cur else None)
                reasons.append(f"rate leg {x['address']} "
                               f"'{x.get('description') or x.get('contract')}'"
                               f" ({cur} -> {out or 'reported price'})")
            convs.append(("rate", cur, out))
            if out:
                current = {out}
            continue
        if k == "pt":
            out = norm_sym(x.get("underlying_symbol")) or \
                (x.get("pair") or [None, None])[1] or pt_underlying(cur)
            if x.get("implicit_share_rate"):
                ytok = norm_sym(x.get("yield_token_symbol"))
                reasons.append(f"Pendle PT rate {x['address']} "
                               "(PT_TO_ASSET): discount of "
                               f"{cur} against {ytok}, then {ytok} -> {out} "
                               "by the SY's share rate")
                convs += [("pt", cur, ytok), ("rate", ytok, out)]
            else:
                reasons.append(f"Pendle PT rate {x['address']} "
                               f"({x.get('pendle_oracle_type')}): discount "
                               f"of {cur} against {out}")
                convs.append(("pt", cur, out))
            current = {out} if out else set()
            continue
        if k == "constant":
            reasons.append(f"constant leg {x['address']} ({x.get('contract')})")
            pr = x.get("pair")
            if pr and pr[1]:
                current, pegged = {pr[1]}, pr[1]
            continue
        pr = x.get("pair")
        priced = pr[0] if pr else None
        if k == "dex":
            priced = priced or cur
        if priced is None:
            return ("unresolved", "market_feed_asset_unknown", None,
                    reasons + [f"market leg {x['address']} "
                               f"({x.get('contract')}, '{x.get('description')}')"
                               " names no asset"])
        match = priced in current
        why = reasons + [f"{k} leg {x['address']} "
                         f"'{x.get('description') or x.get('contract')}' "
                         f"prices {priced}"]
        rates = [c for c in convs if c[0] == "rate"]
        if pegged and match:
            return ("fixed_price", "fixed_peg_then_market_feed",
                    f"a depeg of {coll_sym} against {pegged}",
                    why + [f"{coll_sym} is converted to {pegged} at a "
                           "constant"])
        if not convs:
            if match:
                return "tracks_market", "direct", None, why
            return ("other", "proxy_feed_other_asset",
                    f"a depeg of {coll_sym} against {priced}",
                    why + [f"{coll_sym} is priced by a feed of {priced}"])
        if not rates:
            under = convs[-1][2]
            if match:
                return ("tracks_market", "pt_discount_then_underlying_market",
                        None, why)
            return ("exchange_rate", "pt_underlying_at_other_feed",
                    f"a depeg of {under} against {priced}",
                    why + [f"the PT's underlying {under} is taken at a feed "
                           f"of {priced}"])
        first = rates[0][1]
        return ("exchange_rate",
                "underlying_at_own_feed" if match
                else "underlying_at_other_feed",
                f"a discount of {first} against {priced}",
                why + [f"{first} is priced only through a rate; its own "
                       "market price is never read"])
    rates = [c for c in convs if c[0] == "rate"]
    if not rates:
        under = convs[-1][2] if convs else None
        return ("exchange_rate", "pt_underlying_at_par",
                f"a depeg of {under or 'the PT underlying'}",
                reasons + [f"no market leg after the PT rate: {under} is "
                           "taken at par"])
    first, last_in, out = rates[0][1], convs[-1][1], convs[-1][2]
    if out is None or out in FIAT:
        blind = (f"a discount of {first} against its reported price"
                 if last_in == first else
                 f"a discount of {first} against {last_in} and of "
                 f"{last_in} against its reported price")
        return ("exchange_rate", "issuer_reported_price", blind,
                reasons + ["the last leg is a rate the issuer or its oracle "
                           "reports; no market price is read"])
    return ("exchange_rate", "underlying_at_par",
            f"a discount of {first} against {out} and a depeg of {out}",
            reasons + [f"no market leg after the conversion: {out} is "
                       "taken at par"])


def forward_targets(rs, addrs):
    """For forwarding oracles (FORWARDERS), the oracles they forward to, read from their getters."""
    calls, keys = [], []
    for a in sorted(addrs):
        name = rs.source(a)[0]
        for g in FORWARDERS.get(name, ()):
            calls.append((a, evmabi.selector(g)))
            keys.append((a, g))
    got = _addr_call(rs.target, calls)
    out = {}
    for (a, g), t in zip(keys, got):
        out.setdefault(a, {})[g] = t
    return out


def oracle_vaults(rs, oracles):
    vaults = set()
    for a in oracles:
        p = rs.probes.get(a) or {}
        for k in ("base_vault", "quote_vault", "vault"):
            if nz(p.get(k)):
                vaults.add(p[k])
    return vault_assets(rs.target, vaults) if vaults else {}


def classify_oracle(rs, o, row, vasset, vsym, fwd, depth=0):
    """(type, subtype, blind_to, reasons, evidence) of one Morpho-style market oracle, following forwarding oracles to the one in use."""
    p = rs.probes.get(o) or {}
    name, verified, text, rep = rs.source(o)
    ev = {"address": o, "contract": name, "verified": verified,
          "scale_factor": p.get("scale_factor")}
    if o in rs.via:
        ev["source_via"] = rs.via[o]
    coll = rs.match_sym(row["collateral_token"],
                        row["collateral_symbol"] or row["collateral_token"])
    if o in fwd and depth < 3:
        ev["forwards_to"] = fwd[o]
        cur = next(iter(fwd[o].values()))
        if not cur:
            return ("unresolved", "forwarder_target_unreadable", None,
                    [f"{name} getters returned nothing"], ev)
        t, sub, blind, why, sub_ev = classify_oracle(rs, cur, row, vasset,
                                                     vsym, fwd, depth + 1)
        ev["current"] = sub_ev
        why = [f"{name} forwards price() to {cur}"] + why
        others = {g: x for g, x in fwd[o].items() if x and x != cur}
        for g, x in others.items():
            t2 = classify_oracle(rs, x, row, vasset, vsym, fwd, depth + 1)
            ev.setdefault("alternatives", {})[g] = {"address": x,
                                                    "type": t2[0],
                                                    "subtype": t2[1]}
            why.append(f"{g} {x} would be {t2[0]}")
        return t, sub, blind, why, ev
    if Resolver.is_morpho_composite(p, name) and (name or rs.can_call(o)):
        legs = rs.morpho_legs(o, p)
        for lg in legs:
            if lg.get("role") == "vault_conversion":
                lg["vault_asset"] = vasset.get(lg["address"])
                lg["vault_asset_symbol"] = vsym.get(lg["address"])
        ev["legs"] = legs
        qaddr = {x.get("address") for x in legs if x["side"] == "quote"
                 and x.get("role") != "vault_conversion"}
        both = {x.get("address") for x in legs if x["side"] == "base"
                and x.get("role") != "vault_conversion"} & qaddr
        if both:
            ev["cancelling_feeds"] = sorted(both)
            legs = [x for x in legs if x.get("address") not in both]
        base = [x for x in legs if x["side"] == "base"]
        if not base:
            t, sub, blind, why = classify_no_base(legs, coll)
        else:
            t, sub, blind, why = classify_side(base, coll, vsym)
        if both:
            why = [f"feed {', '.join(sorted(both))} is read on both the base "
                   "and the quote side and cancels"] + why
        return t, sub, blind, why, ev
    leaf = rs.feed(o, trace=False)
    ev.update({k: v for k, v in leaf.items() if k != "address"})
    if leaf.get("kind") == "unknown" and \
            leaf.get("reason") in ("unverified", "no source"):
        got = trace_classify(rs, o, row, coll)
        if got:
            t, sub, blind, why, tev = got
            ev.update(tev)
            ev.pop("kind", None)
            ev.pop("reason", None)
            return t, sub, blind, why, ev
    t, sub, blind, why = classify_single(leaf, row, coll)
    if t == "unresolved":
        got = trace_classify(rs, o, row, coll)
        if got:
            t2, sub2, blind2, why2, tev = got
            ev.update(tev)
            return (t2, sub2, blind2,
                    why2 if t2 != "unresolved" else why + why2, ev)
    return t, sub, blind, why, ev


def classify_morpho(rs, rows, protocol):
    """Morpho markets: each names one oracle with price() in loan units per collateral unit, scaled by 1e36."""
    oracles = {r["oracle"] for r in rows if nz(r["oracle"])}
    rs.prepare(oracles)
    fwd = forward_targets(rs, oracles)
    inner = {t for d in fwd.values() for t in d.values() if t}
    if inner:
        rs.prepare(inner)
        fwd.update(forward_targets(rs, inner))
    vasset = oracle_vaults(rs, oracles | inner)
    syms = rs.token_symbols([r["collateral_token"] for r in rows]
                            + [r["loan_token"] for r in rows]
                            + [x for x in vasset.values() if x])
    vsym = {v: rs.match_sym(a, syms.get(a)) for v, a in vasset.items()}
    out = []
    core = core_address(protocol, rs.cid)
    for r in rows:
        o = r["oracle"]
        set_trace_context(rs, protocol, r["market_id"], [o, core])
        row = {"protocol": protocol, "chain_id": rs.cid,
               "market_id": r["market_id"], "oracle": o,
               "collateral_token": r["collateral_token"],
               "collateral_symbol": syms.get(r["collateral_token"]),
               "loan_token": r["loan_token"],
               "loan_symbol": syms.get(r["loan_token"]),
               "lltv": r["lltv"]}
        if not nz(o):
            row.update(type="none", subtype=None, blind_to=None,
                       reasons=["oracle is the zero address"], evidence=None)
            out.append(row)
            continue
        t, sub, blind, why, ev = classify_oracle(rs, o, row, vasset, vsym,
                                                 fwd)
        row.update(type=t, subtype=sub, blind_to=blind, reasons=why,
                   evidence=ev)
        out.append(row)
    return out


def classify_single(leaf, row, coll):
    """An oracle that is not a Morpho-style composite."""
    k = leaf.get("kind")
    if k == "stored":
        return ("other", "stored_price",
                f"any move of {row['collateral_symbol']} the setter does "
                "not post",
                [f"{leaf.get('contract')}: price() reads only its own "
                 "storage, written by an owner or keeper"])
    if k == "constant":
        return ("fixed_price", "constant_contract",
                f"any move in the price of {row['collateral_symbol']}",
                [f"{leaf.get('contract')} returns a constant"])
    if k == "composite":
        return classify_side([x for x in leaf["legs"] if x["side"] == "base"],
                             coll or "", {})
    if k == "min":
        return ("min_of_two", None, None,
                [f"{leaf.get('contract')} takes a minimum of feeds"])
    if k == "unknown" and leaf.get("reason"):
        sub = {"unverified": "unverified", "no source": "no_source"}.get(
            leaf["reason"], "calls_outside_main_source")
        return ("unresolved", sub, None,
                [f"oracle {leaf['address']}: {leaf['reason']}"])
    return ("unresolved", f"custom_{k}", None,
            [f"custom oracle contract {leaf.get('contract')} "
             f"(signals {leaf.get('source_signals')}); not a Morpho-style "
             "composite, read its source to classify"])


TOKEN_CONFIG = ["(address,address[3],bool[3],uint256)"]


def classify_moolah(rs, rows):
    """Moolah markets. Most name Lista's ResilientOracle (or StockOracle, which forwards to it)."""
    oracles = {r["oracle"] for r in rows if nz(r["oracle"])}
    rs.prepare(oracles)
    keys = sorted({(r["oracle"], r["collateral_token"]) for r in rows
                   if nz(r["oracle"])})
    res = multicall.aggregate3(rs.target, [
        (o, evmabi.encode_call("getTokenConfig(address)", ["address"], [t]))
        for o, t in keys]) if keys else []
    cfg = {}
    for k, (ok, ret) in zip(keys, res):
        if ok and ret not in ("0x", ""):
            try:
                asset, feeds, flags, tol = evmabi.decode(TOKEN_CONFIG, ret)[0]
                cfg[k] = {"feeds": [f.lower() for f in feeds],
                          "enabled": list(flags), "tolerance": tol}
            except Exception:
                pass
    rs.prepare({f for c in cfg.values() for f in c["feeds"] if nz(f)})
    broker = {}
    names = {r["oracle"]: rs.source(r["oracle"])[0] for r in rows
             if nz(r["oracle"])}
    for r in rows:
        k = (r["oracle"], r["collateral_token"])
        if k in cfg or not nz(r["oracle"]):
            continue
        name = names[r["oracle"]]
        if name == "LendingBroker":
            inner = _addr_call(rs.target, [(r["oracle"],
                                            evmabi.selector("ORACLE()"))])[0]
            if inner:
                ok, ret = multicall.aggregate3(rs.target, [(inner, evmabi.encode_call(
                    "getTokenConfig(address)", ["address"],
                    [r["collateral_token"]]))])[0]
                if ok and ret not in ("0x", ""):
                    a, feeds, flags, tol = evmabi.decode(TOKEN_CONFIG, ret)[0]
                    cfg[k] = {"feeds": [x.lower() for x in feeds],
                              "enabled": list(flags), "tolerance": tol,
                              "broker": r["oracle"], "oracle": inner}
                    rs.prepare({x.lower() for x in feeds if nz(x)})
        elif name == "CreditBroker":
            broker[k] = name
    rest = [r for r in rows if (r["oracle"], r["collateral_token"]) not in cfg
            and (r["oracle"], r["collateral_token"]) not in broker]
    out = classify_morpho(rs, rest, "moolah") if rest else []
    syms0 = rs.token_symbols([r["collateral_token"] for r in rows]
                             + [r["loan_token"] for r in rows])
    for r in rows:
        if (r["oracle"], r["collateral_token"]) not in broker:
            continue
        cs = syms0.get(r["collateral_token"])
        out.append({"protocol": "moolah", "chain_id": rs.cid,
                    "market_id": r["market_id"], "oracle": r["oracle"],
                    "collateral_token": r["collateral_token"],
                    "collateral_symbol": cs,
                    "loan_token": r["loan_token"],
                    "loan_symbol": syms0.get(r["loan_token"]),
                    "lltv": r["lltv"], "type": "fixed_price",
                    "subtype": "constant_contract",
                    "blind_to": f"any move in the price of {cs}",
                    "reasons": ["CreditBroker.peek prices the collateral "
                                "credit token at CREDIT_TOKEN_PRICE, a "
                                "constant (source reviewed)"],
                    "evidence": {"address": r["oracle"],
                                 "contract": "CreditBroker",
                                 "kind_basis": "reviewed source"}})
    syms = rs.token_symbols([r["collateral_token"] for r in rows]
                            + [r["loan_token"] for r in rows])
    for r in rows:
        k = (r["oracle"], r["collateral_token"])
        if k not in cfg:
            continue
        c = cfg[k]
        row = {"protocol": "moolah", "chain_id": rs.cid,
               "market_id": r["market_id"], "oracle": r["oracle"],
               "collateral_token": r["collateral_token"],
               "collateral_symbol": syms.get(r["collateral_token"]),
               "loan_token": r["loan_token"],
               "loan_symbol": syms.get(r["loan_token"]), "lltv": r["lltv"]}
        name = names.get(r["oracle"]) or rs.source(r["oracle"])[0]
        if c.get("broker"):
            name = f"LendingBroker -> {rs.source(c['oracle'])[0]}"
        roles = dict(zip(("main", "pivot", "fallback"), c["feeds"]))
        set_trace_context(rs, "moolah", r["market_id"],
                          [c.get("oracle") or r["oracle"],
                           core_address("moolah", rs.cid)])
        legs = {role: rs.feed(f) for role, f in roles.items() if nz(f)}
        ev = {"address": r["oracle"], "contract": name,
              "token_config": {"feeds": roles, "enabled": c["enabled"],
                               "tolerance": c["tolerance"]},
              "legs": legs}
        if "main" not in legs:
            row.update(type="unresolved", subtype="no_main_feed",
                       blind_to=None, evidence=ev,
                       reasons=["getTokenConfig names no main feed for the "
                                "collateral"])
            out.append(row)
            continue
        main = {"side": "base", "role": "main_feed", **legs["main"]}
        t, sub, blind, why = classify_side(
            [main], rs.match_sym(r["collateral_token"],
                                 row["collateral_symbol"]
                                 or r["collateral_token"]), {})
        why = [f"{name}.getTokenConfig(collateral) main feed "
               f"{roles['main']}"] + why
        if c.get("broker"):
            why.insert(0, "LendingBroker.peek returns ORACLE().peek("
                       "collateral) less the broker's accrued interest")
        for role in ("pivot", "fallback"):
            if role in legs:
                why.append(f"{role} feed {roles[role]} "
                           f"({legs[role].get('contract')}, "
                           f"'{legs[role].get('description')}', "
                           f"kind {legs[role].get('kind')})")
        row.update(type=t, subtype=sub, blind_to=blind, reasons=why,
                   evidence=ev)
        out.append(row)
    return out


SILO_WORDS = ["dao_fee", "deployer_fee", "silo", "token",
              "protected_share_token", "collateral_share_token",
              "debt_share_token", "solvency_oracle", "max_ltv_oracle",
              "interest_rate_model", "max_ltv", "lt",
              "liquidation_target_ltv", "liquidation_fee", "flashloan_fee",
              "hook_receiver", "call_before_quote"]
SILO_ADDR = {"silo", "token", "solvency_oracle", "max_ltv_oracle"}
SILO_KIND = {"OracleScaler": "constant", "ERC4626Oracle": "rate",
             "ERC4626OracleHardcodeQuote": "rate",
             "PTLinearOracle": "rate"}


def silo_configs(rs, rows):
    out = {}
    calls = [(r["silo_config"], evmabi.encode_call(
        "getConfig(address)", ["address"], [r["silo"]])) for r in rows]
    res = multicall.aggregate3(rs.target, calls, chunk=100) if calls else []
    for r, (ok, ret) in zip(rows, res):
        words = ([ret[2 + 64 * k:2 + 64 * (k + 1)]
                  for k in range((len(ret) - 2) // 64)] if ok else [])
        layout = (SILO_WORDS if len(words) >= 17 else
                  [f for f in SILO_WORDS if f != "liquidation_target_ltv"]
                  if len(words) == 16 else None)
        if layout is None or words[2][-40:] != r["silo"][2:]:
            out[r["silo"]] = None
            continue
        out[r["silo"]] = {f: ("0x" + words[k][-40:] if f in SILO_ADDR
                              else int(words[k], 16))
                          for k, f in enumerate(layout)
                          if f in SILO_ADDR or f in ("max_ltv", "lt")}
    return out


CL3_CONFIG = ["address", "address", "uint256", "uint256", "uint256",
              "uint256", "address", "address", "bool", "bool"]


def silo_oracle(rs, o, coll_sym, depth=0):
    """Type of one Silo oracle (ISiloOracle.quote in quoteToken units)."""
    if not nz(o):
        return ("none", "no_oracle", None,
                ["oracle is zero: Silo values the token 1:1 in quote "
                 "units, so the token must be the market's quote token"],
                None)
    name, verified, text, rep = rs.source(o)
    ev = {"address": o, "contract": name, "verified": verified,
          "source_signals": signals(text) if text else []}
    if o in rs.via:
        ev["source_via"] = rs.via[o]
    kind = SILO_KIND.get(name)
    if name == "ManageableOracle" and depth < 3:
        inner = _addr_call(rs.target, [(o, evmabi.selector("oracle()"))])[0]
        if inner:
            rs.prepare([inner])
            t, sub, blind, why, sub_ev = silo_oracle(rs, inner, coll_sym,
                                                     depth + 1)
            ev["forwards_to"] = sub_ev
            return (t, sub, blind,
                    [f"ManageableOracle forwards quote() to {inner}"] + why,
                    ev)
    if name == "ChainlinkV3Oracle":
        cfg_addr = _addr_call(rs.target,
                              [(o, evmabi.selector("oracleConfig()"))])[0]
        ok, ret = multicall.aggregate3(rs.target, [
            (cfg_addr, evmabi.selector("getConfig()"))])[0] if cfg_addr \
            else (False, "0x")
        if ok and ret not in ("0x", ""):
            words = (len(ret) - 2) // 64
            try:
                if words not in (9, 10) or (len(ret) - 2) % 64:
                    raise ValueError("unsupported config length")
                c = evmabi.decode(CL3_CONFIG[:words], ret)
            except (DecodingError, ValueError):
                ev.update(config=cfg_addr, config_words=words)
                return ("unresolved", "config_unreadable", None,
                        ["ChainlinkV3Oracle.getConfig returned an "
                         "unsupported or malformed layout"], ev)
            prim, sec = c[0].lower(), c[1].lower()
            invert = c[9] if words == 10 else True
            ev["config_words"] = words
            rs.prepare([x for x in (prim, sec) if nz(x)])
            legs = [{"side": "base", "role": "primary_aggregator",
                     **rs.feed(prim)}]
            if nz(sec):
                leg = {"side": "base", "role": "secondary_aggregator",
                       **rs.feed(sec), "inverted": invert}
                if invert and leg.get("pair"):
                    leg["pair"] = list(reversed(leg["pair"]))
                legs.append(leg)
            ev.update(config=cfg_addr, legs=legs)
            t, sub, blind, why = classify_side(legs, coll_sym, {})
            return t, sub, blind, why, ev
    if kind == "constant":
        return ("fixed_price", "decimal_scaler",
                f"any move in the price of {coll_sym}",
                [f"{name}: quote() rescales decimals only"], ev)
    if kind == "rate":
        under = pt_underlying(norm_sym(coll_sym)) or "its underlying"
        return ("exchange_rate", "underlying_at_par",
                f"a discount of {coll_sym} against {under} and a depeg of "
                f"{under}",
                [f"{name}: quote() is a rate ({'linear PT discount' if name == 'PTLinearOracle' else 'share rate'}) "
                 "with the underlying at par"], ev)
    if name == "ERC4626OracleWithUnderlying":
        return ("exchange_rate", "underlying_at_nested_oracle",
                f"a discount of {coll_sym} against its vault asset",
                [f"{name}: quote() is the vault's share rate times another "
                 "Silo oracle's quote of the vault asset; the collateral's "
                 "own market price is never read"], ev)
    if name == "PendlePTOracle" and depth < 3:
        uo, ut = _addr_call(rs.target, [
            (o, evmabi.selector("UNDERLYING_ORACLE()")),
            (o, evmabi.selector("PT_UNDERLYING_TOKEN()"))])
        usym = rs.token_symbols([ut]).get(ut) if ut else None
        if uo:
            rs.prepare([uo])
            tok0, rs.silo_token = getattr(rs, "silo_token", None), ut
            t, sub, blind, why, sub_ev = silo_oracle(
                rs, uo, rs.match_sym(ut, usym), depth + 1)
            rs.silo_token = tok0
            ev.update(underlying_token=ut, underlying_symbol=usym,
                      underlying_oracle=sub_ev)
            why = [f"PendlePTOracle: Pendle PT-to-SY TWAP rate of "
                   f"{coll_sym} against {usym}, times {usym} from "
                   f"{uo}"] + why
            if t == "tracks_market" and sub == "direct":
                return (t, "pt_discount_then_underlying_market", None,
                        why, ev)
            if t == "tracks_market" or t == "none":
                return ("tracks_market", "pt_discount_underlying_is_numeraire",
                        None, why, ev)
            if t in ("fixed_price", "exchange_rate"):
                return ("exchange_rate",
                        "pt_underlying_at_par" if t == "fixed_price"
                        else f"pt_then_{sub}",
                        f"a depeg of {usym}" + (f"; {blind}" if blind and
                                                t == "exchange_rate" else ""),
                        why, ev)
            return t, sub, blind, why, ev
    tok = getattr(rs, "silo_token", None)
    qt = _addr_call(rs.target, [(o, evmabi.selector("quoteToken()"))])[0]
    if nz(tok) and depth < 4:
        data = evmabi.encode_call("quote(uint256,address)",
                                  ["uint256", "address"],
                                  [_unit_amount(rs, tok), tok])
        tr = trace_call(rs.target, rs.cid, o, data=data)
        f = tr.get("result") or {}
        ev["trace"] = {"block": tr.get("block"), "data": data[:10],
                       "calls": _summary(f)}
        why0 = [f"Silo oracle contract {name!r} not yet reviewed"]
        hist = retry_trace(rs, o, data) if (tr.get("rpc_error") or
                                            f.get("error")) else None
        if hist:
            ev["trace"] = {"block": hist["block"], "data": data[:10],
                           "historical": True, "head_block": tr.get("block"),
                           "calls": _summary(hist["result"])}
            tr, f = hist, hist["result"]
        if tr.get("rpc_error") or f.get("error"):
            return ("unresolved", "trace_reverts", None, why0 + [
                f"quote() of {o} reverts at block {tr.get('block')} "
                f"({f.get('error') or tr.get('rpc_error')}, "
                f"{(f.get('output') or '')[:10]})"], ev)
        qs = rs.token_symbols([qt]).get(qt) if qt else None
        t, sub, blind, why, ev = trace_verdict(
            rs, o, f, ev, [_retry_note(tr, "quote()") if tr.get("historical")
                           else f"quote() trace at block {tr['block']}"],
            coll_sym, qt, qs, coll_sym, tr["block"], data)
        return t, sub, blind, (why if t != "unresolved" else why0 + why), ev
    return ("unresolved", "silo_oracle_not_reviewed", None,
            [f"Silo oracle contract {name!r} not yet reviewed"], ev)


def classify_silo(rs, rows):
    cfgs = silo_configs(rs, rows)
    oracles = {c[k] for c in cfgs.values() if c
               for k in ("solvency_oracle", "max_ltv_oracle") if nz(c[k])}
    rs.prepare(oracles)
    syms = rs.token_symbols([r["token"] for r in rows])
    out = []
    for r in rows:
        c = cfgs.get(r["silo"])
        row = {"protocol": "silo_v2", "chain_id": rs.cid, "silo": r["silo"],
               "silo_config": r["silo_config"], "token": r["token"],
               "token_symbol": syms.get(r["token"])}
        if not c:
            row.update(type="unresolved", subtype="config_unreadable",
                       reasons=["getConfig failed its self-check"])
            out.append(row)
            continue
        row.update(max_ltv=str(c["max_ltv"]), lt=str(c["lt"]))
        rs.silo_token = r["token"]
        for role in ("solvency_oracle", "max_ltv_oracle"):
            set_trace_context(rs, "silo_v2", r["silo"], [c[role], r["silo"]])
            t, sub, blind, why, ev = silo_oracle(
                rs, c[role], rs.match_sym(r["token"], row["token_symbol"]))
            row[role] = {"address": c[role], "type": t, "subtype": sub,
                         "blind_to": blind, "reasons": why, "evidence": ev}
        row["type"] = row["solvency_oracle"]["type"]
        out.append(row)
    return out


EULER_KIND = {
    "ChainlinkOracle": "market", "ChainlinkInfrequentOracle": "market",
    "ChainlinkInfrequentNanosecondOracle": "market",
    "RedstoneCoreOracle": "market", "PythOracle": "market",
    "ChronicleOracle": "market", "StorkChainlinkOracle": "market",
    "UniswapV3Oracle": "dex", "PendleOracle": "pt",
    "PendleUniversalOracle": "pt", "USDatCurveEMAOracle": "dex",
    "CurveEMAOracle": "dex",
    "FixedRateOracle": "constant",
    "RateProviderOracle": "rate", "LidoOracle": "rate",
    "LidoFundamentalOracle": "rate", "VedaAccountantOracle": "rate",
    "IdleTranchesOracle": "rate", "LiminalNAVOracle": "rate",
    "ERC4626Oracle": "rate", "SwaapSafeguardOracle": "rate",
    "CrossAdapter": "cross",
    "FixedRateWithConstraintsOracle": "min",
}
USD = "0x0000000000000000000000000000000000000348"


def _addr_call(target, calls):
    res = multicall.aggregate3(target, calls) if calls else []
    out = []
    for ok, ret in res:
        a = None
        if ok and ret not in ("0x", "") and len(ret) >= 66:
            a = "0x" + ret[-40:].lower()
        out.append(a if nz(a) else None)
    return out


def euler_resolve(rs, pairs):
    """Router path for each (router, base, quote)."""
    paths = {}
    frontier = {(r, b, q): (b, []) for r, b, q in pairs}
    for _depth in range(4):
        if not frontier:
            break
        keys = sorted(frontier)
        calls = []
        for k in keys:
            r, _b0, q = k
            b = frontier[k][0]
            calls.append((r, evmabi.encode_call(
                "getConfiguredOracle(address,address)", ["address", "address"],
                [b, q])))
            calls.append((r, evmabi.encode_call(
                "resolvedVaults(address)", ["address"], [b])))
            calls.append((r, evmabi.selector("fallbackOracle()")))
        res = _addr_call(rs.target, calls)
        nxt = {}
        for i, k in enumerate(keys):
            b, steps = frontier[k]
            adapter, unwrap, fallback = res[3 * i:3 * i + 3]
            if b == k[2]:
                paths[k] = steps + [{"role": "identity", "base": b}]
            elif adapter:
                paths[k] = steps + [{"role": "adapter", "base": b,
                                     "address": adapter}]
            elif unwrap:
                nxt[k] = (unwrap, steps + [{"role": "router_unwrap",
                                            "vault": b, "asset": unwrap}])
            elif fallback:
                paths[k] = steps + [{"role": "fallback_oracle", "base": b,
                                     "address": fallback}]
            else:
                paths[k] = steps + [{"role": "unconfigured", "base": b}]
        frontier = nxt
    for k, (b, steps) in frontier.items():
        paths[k] = steps + [{"role": "too_deep", "base": b}]
    return paths


def _unit_amount(rs, token):
    """10 ** decimals of token (cached); 1e18 for Euler's USD address or a token without decimals()."""
    if not nz(token) or token == USD:
        return 10 ** 18
    got = cached_calls(rs.target, rs.cid,
                       [(token, evmabi.selector("decimals()"))])[0]
    ok = got["ok"] and len(got["ret"]) >= 66
    return 10 ** (int(got["ret"][:66], 16) if ok else 18)


def call_trace_node(rs, addr, data, label):
    """Legs of one quote call of addr (data), from a cached trace, as a composite node classify_side orders from the collateral."""
    tr = trace_call(rs.target, rs.cid, addr, data=data)
    f = tr.get("result") or {}
    node = {"address": addr, "role": "traced", "kind_basis": "trace",
            "trace": {"block": tr.get("block"), "data": data[:10],
                      "calls": _summary(f)}}
    if tr.get("rpc_error") or f.get("error"):
        return {**node, "kind": "unknown",
                "reason": f"{label} reverts in a trace at block "
                          f"{tr.get('block')} ({f.get('error') or tr.get('rpc_error')}, "
                          f"{(f.get('output') or '')[:10]})"}
    legs, checks, notes = trace_legs(rs, addr, addr, f, 1)
    if checks:
        node["trace"]["checks"] = sorted(set(checks))
    if notes:
        node["trace"]["notes"] = notes
    if not legs:
        st = _storage_reads(rs, addr, addr, data)
        return {**node, "kind": "stored" if st else "constant",
                "description": f"{label} of {addr}",
                "kind_basis": "trace: no external call, "
                              + ("reads storage" if st else "reads no storage")}
    unknown = [x for x in legs if x.get("kind") in ("unknown", None)]
    if unknown:
        return {**node, "kind": "unknown", "legs": legs,
                "reason": "; ".join(f"{x.get('selector')} on "
                                    f"{x.get('address')}: "
                                    + (x.get("reason") or "kind not identified")
                                    for x in unknown)}
    for x in legs:
        x.setdefault("role", x.get("selector") or "read")
        x["side"] = "base"
        if x.get("role") == "vault_conversion":
            a = x.get("vault_asset")
            vs = rs.token_symbols([x["address"], a] if a else [x["address"]])
            x["pair"] = [norm_sym(rs.match_sym(x["address"],
                                               vs.get(x["address"]))),
                         norm_sym(rs.match_sym(a, vs.get(a))) if a else None]
            x["kind"], x["role"] = "rate", "vault_rate"
            x["description"] = f"vault conversion {x['address']}"
    return {**node, "kind": "composite", "legs": legs,
            "description": f"{label} of {addr}"}


def euler_adapter_legs(rs, addr, depth=0, base_hint=None, quote_hint=None):
    """Flatten an adapter into ordered legs base -> quote."""
    p = rs.probes.get(addr) or {}
    name = p.get("name")
    kind = EULER_KIND.get(name)
    if kind == "cross" and depth < 3:
        a, b, base, cross, quote = _addr_call(rs.target, [
            (addr, evmabi.selector(s)) for s in
            ("oracleBaseCross()", "oracleCrossQuote()", "base()", "cross()",
             "quote()")])
        rs.prepare([x for x in (a, b) if x])
        return (euler_adapter_legs(rs, a, depth + 1) if a else
                [{"address": None, "kind": "unknown", "reason": "no leg"}]) + \
               (euler_adapter_legs(rs, b, depth + 1) if b else
                [{"address": None, "kind": "unknown", "reason": "no leg"}])
    base, quote, feed = _addr_call(rs.target, [
        (addr, evmabi.selector(s)) for s in ("base()", "quote()", "feed()")])
    syms = rs.token_symbols([x for x in (base, quote) if x])
    leg = {"address": addr, "contract": name, "role": "adapter",
           "kind": kind or "unknown", "base": base, "quote": quote,
           "pair": [norm_sym(rs.match_sym(base, syms.get(base))
                             or ("USD" if base == USD else base)),
                    norm_sym(rs.match_sym(quote, syms.get(quote))
                             or ("USD" if quote == USD else quote))]}
    if kind == "pt":
        mkt = _addr_call(rs.target,
                         [(addr, evmabi.selector("pendleMarket()"))])[0]
        info = rs.pendle_underlying(addr, market=mkt, otype=-1) if mkt \
            else {}
        if info.get("sy"):
            info = rs.pendle_underlying(
                addr, otype=0 if quote == info["sy"] else 1, market=mkt)
        leg.update(info)
        leg["underlying_symbol"] = info.get("underlying_symbol") or \
            leg["pair"][1]
    if not kind:
        leg["reason"] = f"adapter name {name!r} not in the reviewed list"
        b, q = base or base_hint, quote or quote_hint
        if nz(b) and nz(q) and depth < 3:
            data = evmabi.encode_call(
                "getQuote(uint256,address,address)",
                ["uint256", "address", "address"],
                [_unit_amount(rs, b), b, q])
            node = call_trace_node(rs, addr, data, "getQuote")
            out = {**leg, **node, "contract": name,
                   "reason": (leg["reason"] + "; " + node["reason"])
                   if node.get("reason") else None}
            if not out["reason"]:
                out.pop("reason")
            return [out]
    if feed:
        rs.prepare([feed])
        f = rs.feed(feed)
        leg["feed"] = {k: f.get(k) for k in ("address", "contract",
                                             "description", "pair", "kind",
                                             "feed_id")}
        for k in ("source_via", "eoracle_feed_id"):
            if k in f:
                leg["feed"][k] = f[k]
        if kind == "market" and f.get("pair"):
            leg["pair"] = f["pair"]
        if kind == "market" and f.get("kind") == "rate":
            leg["kind"] = "rate"
    return [leg]


def classify_euler(rs, rows):
    vaults = {r["vault"]: r for r in rows if r.get("type") == "vault"}
    last = {}
    for r in rows:
        if r.get("type") == "ltv":
            last[(r["vault"], r["collateral_vault"])] = r
    assets = {v: r["asset"] for v, r in vaults.items()}
    syms = rs.token_symbols(list(assets.values()))
    pairs = set()
    for (v, cv) in last:
        vr = vaults[v]
        if nz(vr.get("oracle")):
            pairs.add((vr["oracle"], cv, vr["unit_of_account"]))
    paths = euler_resolve(rs, pairs)
    adapters = {s["address"] for p in paths.values() for s in p
                if s.get("address")}
    rs.prepare(adapters)
    extra = {s["asset"] for p in paths.values() for s in p
             if s.get("asset")}
    syms = rs.token_symbols(list(extra) + list(assets.values()))
    leg_cache = {}
    out = []
    for (v, cv), lk in sorted(last.items()):
        vr = vaults[v]
        cv_asset = assets.get(cv)
        row = {"protocol": "euler_v2", "chain_id": rs.cid, "vault": v,
               "collateral_vault": cv, "oracle": vr.get("oracle"),
               "unit_of_account": vr.get("unit_of_account"),
               "loan_token": vr.get("asset"),
               "loan_symbol": syms.get(vr.get("asset")),
               "collateral_token": cv_asset,
               "collateral_symbol": syms.get(cv_asset),
               "borrow_ltv": lk["borrow_ltv"],
               "liquidation_ltv": lk["liquidation_ltv"]}
        if not nz(vr.get("oracle")):
            row.update(type="none", subtype=None, blind_to=None,
                       reasons=["vault has no oracle router"], evidence=None)
            out.append(row)
            continue
        steps = paths.get((vr["oracle"], cv, vr["unit_of_account"]), [])
        legs = []
        for s in steps:
            if s["role"] == "router_unwrap":
                if s["vault"] == cv:
                    continue
                legs.append({"side": "base", "role": "vault_conversion",
                             "address": s["vault"], "kind": "rate"})
            elif s["role"] == "identity":
                continue
            elif s.get("address"):
                ck = (s["address"], s.get("base"), vr["unit_of_account"])
                if ck not in leg_cache:
                    leg_cache[ck] = euler_adapter_legs(
                        rs, s["address"], base_hint=s.get("base"),
                        quote_hint=vr["unit_of_account"])
                legs.extend({"side": "base", **x}
                            for x in leg_cache[ck])
            else:
                legs.append({"side": "base", "role": s["role"],
                             "address": None, "kind": "unknown",
                             "reason": s["role"]})
        vsym = {s["vault"]: rs.match_sym(s["asset"], syms.get(s["asset"]))
                for s in steps if s["role"] == "router_unwrap"}
        if steps and steps[-1]["role"] == "identity" and all(
                x["role"] == "vault_conversion" for x in legs):
            under = syms.get(steps[-1]["base"]) or steps[-1]["base"]
            row.update(type="tracks_market" if not legs else "exchange_rate",
                       subtype=("collateral_is_numeraire" if not legs
                                else "underlying_is_numeraire"),
                       blind_to=(None if not legs else
                                 f"a discount of {row['collateral_symbol']} "
                                 f"against {under}"),
                       reasons=["the router reaches the unit of account "
                                "itself (getQuote returns the amount "
                                "unchanged), so the loan is priced in the "
                                "collateral's asset"
                                + ("" if not legs else
                                   " after its share-rate conversion")],
                       evidence={"router_path": steps, "legs": legs})
            out.append(row)
            continue
        t, sub, blind, why = classify_side(
            legs, rs.match_sym(cv_asset, row["collateral_symbol"]
                               or cv_asset or ""), vsym)
        row.update(type=t, subtype=sub, blind_to=blind, reasons=why,
                   evidence={"router_path": steps, "legs": legs})
        out.append(row)
    return out


KNOWN = {
    "1-0xb48bb53f0f2690c71e8813f2dc7ed6fca9ac4b0ace3faa37b4a8e5ece38fa1a2":
        "bUSD0 (USD0++) / USDC, 86%",
    "1-0x64d6a777c466c2eb82a1efb4814aa2eab673a95e66fc229fea54125608e6f342":
        "bUSD0 (USD0++) / wM, 86%",
    "1-0xbd1ad3b968f5f0552dbd8cf1989a62881407c5cccf9e49fb3657c8731caf0c1f":
        "deUSD / USDC, 86%",
    "1-0x0f9563442d64ab3bd3bcb27058db0b0d4046a4c46f0acd811dacae9551d2b129":
        "sdeUSD / USDC, 91.5%",
}
CROSSCHECK = (pathlib.Path(os.environ["CB_INPUTS"]).resolve() if os.environ.get("CB_INPUTS")
           else HERE.parents[1] / "data" / "inputs") / "oracle_check"


def price_samples(target, cid, rows, n_blocks=8):
    """price() of each row's oracle on a grid of archive blocks between the earliest creation and the head; cached per (oracle, block)."""
    cache = AppendCache(CACHE / str(cid) / "price_samples.jsonl", key="key")
    head = rpc.block_number(target)
    lo = min(r["created_block"] for r in rows)
    grid = sorted({lo + (head - 1 - lo) * i // (n_blocks - 1)
                   for i in range(n_blocks)})
    sel = evmabi.selector("price()")
    for b in grid:
        todo = sorted({r["oracle"] for r in rows if r["created_block"] < b
                       and f"{r['oracle']}@{b}" not in cache})
        if not todo:
            continue
        res = multicall.aggregate3(target, [(o, sel) for o in todo], block=b,
                                   chunk=100)
        cache.add_many([{"key": f"{o}@{b}", "oracle": o, "block": b,
                         "price": (str(int(ret, 16)) if ok and ret not in
                                   ("0x", "") else None)}
                        for o, (ok, ret) in zip(todo, res)])
    out = {}
    for r in rows:
        vals = [cache.get(f"{r['oracle']}@{b}") for b in grid
                if r["created_block"] < b]
        out[r["oracle"]] = [(v["block"], v["price"]) for v in vals if v]
    return out


def explain(row):
    """Why a historical-check mismatch is expected, or None."""
    ev = row.get("evidence") or {}
    legs = ev.get("legs") or (ev.get("current") or {}).get("legs") or []
    if row["type"] == "fixed_price" and (
            row["subtype"] == "fixed_peg_then_market_feed"
            or any(x.get("side") == "quote" for x in legs)):
        return "quote-side or peg-asset feed moves the price"
    if row["type"] == "tracks_market" and \
            (row.get("collateral_symbol") or "").upper().startswith("PT-"):
        return "PT oracle reads par at and after maturity"
    return None


def quote_history(protocol, cid, rows):
    """Cross-check Euler and Silo classifications with archive quotes."""
    from collections import Counter

    panel = DATA / "panel" / protocol / str(cid)
    checkpoint = panel / ("checkpoint.json" if protocol == "euler_v2"
                          else "_checkpoint.json")
    if not checkpoint.exists():
        return {"status": "not checked: no panel checkpoint",
                "checkpoint": str(checkpoint.relative_to(DATA))}
    state = json.loads(checkpoint.read_text())
    head = (state["to_block"] if protocol == "euler_v2" else state["head"]) - 1
    regs = load_markets(protocol, cid)
    created = {r["vault"]: r["created_block"] for r in regs
               if r.get("type") == "vault"} if protocol == "euler_v2" else {
                   r["silo"]: r["created_block"] for r in regs}
    lo = min(created.values())
    grid = sorted({lo + (head - lo) * i // 7 for i in range(8)})
    target = logs.rpc_target(logs.load_chains()[str(cid)])
    cache = AppendCache(CACHE / str(cid) / "quote_samples.jsonl", key="key")
    specs, skipped = [], Counter()
    for row in rows:
        if protocol == "euler_v2":
            oracle = row.get("oracle")
            base = row["collateral_vault"]
            steps = (row.get("evidence") or {}).get("router_path", [])
            if steps and steps[0]["role"] == "router_unwrap" and \
                    steps[0]["vault"] == base:
                base = steps[0]["asset"]
            quote = row["unit_of_account"]
            calldata = evmabi.encode_call(
                "getQuote(uint256,address,address)",
                ["uint256", "address", "address"], [10 ** 18, base, quote])
            key = f"{row['vault']}:{row['collateral_vault']}"
            born = max(created.get(row["vault"], lo),
                       created.get(row["collateral_vault"], lo))
            classified = row
        else:
            classified = row.get("solvency_oracle", row)
            oracle = classified.get("address")
            base = row["token"]
            calldata = evmabi.encode_call(
                "quote(uint256,address)", ["uint256", "address"],
                [10 ** 18, base])
            key, born = row["silo"], created[row["silo"]]
        if not nz(oracle):
            skipped["no_oracle"] += 1
            continue
        specs.append({"key": key, "oracle": oracle, "base": base,
                      "calldata": calldata, "created_block": born,
                      "classification": classified})
    for block in grid:
        todo = {}
        for spec in specs:
            key = f"{spec['oracle']}:{spec['calldata']}@{block}"
            if block > spec["created_block"] and key not in cache:
                todo[key] = spec
        if not todo:
            continue
        keys = sorted(todo)
        results = multicall.aggregate3(
            target, [(todo[k]["oracle"], todo[k]["calldata"]) for k in keys],
            block=block, chunk=100)
        cache.add_many([
            {"key": k, "oracle": todo[k]["oracle"],
             "calldata": todo[k]["calldata"], "block": block,
             "price": str(int(ret, 16)) if ok and len(ret) == 66 else None}
            for k, (ok, ret) in zip(keys, results)])
    checked, insufficient, violations, by_type, details = 0, 0, [], Counter(), []
    for spec in specs:
        samples = [cache.get(f"{spec['oracle']}:{spec['calldata']}@{b}")
                   for b in grid if b > spec["created_block"]]
        values = [s["price"] for s in samples if s and s["price"] is not None]
        row = spec["classification"]
        detail = {k: spec[k] for k in ("key", "oracle", "base", "calldata")}
        detail.update(type=row["type"], subtype=row.get("subtype"),
                      samples=[[s["block"], s["price"]] for s in samples if s])
        if len(values) < 2:
            insufficient += 1
            detail["result"] = "insufficient_samples"
        else:
            checked += 1
            const = len(set(values)) == 1
            detail["result"] = "constant" if const else "varies"
            by_type[f"{row['type']}:{detail['result']}"] += 1
            if (row["type"] == "fixed_price" and not const) or \
                    (row["type"] == "tracks_market" and const):
                explanation = explain(row)
                if row.get("subtype") == "collateral_is_numeraire":
                    explanation = "identity quote: collateral is the numeraire"
                violations.append({"market": spec["key"], "type": row["type"],
                                   "subtype": row.get("subtype"),
                                   "explained": explanation,
                                   "samples": detail["samples"]})
        details.append(detail)
    return {"checked": checked, "insufficient_samples": insufficient,
            "skipped": dict(skipped), "by_type": dict(by_type),
            "grid": grid, "input_amount_raw": str(10 ** 18),
            "method": "historical quote of the classified collateral asset; "
                      "current oracle address, historical configuration; "
                      "Euler initial lending-vault unwrap excluded",
            "violations": violations,
            "unexplained": [v["market"] for v in violations if not v["explained"]],
            "rows": details}


def validate(only=(), skip_crosscheck=False):
    """Refresh comparisons and per-target quote checks without relabelling."""
    import random
    from collections import Counter, defaultdict
    started, rpc0 = time.monotonic(), rpc.calls_made()
    previous_path = DATA / "registry" / "oracles" / "validation.json"
    previous, previous_utc = {}, None
    if previous_path.exists():
        previous = json.loads(previous_path.read_text())
        meta_path = previous_path.with_name(previous_path.name + ".meta.json")
        if meta_path.exists():
            previous_utc = json.loads(meta_path.read_text()).get("written_utc")
    res = {}
    new = {}
    targets = {}
    for f in sorted((DATA / "registry" / "oracles").glob("*/*.jsonl")):
        rows = io_utils.read_jsonl(str(f.relative_to(DATA)))
        targets[f"{f.parent.name}:{f.stem}"] = rows
        for r in rows:
            if r.get("market_id"):
                new[f"{r['chain_id']}-{r['market_id']}"] = r
    res["known_cases"] = {
        k: {"label": lab,
            "type": new.get(k, {}).get("type"),
            "subtype": new.get(k, {}).get("subtype"),
            "blind_to": new.get(k, {}).get("blind_to"),
            "reasons": new.get(k, {}).get("reasons"),
            "oracle": new.get(k, {}).get("oracle")}
        for k, lab in KNOWN.items()}
    old = json.loads((DATA / "markets" / "oracle-modes.json").read_text())
    conf = defaultdict(Counter)
    dis = []
    for k, m in old["by_market"].items():
        t = new.get(k, {}).get("type", "missing")
        conf[m][t] += 1
        agree = ((m == "hard" and t == "fixed_price")
                 or (m == "none" and t == "none")
                 or (m == "track" and t not in ("fixed_price", "none")))
        if not agree:
            dis.append({"market": k, "old": m, "new": t,
                        "subtype": new.get(k, {}).get("subtype"),
                        "reasons": new.get(k, {}).get("reasons")})
    res["old_labels"] = {"confusion": {m: dict(c) for m, c in conf.items()},
                         "n_markets": len(old["by_market"]),
                         "n_disagree": len(dis),
                         "disagreements": dis}
    sib_map = {"market": "tracks_market", "hardcoded": "fixed_price",
               "exchange_rate": "exchange_rate",
               "lower_of_component": "min_of_two"}
    sconf = defaultdict(Counter)
    sdis = []
    for cid in (() if skip_crosscheck else (1, 8453)):
        path = CROSSCHECK / "morpho" / f"{cid}.jsonl"
        if not path.exists():
            continue
        sib = {r["oracle"]: r for r in map(json.loads, path.open())
               if r.get("oracle")}
        for k, r in new.items():
            if r["protocol"] != "morpho" or r["chain_id"] != cid:
                continue
            s = sib.get(r["oracle"])
            if not s:
                continue
            sconf[s["classification"]][r["type"]] += 1
            exp = sib_map.get(s["classification"])
            if exp and exp != r["type"]:
                sdis.append({"market": k, "crosscheck": s["classification"],
                             "new": r["type"], "subtype": r.get("subtype"),
                             "reasons": r.get("reasons")})
    res["crosscheck"] = {"confusion": {m: dict(c) for m, c in sconf.items()},
                      "n_disagree_mapped": len(sdis), "disagreements": sdis}
    if skip_crosscheck:
        res["crosscheck"] = previous.get("crosscheck", {})
        res["crosscheck"]["refresh_status"] = "preserved; independent classification not read"
    rng = random.Random(20261002)
    chains = logs.load_chains()
    by_chain = defaultdict(list)
    for k, r in new.items():
        if r["protocol"] in ("morpho", "moolah") and nz(r["oracle"]) and \
                r["type"] in ("fixed_price", "tracks_market",
                              "exchange_rate"):
            by_chain[r["chain_id"]].append(r)
    hist = {"checked": 0, "violations": [], "by_type": Counter(),
            "by_chain": {}}
    for cid, rs_ in sorted(by_chain.items()):
        mk = {f"{cid}-{x['market_id']}": x for x in rs_}
        regs = {x["market_id"]: x for x in load_markets(rs_[0]["protocol"], cid)}
        must = sorted(k for k, x in mk.items()
                      if x["type"] in ("fixed_price", "tracks_market"))
        rest = sorted(k for k, x in mk.items() if k not in set(must))
        pick = must + rng.sample(rest, min(100, len(rest)))
        rows = [{"oracle": mk[k]["oracle"],
                 "created_block": regs[mk[k]["market_id"]]["created_block"],
                 "key": k} for k in pick]
        target = logs.rpc_target(chains[str(cid)])
        samples = price_samples(target, cid, rows)
        per = hist["by_chain"][str(cid)] = {"picked": len(rows), "checked": 0,
                                            "by_type": Counter()}
        for r in rows:
            vals = [p for _, p in samples.get(r["oracle"], []) if p]
            if len(vals) < 2:
                continue
            t = mk[r["key"]]["type"]
            hist["checked"] += 1
            per["checked"] += 1
            const = len(set(vals)) == 1
            hist["by_type"][f"{t}:{'constant' if const else 'varies'}"] += 1
            per["by_type"][f"{t}:{'constant' if const else 'varies'}"] += 1
            if (t == "fixed_price" and not const) or \
                    (t == "tracks_market" and const):
                hist["violations"].append({
                    "market": r["key"], "type": t,
                    "subtype": mk[r["key"]]["subtype"],
                    "collateral": mk[r["key"]]["collateral_symbol"],
                    "explained": explain(mk[r["key"]]),
                    "samples": samples[r["oracle"]]})
    hist["unexplained"] = [v["market"] for v in hist["violations"]
                           if not v["explained"]]
    hist["by_type"] = dict(hist["by_type"])
    for per in hist["by_chain"].values():
        per["by_type"] = dict(per["by_type"])
    res["historical"] = hist
    res["targets"] = {}
    for spec, rows in sorted(targets.items()):
        protocol, cid = spec.split(":")
        summary = {"rows": len(rows),
                   "by_type": dict(Counter(r["type"] for r in rows))}
        if protocol in ("euler_v2", "silo_v2") and (not only or spec in only):
            summary["historical"] = quote_history(protocol, int(cid), rows)
        elif (previous.get("targets", {}).get(spec) or {}).get("historical"):
            summary["historical"] = previous["targets"][spec]["historical"]
            summary["historical_written_utc"] = previous["targets"][spec].get(
                "historical_written_utc", previous_utc)
        res["targets"][spec] = summary
    io_utils.write_json("registry/oracles/validation.json", res,
                        generator="code/data/oracles.py --validate",
                        meta={"rpc_calls": rpc.calls_made() - rpc0,
                              "runtime_seconds": round(time.monotonic() - started, 3),
                              "only": list(only), "skip_crosscheck": skip_crosscheck})
    print(json.dumps({k: (v if k != "old_labels" else
                          {"confusion": v["confusion"],
                           "n_disagree": v["n_disagree"]})
                      for k, v in res.items() if k != "crosscheck"},
                     indent=1, default=str)[:6000], flush=True)
    print("crosscheck", json.dumps(res["crosscheck"]["confusion"], indent=1),
          res["crosscheck"]["n_disagree_mapped"], flush=True)


def load_markets(protocol, cid):
    path = DATA / "registry" / "markets" / protocol / f"{cid}.jsonl"
    return [json.loads(l) for l in path.open() if l.strip()]


HISTORICAL = [False]


def run(protocol, cid):
    chains = logs.load_chains()
    target = logs.rpc_target(chains[str(cid)])
    rs = Resolver(cid, target)
    rs.historical = HISTORICAL[0]
    rows = load_markets(protocol, cid)
    t0 = rpc.calls_made()
    es0, rs0, started = ES_CALLS[0], RS_CALLS[0], time.monotonic()
    if protocol == "morpho":
        out = classify_morpho(rs, rows, protocol)
    elif protocol == "moolah":
        out = classify_moolah(rs, rows)
    elif protocol == "euler_v2":
        out = classify_euler(rs, rows)
    elif protocol == "silo_v2":
        out = classify_silo(rs, rows)
    else:
        raise SystemExit(f"{protocol}: not implemented")
    if rs.historical:
        out = [add_retry_notes(r) for r in out]
    io_utils.write_jsonl(
        f"registry/oracles/{protocol}/{cid}.jsonl", out,
        generator="code/data/oracles.py",
        meta={"protocol": protocol, "chain_id": cid,
              "rpc_calls": rpc.calls_made() - t0,
              "etherscan_calls": ES_CALLS[0] - es0,
              "routescan_calls": RS_CALLS[0] - rs0,
              "runtime_seconds": round(time.monotonic() - started, 3)})
    from collections import Counter
    print(f"{protocol}:{cid} {len(out)} rows "
          f"{dict(Counter(r['type'] for r in out))} "
          f"rpc={rpc.calls_made() - t0} etherscan={ES_CALLS[0] - es0} "
          f"routescan={RS_CALLS[0] - rs0}", flush=True)


def add_retry_notes(row):
    """Append to a row's reasons (each Silo role's, for Silo) the note of every feed read retried from a caller or at a histori."""
    def notes(x, out):
        if isinstance(x, dict):
            n = (x.get("trace") or {}).get("note") \
                if isinstance(x.get("trace"), dict) else None
            if n and n not in out:
                out.append(n)
            for v in x.values():
                notes(v, out)
        elif isinstance(x, list):
            for v in x:
                notes(v, out)
        return out
    for it in ([row.get(k) for k in ("solvency_oracle", "max_ltv_oracle")]
               if row.get("protocol") == "silo_v2" else [row]):
        if not isinstance(it, dict) or not it.get("reasons"):
            continue
        for n in notes(it.get("evidence"), []):
            if n not in it["reasons"]:
                it["reasons"].append(n)
    return row


def reverting(r):
    """A row whose price call, or a feed it reads, reverts in a trace."""
    text = json.dumps([r.get(k) for k in ("subtype", "reasons",
                                          "solvency_oracle",
                                          "max_ltv_oracle")])
    return "trace_reverts" in text or "reverts in a trace" in text


def _row_key(protocol, r):
    if protocol == "euler_v2":
        return (r["vault"], r["collateral_vault"])
    if protocol == "silo_v2":
        return r["silo"]
    return r["market_id"]


def retrace(protocol, cid, write=True, subtypes=None, historical=False,
            select=None):
    """Reclassify the unresolved rows of an existing output (only those with a subtype in subtypes, when given) through the pro."""
    rel = f"registry/oracles/{protocol}/{cid}.jsonl"
    rows = io_utils.read_jsonl(rel)
    chains = logs.load_chains()
    rs = Resolver(cid, logs.rpc_target(chains[str(cid)]))
    rs.historical = historical
    t0, started = rpc.calls_made(), time.monotonic()

    def unres(r):
        if r.get("type") != "unresolved":
            return False
        if subtypes and r.get("subtype") not in subtypes and not any(
                (r.get(k) or {}).get("subtype") in subtypes
                for k in ("solvency_oracle", "max_ltv_oracle")):
            return False
        if select and not select(r):
            return False
        o = r.get("oracle")
        return not (o and (rs.code.get(o) or {}).get("skeleton")
                    in TRACE_SKIP)
    todo = {_row_key(protocol, r) for r in rows if unres(r)}
    regs = load_markets(protocol, cid)
    if protocol in ("morpho", "moolah"):
        sel = [m for m in regs if m["market_id"] in todo]
        out = classify_morpho(rs, sel, protocol) if protocol == "morpho" \
            else classify_moolah(rs, sel)
    elif protocol == "euler_v2":
        vaults = {v for k in todo for v in k}
        sel = [m for m in regs if (m.get("type") == "vault" and
                                   m["vault"] in vaults) or
               (m.get("type") == "ltv" and
                (m["vault"], m["collateral_vault"]) in todo)]
        out = [r for r in classify_euler(rs, sel)
               if _row_key(protocol, r) in todo]
    elif protocol == "silo_v2":
        out = classify_silo(rs, [m for m in regs if m["silo"] in todo])
    else:
        raise SystemExit(f"{protocol}: not supported")
    new = {_row_key(protocol, r): add_retry_notes(r) for r in out}
    changed = []
    for i, r in enumerate(rows):
        k = _row_key(protocol, r)
        if k in todo and k in new:
            n = new[k]
            changed.append((k, r.get("subtype") or
                            (r.get("solvency_oracle") or {}).get("subtype"),
                            n["type"], n.get("subtype") or
                            (n.get("solvency_oracle") or {}).get("subtype")))
            rows[i] = n
    if write:
        side = DATA / (rel + ".meta.json")
        prev = json.loads(side.read_text())
        meta = {k: v for k, v in prev.items()
                if k not in ("generator", "written_utc", "rows")}
        runs = meta.get("trace_fallback")
        runs = runs if isinstance(runs, list) else ([runs] if runs else [])
        runs.append({
            "command": f"code/data/oracles.py --retrace {protocol}:{cid}"
                       + (f" --subtypes {','.join(subtypes)}"
                          if subtypes else "")
                       + (" --historical" if historical else "")
                       + (" --reverting" if select is reverting else ""),
            "rows_reclassified": len(changed),
            "rpc_calls": rpc.calls_made() - t0,
            "runtime_seconds": round(time.monotonic() - started, 3)})
        meta["trace_fallback"] = runs if len(runs) > 1 else runs[0]
        io_utils.write_jsonl(rel, rows, generator=prev["generator"],
                             meta=meta)
    from collections import Counter
    print(f"{protocol}:{cid} retraced {len(changed)} rows "
          f"{dict(Counter((c[2], c[3]) for c in changed))} "
          f"rpc={rpc.calls_made() - t0}", flush=True)
    return changed, new


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", action="append", default=[],
                    help="protocol:chain_id, repeatable")
    ap.add_argument("--validate", action="store_true",
                    help="write data/registry/oracles/validation.json")
    ap.add_argument("--skip-crosscheck", action="store_true",
                    help="preserve the existing crosscheck comparison without "
                         "reading the independent classification")
    ap.add_argument("--retrace", action="append", default=[],
                    help="protocol:chain_id; reclassify rows whose oracle "
                         "has no source from price() traces, in place")
    ap.add_argument("--subtypes", default=None,
                    help="with --retrace: only unresolved rows with these "
                         "subtypes (comma-separated); default all")
    ap.add_argument("--historical", action="store_true",
                    help="retake calls that revert at the head at the "
                         "market's last logged block, and feed reads from "
                         "their real callers")
    ap.add_argument("--reverting", action="store_true",
                    help="with --retrace: only rows whose price call or a "
                         "feed read reverts in a trace")
    ap.add_argument("--dry-run", action="store_true",
                    help="with --retrace: classify and print, write nothing")
    args = ap.parse_args()
    for spec in args.retrace:
        protocol, cid = spec.split(":")
        retrace(protocol, int(cid), write=not args.dry_run,
                subtypes=args.subtypes.split(",") if args.subtypes else None,
                historical=args.historical,
                select=reverting if args.reverting else None)
    if args.retrace:
        return
    if args.validate:
        validate(args.only, args.skip_crosscheck)
        return
    HISTORICAL[0] = args.historical
    for spec in args.only:
        protocol, cid = spec.split(":")
        run(protocol, int(cid))


if __name__ == "__main__":
    main()
