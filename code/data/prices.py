# /// script
# requires-python = ">=3.11"
# dependencies = ["requests>=2.31", "eth-abi>=5.0", "eth-utils>=4.0", "eth-hash[pycryptodome]"]
# ///
"""Build address-based daily USD prices for the E2, E3 and E4 experiments."""
import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import random
import re
import statistics
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "code"))
from common import blocktime, evmabi, io_utils, multicall, rpc

START = dt.date(2024, 1, 1)
END = dt.date(2026, 10, 4)
DAY = 86400
CACHE = ROOT / ".cache/prices"
OUT = ROOT / "data/prices"
GENERATOR = "code/data/prices.py"
API = "https://coins.llama.fi"
FEED_SOURCE = "https://reference-data-directory.vercel.app/feeds-mainnet.json"
FEED_SOURCE_CODE = "https://github.com/smartcontractkit/documentation/blob/main/src/features/data/chains.ts"
SCOPE = {"morpho": [1, 8453], "moolah": [56], "euler_v2": [1, 9745],
         "silo_v2": [1, 42161, 43114, 146]}
CHAIN_IDS = sorted({cid for cids in SCOPE.values() for cid in cids})
CHAINS = json.loads((ROOT / "code/config/chains.json").read_text())
ZERO = "0x" + "0" * 40
SEED = 20261004
VERSION = 1

ADDRESS_RULES = {
    (1, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): ("usd_stable", "USD", "canonical_usdc"),
    (1, "0xdac17f958d2ee523a2206206994597c13d831ec7"): ("usd_stable", "USD", "canonical_usdt"),
    (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"): ("eth", "ETH", "canonical_weth"),
    (1, "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"): ("btc", "BTC", "canonical_wbtc"),
    (1, "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0"): ("eth", "stETH", "wsteth_underlying"),
    (1, "0x9d39a5de30e57443bff2a8307a4256c8797a3497"): ("usd_stable", "USDe", "susde_underlying"),
    (8453, "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"): ("usd_stable", "USD", "canonical_base_usdc"),
    (8453, "0x4200000000000000000000000000000000000006"): ("eth", "ETH", "canonical_base_weth"),
}
for _cid, _addresses in {
    1: ["0x3d4762b4bb4b4c922377fe5b887e900d7fb64cdf",
        "0x564fa4e3eee769b911643ddc637e8fb8489cc1a2",
        "0x59d5c6f5fdf8fa53c6fe44bb053b41ab1eaaaa23",
        "0xb6a9c3375b3a57da78f7c467bd7b91cb8665a61e",
        "0xcbfb9b444d9735c345df3a0f66cd89bd741692e9",
        "0x056b269eb1f75477a8666ae8c7fe01b64dd55ecc",
        "0x0d86883faf4ffd7aeb116390af37746f45b6f378",
        "0xe30f0b62e8ae5ce2934151146dae5651c2288c84"],
    8453: ["0x5f5913aa5e21b1be4f2f4855da708e3f8dec1447",
           "0x79ee72cd7fee8d0395f75921bbaa55952feea7f0",
           "0xb48bcf35acd74e69bbdef17cdbea9de9983e786b"],
}.items():
    for _address in _addresses:
        ADDRESS_RULES[(_cid, _address)] = ("unknown", None, "ambiguous_symbol_unverified_identity")
ADDRESS_RULES[(1, "0x35d8949372d46b7a3d5a56006ae77b215fc69bc0")] = (
    "usd_stable", "USD0", "usd0pp_now_busd0")
ADDRESS_RULES[(1, "0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5")] = (
    "usd_stable", "USD", "canonical_usd0")

ADDRESS_RULES.update({
    (1, "0x363a1bd7bbb72df6fea3168a54a3c6b4d0998c2d"): ("unknown", None, "btc_mock_token"),
    (1, "0x4470a52823a553add79ed6ae9c087bf4ef7af9ce"): ("unknown", None, "eth_mock_token"),
    (56, "0x2170ed0880ac9a755fd29b2688956bd959f933f8"): ("eth", "ETH", "bsc_binance_pegged_eth"),
    (8453, "0xe722bb8cf375b700bd8e77c172880c6821587688"): ("unknown", None, "usdc_symbol_noncanonical_contract"),
    (8453, "0xfb9bd0cde61c6aeda32e8f4fa866df87b2583d98"): ("unknown", None, "usdt_symbol_unverified_identity"),
    (8453, "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"): ("usd_stable", "USD", "base_bridged_usdt"),
    (42161, "0xaf88d065e77c8cc2239327c5edb3a432268e5831"): ("usd_stable", "USD", "arbitrum_native_usdc"),
    (42161, "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"): ("usd_stable", "USD", "arbitrum_bridged_usdc"),
    (1, "0xaca92e438df0b2401ff60da7e4337b687a2435da"): ("usd_stable", "USD", "metamask_musd"),
    (1, "0xdd468a1ddc392dcdbef6db6e34e89aa338f9f186"): ("usd_stable", "USD", "mezo_musd"),
    (1, "0xcccc62962d17b8914c62d74ffb843d73b2a3cccc"): ("usd_stable", "USD", "cap_cusd"),
})
ADDRESS_RULES[(1, "0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e")] = (
    "other", "Uniswap v4 LP positions", "erc721_position_manager")

SYMBOL_RULES = [
    ("wsteth", r"wstETH", "eth", "stETH"),
    ("susde", r"sUSDe", "usd_stable", "USDe"),
    ("usd0pp", r"(?:USD0\+\+|bUSD0)", "usd_stable", "USD0"),
    ("usd0", r"USD0", "usd_stable", "USD"),
    ("savings", r"s(DAI|USDS|FRAX|frxUSD|USR|deUSD|USDD|USDf|USD0|USD1|USDz|USD3|YUSD|USDaf|USDat|NUSD|USDh|USDai|lvlUSD|msUSD)", "usd_stable", None),
    ("wrapped_usd", r"w(USDM|USDL|USDS|USDC|USDT|USDe)", "usd_stable", None),
    ("usd_direct", r"(?:USDG|USDQ|USDaf|USDat|USDi|cUSD|DUSD|VUSD|satUSD|scUSD|pmUSD|USDh|iUSD|USDC(?:\.e)?|USDT(?:\.e|0)?|DAI|USDS|USDe|deUSD|USDD|FDUSD|TUSD|BUSD|GUSD|LUSD|crvUSD|FRAX|frxUSD|USDP|PYUSD|USDM|USDL|USD0|USD1|USDf|USDF|USDx|USDz|USR|RLUSD|DOLA|MIM|alUSD|sUSD|USDbC|USDBC|USDCn|USDC\.n|USDA|USDa|USN|USDO|USDU|USX|lisUSD|fxUSD|mUSD|msUSD|reUSD|rUSD|lvlUSD|yUSD|hUSD|xUSD|USDX|USDTb|USDB|AUSD|avUSD|USDU|USDUC|USDi|USDCv2)", "usd_stable", "USD"),
    ("weeth", r"weETH", "eth", "eETH"),
    ("woeth", r"wOETH", "eth", "OETH"),
    ("wsuperoeth", r"wsuperOETHb", "eth", "superOETHb"),
    ("wrs_eth", r"wrsETH", "eth", "rsETH"),
    ("xpufeth", r"xPufETH", "eth", "pufETH"),
    ("xsolvbtc", r"xSolvBTC", "btc", "SolvBTC"),
    ("eur", r"(?:EURC(?:\.e)?|EURA|EURS|EURe|EURCV|EUROP|agEUR)", "other", "EUR"),
    ("bnb", r"(?:W?BNB|slisBNB|asBNB|BNBx|stkBNB)", "other", "BNB"),
    ("avax", r"(?:W?AVAX|sAVAX)", "other", "AVAX"),
    ("sonic", r"(?:S|wS|stS|wstS)", "other", "S"),
    ("eth_direct", r"(?:alETH|wBETH|LsETH|apxETH|pzETH|agETH|rsETH|ETH|WETH(?:\.e)?|stETH|rETH|cbETH|weETH|eETH|ezETH|rsETH|osETH|frxETH|sfrxETH|ETHx|pufETH|swETH|rswETH|mETH|cmETH|ynETH|ynETHx|wOETH|OETH|superOETHb|wsuperOETHb|tETH|wrsETH|wfrxETH)", "eth", "ETH"),
    ("btc_direct", r"(?:BTC|WBTC(?:\.e)?|BTCB|cbBTC|tBTC|LBTC|eBTC|swBTC|solvBTC|solvBTC\.BBN|xSolvBTC|pumpBTC|uniBTC|FBTC|sBTC|BTC\.b|aBTC|cBTC|dlcBTC|iBTC)", "btc", "BTC"),
    ("other_known", r"(?:EURA|EURCV|EUROP|EURC\.e|W?BNB|W?AVAX|W?S|stS|wstS|SILO|AAVE|LINK|UNI|CRV|CVX|COMP|MKR|SKY|MORPHO|EUL|WELL|BAL|PENDLE|ENA|ONDO|LDO|RPL|SNX|ARB|OP|AERO|USUAL|USUALx|CAKE|HYPE|WHYPE|EURC|EURS|EURe|agEUR|XAUt|PAXG|W?SOL|W?FTM|BEETS|wOS|OS)", "other", None),
]


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def timestamp(date):
    return int(dt.datetime.combine(date, dt.time(23, 59, 59), dt.timezone.utc).timestamp())


def date_of(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).date()


def atomic(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(obj, sort_keys=True, allow_nan=False) + "\n")
    os.replace(temp, path)


def read_json(path, default=None):
    return json.loads(path.read_text()) if path.exists() else default


def jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def meta(what, **extra):
    return {"what": what, "source": API + "/chart/{chain}:{address}",
            "time_range": {"start": START.isoformat(), "end": END.isoformat()},
            "date_semantics": "UTC day, nearest quote to 23:59:59 within 12h",
            "missing_price": "blank; no fill, interpolation or assumed peg",
            "built_utc": now(), "generator_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(), **extra}


class Audit:
    """Journal HTTP attempts without recording RPC URLs or credentials."""
    def __init__(self):
        CACHE.mkdir(parents=True, exist_ok=True)
        self.run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        self.lock = threading.Lock()
        self.path = CACHE / "requests.jsonl"

    def record(self, **fields):
        with self.lock, self.path.open("a") as handle:
            handle.write(json.dumps({"run_id": self.run_id, "utc": now(), **fields}) + "\n")

    def instrument_rpc(self):
        original = requests.post

        def post(url, **kwargs):
            body = kwargs.get("json", {})
            params = body.get("params", [])
            method = body.get("method")
            tag = params[-1] if method == "eth_call" and params else None
            fields = {"service": "rpc", "method": method,
                      "archive": method == "eth_call" and tag != "latest"}
            try:
                response = original(url, **kwargs)
                fields["http_status"] = response.status_code
                try:
                    err = response.json().get("error")
                    if err:
                        fields["rpc_error_code"] = err.get("code")
                        fields["rate_limited"] = (err.get("code") in (429, -32005)
                                                   or "rate" in str(err).lower())
                except ValueError:
                    fields["invalid_json"] = True
                return response
            except requests.RequestException:
                fields["transport_error"] = True
                raise
            finally:
                self.record(**fields)
        requests.post = post
        return original


class Llama:
    """Limit all chart workers together to four request starts per second."""
    def __init__(self, audit):
        self.audit = audit
        self.lock = threading.Lock()
        self.next_start = 0

    def get(self, path, params=None):
        for attempt in range(5):
            with self.lock:
                time.sleep(max(0, self.next_start - time.monotonic()))
                self.next_start = time.monotonic() + 0.26
            fields = {"service": "defillama", "endpoint": path.split('/')[1],
                      "started_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
            delay = 2 ** attempt
            try:
                response = requests.get(API + path, params=params, timeout=60)
                fields["http_status"] = response.status_code
                if response.status_code == 429:
                    fields["rate_limited"] = True
                    delay = max(delay, float(response.headers.get("Retry-After", 5)))
                if response.ok:
                    return response.json()
                if response.status_code not in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"DefiLlama HTTP {response.status_code}")
            except (requests.RequestException, ValueError):
                fields["transport_error"] = True
            finally:
                self.audit.record(**fields)
            time.sleep(delay)
        raise RuntimeError("DefiLlama retry budget exhausted")


def views(cid, calls):
    """Split failing multicalls so one malformed token cannot erase a batch."""
    if not calls:
        return []
    target = CHAINS[str(cid)]["drpc"]
    try:
        return multicall.aggregate3(target, calls, chunk=50)
    except Exception:
        if len(calls) == 1:
            return [(False, "0x")]
        middle = len(calls) // 2
        return views(cid, calls[:middle]) + views(cid, calls[middle:])


def decode_text(ok, raw):
    if not ok or raw == "0x":
        return None
    try:
        return evmabi.decode(["string"], raw)[0].replace("\x00", "")
    except Exception:
        try:
            return bytes.fromhex(raw[2:66]).rstrip(b"\x00").decode("utf-8") or None
        except (UnicodeDecodeError, ValueError):
            return None


def universe(cids, fresh):
    """Deduplicate tokens and market identities, resolving Euler asset views."""
    tokens = {}
    sources = []
    unresolved = []

    def add(cid, address, row, market, role, hint=None):
        if not address or not re.fullmatch(r"0x[0-9a-fA-F]{40}", address):
            raise ValueError(f"Invalid registry address for {market}")
        address = address.lower()
        key = (cid, address)
        block = row.get("created_block", row.get("block"))
        ts = row.get("created_ts", row.get("ts"))
        if not isinstance(block, int) or not isinstance(ts, int):
            raise ValueError(f"Missing creation timestamp for {market}")
        if key not in tokens:
            tokens[key] = {"chain_id": cid, "token": address,
                           "first_market_block": block, "first_market_timestamp": ts,
                           "markets": set(), "roles": set(), "symbol": None,
                           "decimals": None, "name": None}
        token = tokens[key]
        token["first_market_block"] = min(block, token["first_market_block"])
        token["first_market_timestamp"] = min(ts, token["first_market_timestamp"])
        token["markets"].add(market)
        token["roles"].add(role)
        if hint:
            for field in ("symbol", "decimals"):
                if hint.get(field) is not None:
                    token[field] = hint[field]

    for proto, chains in SCOPE.items():
        for cid in chains:
            if cid not in cids:
                continue
            path = ROOT / f"data/registry/markets/{proto}/{cid}.jsonl"
            sources.append({"path": str(path.relative_to(ROOT)),
                            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
            rows = jsonl(path)
            if proto in ("morpho", "moolah"):
                for row in rows:
                    for field, role in (("loan_token", "loan"), ("collateral_token", "collateral")):
                        add(cid, row[field], row, proto + ":" + row["market_id"], role)
            elif proto == "silo_v2":
                path = ROOT / f"data/panel/silo_v2/{cid}/configs.jsonl"
                sources.append({"path": str(path.relative_to(ROOT)),
                                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
                configs = {r["silo"].lower(): r for r in jsonl(path)}
                for row in rows:
                    cfg = configs[row["silo"].lower()]
                    if cfg["token"].lower() != row["token"].lower():
                        raise ValueError("Silo registry/config token mismatch")
                    pair = configs[cfg["paired_silo"].lower()]
                    for r in (row, pair):
                        add(cid, r["token"], row, "silo_v2:" + row["silo_config"].lower(),
                            "loan_and_collateral", configs.get(r["silo"].lower()))
            else:
                vaults = {r["vault"].lower(): r for r in rows if r["type"] == "vault"}
                external = sorted({r["collateral_vault"].lower() for r in rows
                                   if r["type"] != "vault"} - vaults.keys())
                cache_path = CACHE / f"euler_assets_{cid}.json"
                assets = {} if fresh else read_json(cache_path, {})
                resolution_path = CACHE / f"euler_assets_{cid}.sources.json"
                resolution_sources = {} if fresh else read_json(resolution_path, {})
                need = [a for a in external if a not in assets]
                for address, (ok, raw) in zip(need, views(cid, [(a, evmabi.selector("asset()")) for a in need])):
                    assets[address] = evmabi.addr(raw) if ok and len(raw) == 66 else None
                    if assets[address]:
                        resolution_sources[address] = {"method": "asset()", "block": "latest"}
                for address in external:
                    if assets.get(address):
                        continue
                    first = min(r["block"] for r in rows
                                if r.get("collateral_vault", "").lower() == address)
                    try:
                        raw = rpc.eth_call(CHAINS[str(cid)]["drpc"], address,
                                           evmabi.selector("underlying()"), block=first)
                        if len(raw) == 66 and evmabi.addr(raw) != ZERO:
                            assets[address] = evmabi.addr(raw)
                            resolution_sources[address] = {"method": "underlying()", "block": first}
                    except Exception:
                        pass
                atomic(cache_path, assets)
                atomic(resolution_path, resolution_sources)
                for row in vaults.values():
                    add(cid, row["asset"], row, "euler_v2:" + row["vault"].lower(), "vault_asset")
                for row in rows:
                    if row["type"] == "vault":
                        continue
                    collateral = row["collateral_vault"].lower()
                    asset = vaults.get(collateral, {}).get("asset") or assets.get(collateral)
                    if asset:
                        add(cid, asset, row, "euler_v2:" + row["vault"].lower(), "collateral")
                    else:
                        unresolved.append({"chain_id": cid, "vault": row["vault"],
                                           "collateral_vault": collateral,
                                           "reason": "asset() and historical underlying() unavailable; asset not guessed"})
    for token in tokens.values():
        token["market_count"] = len(token.pop("markets"))
        token["roles"] = sorted(token["roles"])
        token["start_date"] = max(START, date_of(token["first_market_timestamp"])).isoformat()
    return sorted(tokens.values(), key=lambda t: (t["chain_id"], t["token"])), sources, unresolved


def metadata(tokens, fresh):
    for cid in sorted({t["chain_id"] for t in tokens}):
        path = CACHE / f"metadata_{cid}.json"
        cached = {} if fresh else read_json(path, {})
        chain = [t for t in tokens if t["chain_id"] == cid]
        missing = [t for t in chain if t["token"] not in cached]
        for i in range(0, len(missing), 40):
            batch = missing[i:i + 40]
            calls = [(t["token"], evmabi.selector(sig)) for t in batch
                     for sig in ("symbol()", "decimals()", "name()")]
            result = views(cid, calls)
            for j, token in enumerate(batch):
                sym, dec, name = result[j * 3:j * 3 + 3]
                cached[token["token"]] = {
                    "symbol": decode_text(*sym) or token["symbol"],
                    "decimals": int(dec[1], 16) if dec[0] and len(dec[1]) == 66 else token["decimals"],
                    "name": decode_text(*name), "metadata_source": "ERC20 latest eth_call",
                }
            atomic(path, cached)
        for token in chain:
            token.update(cached[token["token"]])
        print(f"metadata chain={cid} tokens={len(chain)}", flush=True)


def classify(token):
    key = (token["chain_id"], token["token"])
    if key in ADDRESS_RULES:
        family, reference, rule = ADDRESS_RULES[key]
        return family, reference, "address:" + rule
    symbol = token.get("symbol") or ""
    principal = re.fullmatch(r"(?:n-)?PT-(.+?)-(?:\d{1,2}[A-Z]{3}\d{4}|\d{10})(?:-\([A-Z]+\))?", symbol, re.I)
    if principal:
        underlying = principal.group(1)
        for name, pattern, family, _ in SYMBOL_RULES:
            if re.fullmatch(pattern, underlying, re.I) and family in ("usd_stable", "eth", "btc"):
                return family, underlying, "symbol:pendle_principal/" + name
    for name, pattern, family, reference in SYMBOL_RULES:
        match = re.fullmatch(pattern, symbol, re.I)
        if match:
            if reference is None:
                reference = symbol[1:] if name in ("savings", "wrapped_usd") else symbol
            return family, reference, "symbol:" + name
    return "unknown", None, "default:unrecognized_symbol"


def chart_token(token, llama, fresh):
    cid, address = token["chain_id"], token["token"]
    path = CACHE / "tokens" / str(cid) / (address + ".json")
    start = dt.date.fromisoformat(token["start_date"])
    if address == ZERO:
        return {"chunks": {}, "reason": "zero_address_is_not_an_ERC20"}
    config = {"version": VERSION, "start": start.isoformat(), "end": END.isoformat(),
              "period": "1d", "search_width": "12h", "grid": "23:59:59 UTC"}
    checkpoint = read_json(path, {}) if not fresh else {}
    if checkpoint.get("config") != config:
        checkpoint = {"config": config, "chunks": {}, "token": address, "chain_id": cid}
    coin = CHAINS[str(cid)]["defillama"] + ":" + address
    for offset in range(0, max(0, (END - start).days + 1), 500):
        count = min(500, (END - start).days + 1 - offset)
        key = str(offset)
        if key in checkpoint["chunks"]:
            continue
        first = timestamp(start + dt.timedelta(days=offset))
        raw = llama.get("/chart/" + coin, {"start": first, "span": count,
                                         "period": "1d", "searchWidth": "12h"})
        if not isinstance(raw, dict) or not isinstance(raw.get("coins"), dict):
            raise RuntimeError("DefiLlama response has no coins mapping")
        checkpoint["chunks"][key] = {"requested_start": first, "span": count,
                                      "fetched_utc": now(), "response": raw}
        atomic(path, checkpoint)
    return checkpoint


def refresh_final_day(tokens, llama):
    """Refresh quotes fetched before the final UTC day had closed, in batches."""
    close = timestamp(END)
    if time.time() <= close:
        return {"status": "day_not_closed", "tokens_refreshed": 0}
    pending = {}
    for token in tokens:
        if token["token"] == ZERO:
            continue
        path = CACHE / "tokens" / str(token["chain_id"]) / (token["token"] + ".json")
        cp = read_json(path)
        if not cp:
            continue
        ending = [chunk for chunk in cp["chunks"].values()
                  if chunk["requested_start"] + (chunk["span"] - 1) * DAY == close]
        if ending and all(dt.datetime.fromisoformat(c["fetched_utc"]).timestamp() <= close for c in ending):
            pending[(token["chain_id"], token["token"])] = (path, cp)
    items = list(pending.items())
    for i in range(0, len(items), 50):
        batch = items[i:i + 50]
        coins = {CHAINS[str(cid)]["defillama"] + ":" + address: (path, cp)
                 for (cid, address), (path, cp) in batch}
        raw = llama.get("/chart/" + ",".join(coins), {"start": close, "span": 1,
                                                       "period": "1d", "searchWidth": "12h"})
        if not isinstance(raw.get("coins"), dict):
            raise RuntimeError("DefiLlama final-day response has no coins mapping")
        for coin, (path, cp) in coins.items():
            cp["chunks"]["final_day"] = {"requested_start": close, "span": 1,
                "fetched_utc": now(), "response": {"coins": {coin: raw["coins"][coin]} if coin in raw["coins"] else {}}}
            atomic(path, cp)
    return {"status": "closed", "tokens_refreshed": len(items)}


def observations(checkpoint, token):
    if token["token"] == ZERO:
        return {}, {}
    coin = CHAINS[str(token["chain_id"])]["defillama"] + ":" + token["token"]
    out = {}
    api_meta = {}
    for chunk_id, chunk in checkpoint["chunks"].items():
        obj = chunk["response"].get("coins", {}).get(coin, {})
        api_meta.update({k: obj[k] for k in ("symbol", "decimals") if k in obj})
        for point in obj.get("prices", []):
            ts = point.get("timestamp")
            px = point.get("price")
            if ts is None or not isinstance(px, (int, float)) or not math.isfinite(px) or px < 0:
                continue
            offset = math.floor((ts - chunk["requested_start"]) / DAY + 0.5)
            target = chunk["requested_start"] + offset * DAY
            if not 0 <= offset < chunk["span"] or abs(ts - target) > 43200:
                continue
            date = date_of(target).isoformat()
            if date == END.isoformat() and "final_day" in checkpoint["chunks"] and chunk_id != "final_day":
                continue
            candidate = {"price_usd": px, "confidence": point.get("confidence", obj.get("confidence")),
                         "quote_timestamp": ts, "target_timestamp": target}
            previous = out.get(date)
            if previous is None or abs(ts - target) < abs(previous["quote_timestamp"] - target):
                out[date] = candidate
    return out, api_meta


def download(tokens, llama, fresh):
    failures = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(chart_token, t, llama, fresh): t for t in tokens}
        for i, future in enumerate(as_completed(futures), 1):
            token = futures[future]
            try:
                future.result()
            except Exception as exc:
                failures.append({"chain_id": token["chain_id"], "token": token["token"],
                                 "error": type(exc).__name__ + ": " + str(exc)[:100]})
            if i % 50 == 0 or i == len(tokens):
                print(f"prices {i}/{len(tokens)} failed={len(failures)}", flush=True)
    return failures


def publish_chain(cid, tokens, input_sources, failures):
    path = OUT / f"{cid}.csv"
    temp = CACHE / f"{cid}.csv.tmp"
    stats = {"tokens_total": len(tokens), "priced": 0, "unpriced": 0,
             "rows": 0, "priced_rows": 0, "unpriced_tokens": [], "partial_tokens": 0,
             "fully_priced_tokens": 0, "quote_offset_seconds_max": 0}
    failure_tokens = {r["token"] for r in failures if r["chain_id"] == cid}
    with temp.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "token", "symbol", "price_usd", "confidence", "source"])
        for token in tokens:
            cp = read_json(CACHE / "tokens" / str(cid) / (token["token"] + ".json"), {"chunks": {}})
            quotes, api_meta = observations(cp, token)
            if not token["symbol"] and api_meta.get("symbol"):
                token["symbol"] = api_meta["symbol"]
                token["metadata_source"] += "; symbol from DefiLlama chart"
            token["token_kind"] = ("zero_address_sentinel" if token["token"] == ZERO else
                                    "erc721" if token["token"] == "0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e" and cid == 1 else
                                    "erc20_or_unverified")
            token["defillama_decimals"] = api_meta.get("decimals")
            if token["decimals"] is None and api_meta.get("decimals") is not None:
                token["decimals"] = api_meta["decimals"]
                token["metadata_source"] += "; decimals from DefiLlama chart"
            token["peg_family"], token["reference"], token["peg_rule"] = classify(token)
            start = dt.date.fromisoformat(token["start_date"])
            n = max(0, (END - start).days + 1)
            got = 0
            for offset in range(n):
                date = (start + dt.timedelta(days=offset)).isoformat()
                quote = quotes.get(date)
                if quote:
                    got += 1
                    stats["quote_offset_seconds_max"] = max(stats["quote_offset_seconds_max"],
                        abs(quote["quote_timestamp"] - quote["target_timestamp"]))
                writer.writerow([date, token["token"], token["symbol"],
                                 quote["price_usd"] if quote else None,
                                 quote["confidence"] if quote else None,
                                 "defillama:chart" if quote else ("unpriced:zero_address_sentinel"
                                     if token["token"] == ZERO else
                                     "error:defillama" if token["token"] in failure_tokens else "unpriced:defillama")])
            token["priced_days"] = got
            token["expected_days"] = n
            stats["rows"] += n
            stats["priced_rows"] += got
            if got:
                stats["priced"] += 1
                stats["fully_priced_tokens" if got == n else "partial_tokens"] += 1
            else:
                stats["unpriced"] += 1
                stats["unpriced_tokens"].append({"token": token["token"], "symbol": token["symbol"],
                                                 "market_count": token["market_count"]})
    os.replace(temp, path)
    stats["missing_rows"] = stats["rows"] - stats["priced_rows"]
    stats["unpriced_tokens"].sort(key=lambda t: (-t["market_count"], t["token"]))
    io_utils._meta_sidecar(path, GENERATOR, meta("Daily token market prices in USD", chain_id=cid,
        rows=stats["rows"], priced_rows=stats["priced_rows"], inputs=input_sources,
        csv_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        confidence_semantics="Per-point confidence if present, otherwise token-level chart confidence"))
    return stats


def quantile(values, p):
    values = sorted(values)
    x = (len(values) - 1) * p
    lo = math.floor(x)
    return values[lo] + (values[math.ceil(x)] - values[lo]) * (x - lo)


def distribution(values):
    if not values:
        return {"n": 0}
    return {"n": len(values), "min": min(values), "mean": statistics.mean(values),
            "p05": quantile(values, .05), "median": statistics.median(values),
            "p95": quantile(values, .95), "max": max(values)}


def stable_validation(tokens):
    direct = [t for t in tokens if t["peg_family"] == "usd_stable" and t["reference"] == "USD"]
    eligible = [t for t in direct if t["priced_days"] > 0]
    selected = random.Random(SEED).sample(sorted(eligible, key=lambda t: (t["chain_id"], t["token"])),
                                         min(20, len(eligible)))
    selected_keys = {(t["chain_id"], t["token"]) for t in selected}
    published = {key: {} for key in selected_keys}
    for cid in sorted({t["chain_id"] for t in selected}):
        with (OUT / f"{cid}.csv").open(newline="") as handle:
            for row in csv.DictReader(handle):
                key = (cid, row["token"])
                if key in selected_keys and row["price_usd"]:
                    published[key][row["date"]] = {"price_usd": float(row["price_usd"])}
    samples = []
    for token in selected:
        quotes = published[(token["chain_id"], token["token"])]
        inside = sum(abs(q["price_usd"] - 1) <= .010000000000001 for q in quotes.values())
        outside = [{"date": date, "price_usd": q["price_usd"]} for date, q in sorted(quotes.items())
                   if abs(q["price_usd"] - 1) > .030000000000001]
        samples.append({"chain_id": token["chain_id"], "token": token["token"], "symbol": token["symbol"],
                        "expected_days": token["expected_days"], "priced_days": len(quotes),
                        "days_within_1pct": inside, "share_within_1pct_of_priced_days": inside / len(quotes) if quotes else None,
                        "share_within_1pct_of_all_days": inside / token["expected_days"] if token["expected_days"] else None,
                        "days_outside_3pct": outside, "outside_3pct_count": len(outside)})
    return {"seed": SEED, "eligible_tokens": len(eligible), "unpriced_direct_usd_tokens_excluded": len(direct) - len(eligible),
            "sampling": "Uniform without replacement over priced chain/token pairs with family usd_stable and reference USD; interest-bearing wrappers excluded",
            "samples": samples}


def chainlink_validation(tokens, audit, fresh=False):
    eth = {t["token"]: t for t in tokens if t["chain_id"] == 1}
    assets = {"ETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
              "BTC": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
              "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}
    if not all(address in eth for address in assets.values()):
        return {"status": "not_run", "reason": "Ethereum output unavailable"}
    path = CACHE / "chainlink_validation.json"
    previous = {} if fresh else read_json(path, {})
    if previous.get("status") == "complete":
        unchanged = True
        for index, (symbol, address) in enumerate(assets.items()):
            cp = read_json(CACHE / "tokens/1" / (address + ".json"), {"chunks": {}})
            quotes, _ = observations(cp, eth[address])
            dates = sorted(random.Random(SEED + index).sample(sorted(quotes), min(30, len(quotes))))
            points = previous.get("assets", {}).get(symbol, {}).get("points", [])
            unchanged &= dates == sorted(p["date"] for p in points)
            unchanged &= all(p["date"] in quotes and p["price_usd"] == quotes[p["date"]]["price_usd"]
                             and p["quote_timestamp"] == quotes[p["date"]]["quote_timestamp"] for p in points)
        if unchanged:
            return previous
    response = requests.get(FEED_SOURCE, timeout=45)
    audit.record(service="chainlink_reference_directory", http_status=response.status_code)
    response.raise_for_status()
    feeds = {r["path"]: r for r in response.json()}
    atomic(CACHE / "chainlink_reference.json", response.json())
    clock = blocktime.BlockClock("ethereum")
    blocks_path = CACHE / "validation_blocks.json"
    blocks = read_json(blocks_path, {})
    result = {"status": "complete", "seed": SEED, "feed_source": FEED_SOURCE,
              "feed_source_official_config": FEED_SOURCE_CODE,
              "reference_sha256": hashlib.sha256(response.content).hexdigest(),
              "sampling": "30 uniformly random priced dates per token, without replacement",
              "relative_difference": "(DefiLlama - Chainlink) / Chainlink; denominator is Chainlink",
              "wrapper_proxies": "WETH represents ETH and WBTC represents BTC; includes wrapper basis",
              "assets": {}}
    for index, (symbol, address) in enumerate(assets.items()):
        feed = feeds[symbol.lower() + "-usd"]
        proxy = feed["proxyAddress"].lower()
        cp = read_json(CACHE / "tokens/1" / (address + ".json"))
        quotes, _ = observations(cp, eth[address])
        dates = random.Random(SEED + index).sample(sorted(quotes), min(30, len(quotes)))
        records = {p["date"]: p for p in previous.get("assets", {}).get(symbol, {}).get("points", [])
                   if p.get("chainlink_price") is not None and p["date"] in quotes
                   and p["price_usd"] == quotes[p["date"]]["price_usd"]
                   and p["quote_timestamp"] == quotes[p["date"]]["quote_timestamp"]}
        for date in sorted(dates):
            if date in records and not fresh:
                continue
            ts = timestamp(dt.date.fromisoformat(date))
            if date not in blocks:
                guess = clock.block_at_verified(ts, tol_secs=0)
                block = rpc.get_block("ethereum", guess)
                block_ts = int(block["timestamp"], 16)
                successor = rpc.get_block("ethereum", guess + 1)
                next_ts = int(successor["timestamp"], 16)
                if not block_ts <= ts < next_ts:
                    lo, hi = (guess, clock.head[1]) if next_ts <= ts else (1, guess)
                    while hi - lo > 1:
                        mid = (lo + hi) // 2
                        if int(rpc.get_block("ethereum", mid)["timestamp"], 16) <= ts:
                            lo = mid
                        else:
                            hi = mid
                    guess = lo
                    block_ts = int(rpc.get_block("ethereum", guess)["timestamp"], 16)
                    next_ts = int(rpc.get_block("ethereum", guess + 1)["timestamp"], 16)
                if not block_ts <= ts < next_ts:
                    raise RuntimeError("Could not verify day's last block")
                blocks[date] = {"block": guess, "block_timestamp": block_ts, "next_block_timestamp": next_ts}
                atomic(blocks_path, blocks)
            b = blocks[date]
            raw = rpc.eth_call("ethereum", proxy, evmabi.selector("latestRoundData()"), block=b["block"])
            round_id, answer, started, updated, answered = evmabi.decode(
                ["uint80", "int256", "uint256", "uint256", "uint80"], raw)
            decimals = int(rpc.eth_call("ethereum", proxy, evmabi.selector("decimals()"), block=b["block"]), 16)
            if answer <= 0 or updated > ts or updated == 0 or answered < round_id:
                raise RuntimeError("Invalid Chainlink round")
            px = answer / 10 ** decimals
            records[date] = {"date": date, **b, **quotes[date], "chainlink_price": px,
                             "chainlink_decimals": decimals, "round_id": str(round_id),
                             "started_at": started, "updated_at": updated,
                             "round_age_seconds": ts - updated, "answered_in_round": str(answered),
                             "relative_difference": (quotes[date]["price_usd"] - px) / px}
            partial = {**result, "status": "incomplete"}
            partial["assets"] = {**result["assets"], symbol: {"points": list(records.values())}}
            atomic(path, partial)
        points = [records[date] for date in sorted(dates)]
        result["assets"][symbol] = {"token": address, "proxy": proxy,
                                     "feed_name": feed["name"], "feed_path": feed["path"],
                                     "points": points,
                                     "relative_difference": distribution([p["relative_difference"] for p in points]),
                                     "absolute_relative_difference": distribution([abs(p["relative_difference"]) for p in points])}
        if len(points) != 30:
            result["status"] = "incomplete"
        print(f"Chainlink validation {symbol} n={len(points)}", flush=True)
    atomic(path, result)
    return result


def validate_outputs(tokens, coverage):
    """Re-read published CSVs to check completeness, uniqueness and provenance."""
    checks = {}
    for cid in sorted({t["chain_id"] for t in tokens}):
        wanted = {t["token"]: t for t in tokens if t["chain_id"] == cid}
        seen = Counter()
        priced = Counter()
        prior_date = {}
        total = 0
        confidence_missing = 0
        for row in csv.DictReader((OUT / f"{cid}.csv").open(newline="")):
            address = row["token"]
            assert address == address.lower() and address in wanted
            token = wanted[address]
            date = dt.date.fromisoformat(row["date"])
            expected = dt.date.fromisoformat(token["start_date"]) + dt.timedelta(days=seen[address])
            assert date == expected and date <= END, (cid, address, date, expected)
            assert row["symbol"] == (token["symbol"] or "")
            assert date > prior_date.get(address, dt.date.min)
            prior_date[address] = date
            seen[address] += 1
            total += 1
            if row["price_usd"]:
                assert math.isfinite(float(row["price_usd"])) and float(row["price_usd"]) >= 0
                assert row["source"] == "defillama:chart" and address != ZERO
                priced[address] += 1
                confidence_missing += not bool(row["confidence"])
            else:
                assert row["confidence"] == "" and row["source"].startswith(("unpriced:", "error:"))
        for address, token in wanted.items():
            assert seen[address] == token["expected_days"]
            assert priced[address] == token["priced_days"]
        c = coverage["chains"][str(cid)]
        assert total == c["rows"] and sum(priced.values()) == c["priced_rows"]
        assert sum(bool(priced[a]) for a in wanted) == c["priced"]
        checks[str(cid)] = {"status": "passed", "rows": total, "tokens": len(wanted),
                            "priced_rows": sum(priced.values()), "priced_rows_missing_confidence": confidence_missing,
                            "date_gaps_or_duplicates": 0}
    return {"chains": checks, "metadata_missing": {
        field: sum(t.get(field) is None for t in tokens) for field in ("symbol", "decimals", "name")},
        "token_kinds": dict(Counter(t.get("token_kind", "unverified") for t in tokens)),
        "decimals_disagreements": [{"chain_id": t["chain_id"], "token": t["token"],
            "symbol": t["symbol"], "onchain_decimals": t["decimals"],
            "defillama_decimals": t["defillama_decimals"]} for t in tokens
            if t.get("decimals") is not None and t.get("defillama_decimals") is not None
            and t["decimals"] != t["defillama_decimals"]]}


def usage(audit):
    events = jsonl(audit.path) if audit.path.exists() else []
    def summarize(rows):
        return {"defillama_requests": sum(r["service"] == "defillama" for r in rows),
                "defillama_http_statuses": dict(Counter(str(r.get("http_status", "transport_error")) for r in rows if r["service"] == "defillama")),
                "defillama_rate_limits": sum(bool(r.get("rate_limited")) for r in rows if r["service"] == "defillama"),
                "defillama_max_completions_in_one_utc_second": max(Counter(
                    r["utc"] for r in rows if r["service"] == "defillama").values(), default=0),
                "chainlink_reference_requests": sum(r["service"] == "chainlink_reference_directory" for r in rows),
                "rpc_requests": sum(r["service"] == "rpc" for r in rows),
                "rpc_methods": dict(Counter(r.get("method") for r in rows if r["service"] == "rpc")),
                "archive_eth_calls": sum(bool(r.get("archive")) for r in rows),
                "archive_eth_call_cost_usd_at_14_per_million": sum(bool(r.get("archive")) for r in rows) * 14 / 1_000_000,
                "rpc_error_codes": dict(Counter(str(r["rpc_error_code"]) for r in rows if r.get("rpc_error_code") is not None)),
                "rpc_rate_limits": sum(bool(r.get("rate_limited")) for r in rows if r["service"] == "rpc"),
                "derived_price_archive_calls": 0,
                "rpc_cost_estimate_usd_at_14_per_million": sum(r["service"] == "rpc" for r in rows) * 14 / 1_000_000}
    return {"run_id": audit.run_id, "this_run": summarize([r for r in events if r["run_id"] == audit.run_id]),
            "all_script_runs": summarize(events),
            "preflight_api_probes": read_json(CACHE / "preflight.json", {}),
            "request_dispatch_policy": "At least 0.26 seconds between DefiLlama request starts per process; one concurrent single-token chain check added one request",
            "journal_timing_note": "utc records completion time. Concurrent responses can complete together; completion bursts do not measure request-start rate. Earlier requests have no recorded start timestamp.",
            "request_journal": ".cache/prices/requests.jsonl"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain", type=int, choices=CHAIN_IDS)
    parser.add_argument("--fresh", action="store_true")
    args = parser.parse_args()
    start = time.monotonic()
    audit = Audit()
    original_post = audit.instrument_rpc()
    OUT.mkdir(parents=True, exist_ok=True)
    selected = [args.chain] if args.chain else CHAIN_IDS
    print(f"run={audit.run_id} chains={selected}", flush=True)
    try:
        tokens, sources, unresolved = universe(selected, args.fresh)
        print(f"universe tokens={len(tokens)} unresolved_euler_relationships={len(unresolved)}", flush=True)
        metadata(tokens, args.fresh)
        llama = Llama(audit)
        failures = download(tokens, llama, args.fresh)
        final_day_refresh = refresh_final_day(tokens, llama)
        coverage = read_json(OUT / "coverage.json", {"chains": {}})
        previous_failures = [r for r in coverage.get("download_failures", []) if r["chain_id"] not in selected]
        previous_unresolved = [r for r in coverage.get("unresolved_euler_collateral_assets", [])
                               if r["chain_id"] not in selected]
        for cid in selected:
            chain_tokens = [t for t in tokens if t["chain_id"] == cid]
            coverage["chains"][str(cid)] = publish_chain(cid, chain_tokens, sources, failures)
        old = jsonl(OUT / "tokens.jsonl") if (OUT / "tokens.jsonl").exists() else []
        tokens += [t for t in old if t["chain_id"] not in selected]
        tokens.sort(key=lambda t: (t["chain_id"], t["token"]))
        prior_meta = read_json(OUT / "tokens.jsonl.meta.json", {})
        all_sources = {r["path"]: r for r in prior_meta.get("inputs", []) + sources}
        io_utils.write_jsonl("prices/tokens.jsonl", tokens, GENERATOR,
                            meta("Token universe, metadata and explicit peg classification", source="in-scope registries, ERC20 eth_call, symbol/address rules", inputs=list(all_sources.values())))
        coverage.update({"tokens_total_definition": "Distinct registry/resolved asset addresses, including explicit zero-address sentinels and nonfungible collateral addresses",
                         "priced_definition": "At least one valid price in the token's requested window",
                         "unpriced_definition": "No valid price in the requested window",
                         "market_count_definition": "Distinct Morpho/Moolah market IDs, Euler lending vault addresses (including own asset vault), Silo config pairs",
                         "family_counts": dict(Counter(t["peg_family"] for t in tokens)),
                         "external_euler_asset_resolutions": {str(cid): {
                             a: {"asset": asset, **read_json(CACHE / f"euler_assets_{cid}.sources.json", {}).get(a, {})}
                             for a, asset in read_json(CACHE / f"euler_assets_{cid}.json", {}).items()}
                             for cid in SCOPE["euler_v2"] if (CACHE / f"euler_assets_{cid}.json").exists()},
                         "download_failures": previous_failures + failures,
                         "unresolved_euler_collateral_assets": previous_unresolved + unresolved})
        io_utils.write_json("prices/coverage.json", coverage, GENERATOR, meta("Price and token coverage"))
        validation = {"stablecoins": stable_validation(tokens), "coverage": coverage["chains"],
                      "top_20_unpriced": sorted([{**t, "chain_id": int(cid)}
                        for cid, c in coverage["chains"].items() for t in c["unpriced_tokens"]],
                        key=lambda t: (-t["market_count"], t["chain_id"], t["token"]))[:20]}
        try:
            validation["chainlink"] = chainlink_validation(tokens, audit, args.fresh and 1 in selected)
        except Exception as exc:
            validation["chainlink"] = {"status": "failed", "error_type": type(exc).__name__,
                                      "checkpoint": ".cache/prices/chainlink_validation.json"}
            print(f"Chainlink validation failed: {type(exc).__name__}", flush=True)
        validation["chain_scope_check"] = read_json(CACHE / "chain_scope_check.json")
        validation["final_day_refresh"] = final_day_refresh
        validation["output_integrity"] = validate_outputs(tokens, coverage)
        validation["usage"] = usage(audit)
        validation["runtime_seconds"] = round(time.monotonic() - start, 3)
        validation["completed_run_history"] = [read_json(p) for p in sorted((CACHE / "runs").glob("*.json"))]
        validation["cumulative_pipeline_runtime_seconds"] = round(validation["runtime_seconds"] + sum(
            r["runtime_seconds"] for r in validation["completed_run_history"]), 3)
        run_ids = [r["run_id"] for r in validation["completed_run_history"]] + [audit.run_id]
        first_run = min(dt.datetime.strptime(r, "%Y%m%dT%H%M%S.%fZ").replace(tzinfo=dt.timezone.utc)
                        for r in run_ids)
        validation["pipeline_first_started_utc"] = first_run.isoformat()
        validation["pipeline_wall_seconds"] = round((dt.datetime.now(dt.timezone.utc) - first_run).total_seconds(), 3)
        validation["limitations"] = [
            "Nearest quote within 12h of day end; see checkpoints for actual quote timestamps.",
            "Price coverage is incomplete. No peg fills or derived wrapper prices.",
            "Zero-address registry sentinels are retained as unpriced entries; DefiLlama native-coin aliases at zero are rejected.",
            "Symbol rules express intended families and are not identity/backing verification.",
            "WETH and WBTC are the Ethereum token proxies used for ETH/BTC validation.",
            "Chainlink USDC/USD may cap upward deviations; this is an oracle comparison, not ground truth.",
            "Reference follows the underlying asset: USD0 tracks USD; bUSD0 (formerly USD0++) tracks USD0, per https://docs.usual.money/resources-and-ecosystem/fact-sheets/usual-products/busd0.",
            "Latest ERC20 metadata may differ from historical metadata.",
            "Euler's eight external Uniswap v4 collateral wrappers resolve to an ERC721 position manager; a fungible token USD price cannot value individual NFT positions.",
            "The existing vault AUM checkpoints contain aggregated monthly valuations, not reusable daily quote records."]
        io_utils.write_json("prices/validation.json", validation, GENERATOR,
                            meta("Seeded Chainlink comparisons, stablecoin deviations and coverage",
                                 source=[API, FEED_SOURCE, "Ethereum archive eth_call"]))
        run = {"run_id": audit.run_id, "built_utc": now(), "chains": selected,
               "runtime_seconds": validation["runtime_seconds"], "usage": validation["usage"]["this_run"]}
        atomic(CACHE / "runs" / (audit.run_id + ".json"), run)
        print(json.dumps({"chains": {cid: {k: v for k, v in c.items() if k != "unpriced_tokens"}
                                     for cid, c in coverage["chains"].items()},
                          "families": coverage["family_counts"], "usage": validation["usage"],
                          "runtime_seconds": validation["runtime_seconds"]}, indent=2), flush=True)
    finally:
        requests.post = original_post


if __name__ == "__main__":
    main()
