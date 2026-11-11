# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""E8 observation stream, market-side extensions of the E1 panel."""
import argparse
import asyncio
import bisect
import csv
import heapq
import io
import json
import os
import pathlib
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common import evmabi, hypersync_client, io_utils, logs, multicall, rpc

GEN = "code/data/panel_ext.py"
DATA = io_utils.DATA
WORK = pathlib.Path(os.environ.get("AUTOCURATOR_WORK", "/tmp/claude_newdata_work")) / "panel_ext"
DAY = 86400
ZERO = "0x" + "0" * 40
ZERO_TOPIC = "0x" + "0" * 64
SCOPE = {"morpho": ["1", "8453", "4663", "42161", "747474", "999"],
         "moolah": ["56"],
         "euler_v2": ["1", "9745", "59144"],
         "silo_v2": ["1", "146", "42161", "43114"]}

T_TRANSFER = evmabi.topic0("Transfer(address,address,uint256)")
T_M_BORROW = evmabi.topic0("Borrow(bytes32,address,address,address,uint256,uint256)")
T_M_REPAY = evmabi.topic0("Repay(bytes32,address,address,uint256,uint256)")
T_S_BORROW = evmabi.topic0("Borrow(address,address,address,uint256,uint256)")
T_S_REPAY = evmabi.topic0("Repay(address,address,uint256,uint256)")
T_PA_SUPPLY = evmabi.topic0("PublicReallocateTo(address,address,bytes32,uint256)")
T_PA_WITHDRAW = evmabi.topic0("PublicWithdrawal(address,address,bytes32,uint256)")

PUBLIC_ALLOCATOR = {
    ("morpho", "1"): "0xfd32fa2ca22c76dd6e550706ad913fc6ce91c75d",
    ("morpho", "8453"): "0xa090dd1a701408df1d4d0b85b716c87565f90467",
    ("morpho", "999"): "0x517505be22d9068687334e69ae7a02fc77edf4fc",
    ("morpho", "42161"): "0x769583af5e9d03589f159ebec31cc2c23e8c355e",
    ("morpho", "747474"): "0x39eb6da5e88194c82b13491df2e8b3e213ed2412",
    ("moolah", "56"): "0x9ecf66f016fcaa853fda24d223bdb4276e5b524a",
}

MAJORS = {
    "1": {"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
          "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
          "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf": "cbBTC",
          "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
          "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
          "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
          "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409": "FDUSD",
          "0xb0f70c0bd6fd87dbeb7c10dc692a2a6106817072": "BTC.b"},
    "8453": {"0x4200000000000000000000000000000000000006": "WETH",
             "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf": "cbBTC",
             "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "USDC",
             "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca": "USDbC",
             "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2": "USDT",
             "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": "DAI",
             "0x0555e30da8f98308edb960aa94c0db47230d2b9c": "WBTC"},
    "42161": {"0x82af49447d8a07e3bd95bd0d56f35241523fbab1": "WETH",
              "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": "WBTC",
              "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf": "cbBTC",
              "0xaf88d065e77c8cc2239327c5edb3a432268e5831": "USDC",
              "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": "USDC.e",
              "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": "USDT0",
              "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": "DAI"},
    "56": {"0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": "WBNB",
           "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c": "BTCB",
           "0x2170ed0880ac9a755fd29b2688956bd959f933f8": "ETH (Binance-Peg)",
           "0x55d398326f99059ff775485246999027b3197955": "USDT (Binance-Peg)",
           "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": "USDC (Binance-Peg)",
           "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409": "FDUSD"},
    "146": {"0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38": "wS",
            "0x50c42deacd8fc9773493ed674b675be577f2634b": "WETH",
            "0x0555e30da8f98308edb960aa94c0db47230d2b9c": "WBTC",
            "0x29219dd400f2bf60e5a23d13be72b486d4038894": "USDC",
            "0x6047828dc181963ba44974801ff68e538da5eaf9": "USDT"},
    "43114": {"0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7": "WAVAX",
              "0x152b9d0fdc40c096757f570a51e494bd4b943e50": "BTC.b",
              "0x0555e30da8f98308edb960aa94c0db47230d2b9c": "WBTC",
              "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab": "WETH.e",
              "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": "USDC",
              "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7": "USDt"},
    "9745": {"0x6100e367285b01f48d07953803a2d8dca5d19873": "WXPL",
             "0x9895d81bb462a195b4922ed7de0e3acd007c32cb": "WETH",
             "0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb": "USDT0"},
    "999": {"0x5555555555555555555555555555555555555555": "WHYPE",
            "0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb": "USDT0",
            "0xb88339cb7199b77e23db6e890353e22632ba630f": "USDC",
            "0x9fdbda0a5e284c32744d2f17ee5c74b284993463": "UBTC (Unit bridged BTC)",
            "0xbe6727b535545c67d5caa73dea54865b92cf7907": "UETH (Unit bridged ETH)"},
    "59144": {"0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f": "WETH",
              "0x176211869ca2b568f2a7d4ee941e073a821ee1ff": "USDC",
              "0xa219439258ca9da29e9cc4ce5596924745e12b93": "USDT",
              "0x3aab2285ddcddad8edf438c1bab47e1a9d05a9b4": "WBTC"},
    "747474": {"0xee7d8bcfb72bc1880d0cf19822eb0a2e6577ab62": "vbETH (WETH)",
               "0x0913da6da4b42f538b445599b46bb4622342cf52": "vbWBTC",
               "0x203a662b0bd271a6ed5a60edfbd04bfce608fd36": "vbUSDC",
               "0x2dca96907fde857dd3d816880a0df407eeb2d2f2": "vbUSDT",
               "0xb0f70c0bd6fd87dbeb7c10dc692a2a6106817072": "BTC.b"},
    "4663": {"0x0bd7d308f8e1639fab988df18a8011f41eacad73": "WETH",
             "0x5fc5360d0400a0fd4f2af552add042d716f1d168": "USDG"},
}
REBASING = {
    "1": {"0x5ee5bf7ae06d1be5997a1a72006fe6c607ec6de8": "aEthWBTC (Aave aToken)",
          "0x856c4efb76c1d1ae02e20ceb03a2a6a08b0b8dc3": "OETH",
          "0x8238884ec9668ef77b90c6dff4d1a9f4f4823bfe": "USDO",
          "0xae7ab96520de3a18e5e111b5eaab095312d7fe84": "stETH"},
}
MISMATCH_TOL = 1e-3
TOKEN_CHUNK = 2000
SPOT_SAMPLE = 12
ERR_FLAG = 0.01
DUST_UNITS = 10_000


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def day_str(d):
    return datetime.fromtimestamp(d * DAY, timezone.utc).strftime("%Y-%m-%d")


def jsonl(path):
    with open(path) as fh:
        return [json.loads(x) for x in fh if x.strip()]


def words(data):
    h = data[2:] if data.startswith("0x") else data
    return [int(h[i:i + 64], 16) for i in range(0, len(h), 64)]


def chain_cfg(cid):
    return logs.load_chains()[str(cid)]


def slug(cid):
    return chain_cfg(cid)["hypersync"]


def save_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj))
    os.replace(tmp, path)


def write_csv(rel, header, rows, meta):
    """Atomic CSV write under data/ with a .meta.json sidecar."""
    path = DATA / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    io_utils._atomic_text(path, buf.getvalue())
    io_utils._meta_sidecar(path, GEN, {**meta, "rows": len(rows),
                                        "columns": list(header)})
    return path


async def _block_ts(chain, block):
    from hypersync import BlockField, FieldSelection, Query
    c = hypersync_client._client(chain)
    r = await c.get(Query(from_block=block, to_block=block + 1,
                          include_all_blocks=True,
                          field_selection=FieldSelection(
                              block=[BlockField.NUMBER, BlockField.TIMESTAMP])))
    for b in r.data.blocks or []:
        if b.number == block:
            t = b.timestamp
            return int(t, 16) if isinstance(t, str) else t
    raise RuntimeError(f"{chain}: no block {block} from HyperSync")


def block_ts(cid, block):
    return asyncio.run(_block_ts(slug(cid), block))


async def _scan_multi_stream(chain, selections, from_block, to_block, on_page):
    from hypersync import FieldSelection, LogSelection, Query
    c = hypersync_client._client(chain)
    sels = [LogSelection(address=a, topics=t) for a, t in selections]
    frm = from_block
    while frm < to_block:
        r = await c.get(Query(from_block=frm, to_block=to_block, logs=sels,
                              field_selection=FieldSelection(
                                  log=hypersync_client.LOG_FIELDS,
                                  block=hypersync_client.BLOCK_FIELDS)))
        ts = {}
        for b in r.data.blocks or []:
            t = b.timestamp
            ts[b.number] = int(t, 16) if isinstance(t, str) else t
        page = []
        for lg in r.data.logs:
            d = hypersync_client._log_to_dict(lg)
            d["timestamp"] = ts.get(lg.block_number)
            page.append(d)
        if page:
            on_page(page)
        if r.next_block <= frm:
            break
        frm = r.next_block


def scan_multi_stream(chain, selections, from_block, to_block, on_page):
    """Stream logs matching any of [(addresses, topics)] in one pass (the shared client takes one selection per query); a log matching two selections comes once."""
    asyncio.run(_scan_multi_stream(chain, selections, from_block, to_block, on_page))


def panel_head(proto, cid):
    """Exclusive end block of the E1 market panel of one protocol and chain."""
    if proto in ("morpho", "moolah"):
        return json.loads((DATA / f"registry/markets/{proto}/{cid}.jsonl.meta.json")
                          .read_text())["scanned_to_block"]
    if proto == "euler_v2":
        return json.loads((DATA / f"panel/euler_v2/{cid}/checkpoint.json")
                          .read_text())["to_block"]
    return json.loads((DATA / f"panel/silo_v2/{cid}/_checkpoint.json")
                      .read_text())["head"]


class Pull:
    """One HyperSync log stream into a work file of CSV lines, checkpointed after every page (next block, file length); a rerun."""

    def __init__(self, name, cid, addresses, topics, from_block, to_block, line):
        self.path = WORK / f"{name}.csv"
        self.ck_path = WORK / f"{name}.ckpt.json"
        self.cid, self.addresses, self.topics = cid, addresses, topics
        self.from_block, self.to_block, self.line = from_block, to_block, line

    def run(self):
        ck = {"next_block": self.from_block, "bytes": 0, "to_block": self.to_block,
              "logs": 0}
        if self.ck_path.exists():
            ck = json.loads(self.ck_path.read_text())
            if ck["to_block"] != self.to_block:
                raise RuntimeError(f"{self.ck_path} scans to {ck['to_block']}, "
                                   f"wanted {self.to_block}; delete it to rebuild")
            if ck.get("done"):
                return ck
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a+b") as fh:
            fh.truncate(ck["bytes"])

        def on_page(page):
            out = [self.line(lg) for lg in page]
            with self.path.open("a") as fh:
                fh.writelines(x for x in out if x)
            ck["next_block"] = page[-1]["block_number"] + 1
            ck["bytes"] = self.path.stat().st_size
            ck["logs"] += len(page)
            save_json(self.ck_path, ck)

        t0 = time.time()
        hypersync_client.scan_logs_stream(
            slug(self.cid), addresses=self.addresses, topics=self.topics,
            from_block=ck["next_block"], to_block=self.to_block, on_page=on_page)
        ck["done"] = True
        ck["bytes"] = self.path.stat().st_size if self.path.exists() else 0
        save_json(self.ck_path, ck)
        print(f"  pulled {self.path.name}: {ck['logs']:,} logs in "
              f"{time.time() - t0:.0f}s", flush=True)
        return ck

    def lines(self):
        with self.path.open() as fh:
            for x in fh:
                yield x.rstrip("\n").split(",")


def concentration(bal):
    """(total, n, top1, top5, top1 holder) of a {holder."""
    if not bal:
        return 0, 0, 0, 0, ""
    top = heapq.nlargest(5, bal.items(), key=lambda kv: kv[1])
    return sum(bal.values()), len(bal), top[0][1], sum(v for _, v in top), top[0][0]


def day_ends(path, ts_col, cols):
    """{day: tuple of int columns} of the last row of each day of a panel CSV."""
    out = {}
    with open(path) as fh:
        next(fh)
        for line in fh:
            f = line.split(",")
            out[int(f[ts_col]) // DAY] = tuple(int(f[c]) for c in cols)
    return out


def borrower_rows(market, d0, d1, deltas, dayend, method, debt_of):
    """Daily rows of one market."""
    bal, rows, errs = {}, [], []
    state, conc = None, (0, 0, 0, 0, "")
    for d in range(d0, d1 + 1):
        moves = deltas.get(d)
        if moves:
            for h, x in moves:
                v = bal.get(h, 0) + x
                if v:
                    bal[h] = v
                else:
                    bal.pop(h, None)
            conc = concentration({h: v for h, v in bal.items() if v > 0})
        state = dayend.get(d, state)
        tot, n, t1, t5, who = conc
        debt, panel_total, dust = debt_of(tot, state)
        if dust or 0 < max(debt, panel_total) < DUST_UNITS:
            errs.append((d, None))
        elif panel_total:
            errs.append((d, (debt - panel_total) / panel_total))
        elif debt:
            errs.append((d, float("inf")))
        if tot > 0:
            rows.append([day_str(d), market, debt, n, f"{t1 / tot:.6f}",
                         f"{t5 / tot:.6f}", who, method])
        else:
            rows.append([day_str(d), market, debt, 0, "", "", "", method])
    return rows, errs, sum(1 for v in bal.values() if v < 0)


def borrowers_morpho(proto, cid):
    entry = json.loads((HERE.parent / "config/deployments.json").read_text())[
        "protocols"][proto]["chains"][cid]
    singleton = entry[proto].lower()
    reg = {r["market_id"]: r for r in jsonl(DATA / f"registry/markets/{proto}/{cid}.jsonl")}
    head = panel_head(proto, cid)

    def line(lg):
        t = lg["topics"]
        w = words(lg["data"])
        if t[0] == T_M_BORROW:
            return f"{t[1]},{lg['timestamp'] // DAY},{evmabi.addr(t[2])},{w[2]}\n"
        if t[0] == T_M_REPAY:
            return f"{t[1]},{lg['timestamp'] // DAY},{evmabi.addr(t[3])},-{w[1]}\n"
        return ""
    pull = Pull(f"borrows/{proto}_{cid}", cid, [singleton], [[T_M_BORROW, T_M_REPAY]],
                min(r["created_block"] for r in reg.values()), head, line)
    ck = pull.run()
    deltas = defaultdict(lambda: defaultdict(list))
    for mid, d, who, x in pull.lines():
        deltas[mid][int(d)].append((who, int(x)))
    pdir = DATA / f"panel/{proto}/{cid}"
    n_liq = Counter()
    with open(pdir / "liquidations.csv") as fh:
        for r in csv.DictReader(fh):
            x = int(r["bad_debt_shares"])
            if r["type"] != "broker":
                x += int(r["repaid_shares"])
            deltas[r["market_id"]][int(r["timestamp"]) // DAY].append((r["borrower"], -x))
            n_liq[r["type"]] += 1
    end_day = (block_ts(cid, head - 1)) // DAY

    def debt_of(tot, st):
        if not st:
            return 0, 0, False
        tba, tbs = st
        if not tbs:
            return 0, tba, tba > 0 and tot == 0
        return tot * tba // tbs, tba, False
    return deltas, reg, end_day, head, debt_of, {
        "source": "hypersync Borrow/Repay of the singleton + panel liquidations.csv",
        "singleton": singleton, "logs_pulled": ck["logs"],
        "liquidations": dict(n_liq)}, \
        lambda m: (pdir / f"{m}.csv", 2, (6, 7)), \
        lambda m: int(reg[m]["created_ts"]) // DAY


def borrowers_silo(cid):
    cfg = {r["silo"]: r for r in jsonl(DATA / f"panel/silo_v2/{cid}/configs.jsonl")}
    head = panel_head("silo_v2", cid)
    lo = min(r["created_block"] for r in cfg.values())
    debt_tok = {r["debt_share_token"]: s for s, r in cfg.items()}

    def tline(lg):
        t = lg["topics"]
        if len(t) != 3:
            return ""
        return (f"{lg['address'].lower()},{lg['timestamp'] // DAY},{evmabi.addr(t[1])},"
                f"{evmabi.addr(t[2])},{words(lg['data'])[0]}\n")

    def eline(lg):
        t = lg["topics"]
        w = words(lg["data"])
        if t[0] == T_S_BORROW:
            return f"{lg['address'].lower()},{lg['timestamp'] // DAY},{w[1]}\n"
        if t[0] == T_S_REPAY:
            return f"{lg['address'].lower()},{lg['timestamp'] // DAY},-{w[1]}\n"
        return ""
    p1 = Pull(f"borrows/silo_v2_{cid}_debt_transfers", cid, sorted(debt_tok),
              [[T_TRANSFER]], lo, head, tline)
    p2 = Pull(f"borrows/silo_v2_{cid}_borrow_repay", cid, sorted(cfg),
              [[T_S_BORROW, T_S_REPAY]], lo, head, eline)
    c1, c2 = p1.run(), p2.run()
    deltas = defaultdict(lambda: defaultdict(list))
    for tok, d, frm, to, v in p1.lines():
        s, d, v = debt_tok[tok], int(d), int(v)
        if frm != ZERO:
            deltas[s][d].append((frm, -v))
        if to != ZERO:
            deltas[s][d].append((to, v))
    supply = defaultdict(lambda: defaultdict(int))
    for s, d, x in p2.lines():
        supply[s][int(d)] += int(x)
    end_day = block_ts(cid, head - 1) // DAY
    pdir = DATA / f"panel/silo_v2/{cid}"

    def dayend_of(s):
        de = day_ends(pdir / f"{s}.csv", 1, (8,)) if (pdir / f"{s}.csv").exists() else {}
        out, run = {}, 0
        days = sorted(set(de) | set(supply[s]))
        last = (0,)
        for d in days:
            run += supply[s].get(d, 0)
            last = de.get(d, last)
            out[d] = (last[0], run)
        return out

    def debt_of(tot, st):
        if not st:
            return 0, 0, False
        assets, shares = st
        if not shares:
            return 0, assets, assets > 0 and tot == 0
        return tot * assets // shares, assets, False
    tokens_final = {s: 0 for s in cfg}
    for s, by_day in deltas.items():
        tokens_final[s] = sum(x for v in by_day.values() for _, x in v)
    return deltas, cfg, end_day, head, debt_of, {
        "source": "hypersync Transfer of each debt share token + Borrow/Repay "
                  "shares of each silo; panel debt_assets",
        "logs_pulled": {"debt_transfers": c1["logs"], "borrow_repay": c2["logs"]},
        "debt_share_token": {s: r["debt_share_token"] for s, r in cfg.items()},
        "_final_supply": tokens_final, "_event_supply": {
            s: sum(supply[s].values()) for s in cfg}}, \
        dayend_of, lambda s: int(cfg[s]["created_ts"]) // DAY


def borrowers_euler(cid):
    rows = jsonl(DATA / f"registry/markets/euler_v2/{cid}.jsonl")
    reg = {r["vault"]: r for r in rows if r["type"] == "vault"}
    head = panel_head("euler_v2", cid)
    vhead = json.loads((DATA / f"vaults/euler_v2/{cid}/head.json").read_text())["block"]
    if vhead + 1 != head:
        raise RuntimeError(f"euler {cid}: debt events end at {vhead}, panel at {head}")
    ev = defaultdict(list)
    kinds = Counter()
    for r in jsonl(DATA / f"vaults/euler_v2/{cid}/events/debt.jsonl"):
        kinds[r["event"]] += 1
        sign = {"Borrow": 1, "InterestAccrued": 1, "Repay": -1}.get(r["event"])
        if sign:
            ev[r["vault"]].append((r["block"], r["log_index"], r["timestamp"],
                                   r["account"], sign * int(r["assets"])))
    pdir = DATA / f"panel/euler_v2/{cid}"
    end_day = block_ts(cid, head - 1) // DAY
    issues = Counter()
    SCALE = 10 ** 36

    def prepare(v):
        """Per-day deltas of index-normalized debt n = D * SCALE / I and the panel's day-end (totalBorrows, accumulator)."""
        acc, dayend = {}, {}
        with open(pdir / f"{v}.csv") as fh:
            next(fh)
            for line in fh:
                f = line.split(",")
                blk, ts, tb, ia = int(f[0]), int(f[2]), int(f[4]), int(f[7])
                acc[blk] = ia
                dayend[ts // DAY] = (tb, ia)
        blocks = sorted(acc)
        D, n = {}, {}
        deltas = defaultdict(list)
        for blk, _li, ts, who, x in sorted(ev[v]):
            D[who] = D.get(who, 0) + x
            ia = acc.get(blk)
            if ia is None:
                issues["touch_without_vault_status"] += 1
                i = bisect.bisect_right(blocks, blk) - 1
                ia = acc[blocks[i]] if i >= 0 else 10 ** 27
            new = D[who] * SCALE // ia if D[who] > 0 else 0
            if D[who] < 0:
                issues["negative_account_debt"] += 1
            deltas[ts // DAY].append((who, new - n.get(who, 0)))
            n[who] = new
        return deltas, dayend

    def debt_of(tot, st):
        if not st:
            return 0, 0, False
        tb, ia = st
        return tot * ia // SCALE, tb, False
    return ev, reg, end_day, head, debt_of, {
        "source": "data/vaults/euler_v2/<chain>/events/debt.jsonl (HyperSync, "
                  "code/data/vaults_euler.py) + panel VaultStatus",
        "debt_events": dict(kinds), "_issues": issues}, prepare, \
        lambda v: int(reg[v]["created_ts"]) // DAY


def build_borrowers(proto, cid):
    t0 = time.time()
    print(f"borrowers {proto}:{cid}", flush=True)
    if proto in ("morpho", "moolah"):
        deltas, reg, end_day, head, debt_of, meta, src, start = borrowers_morpho(proto, cid)
        method = "shares"
        markets = sorted(m for m in deltas if m in reg)

        def market_inputs(m):
            path, tsc, cols = src(m)
            return deltas[m], day_ends(path, tsc, cols)
    elif proto == "silo_v2":
        deltas, reg, end_day, head, debt_of, meta, dayend_of, start = borrowers_silo(cid)
        method = "shares"
        markets = sorted(s for s in deltas if s in reg)

        def market_inputs(s):
            return deltas[s], dayend_of(s)
    else:
        ev, reg, end_day, head, debt_of, meta, prepare, start = borrowers_euler(cid)
        method = "index_normalized"
        markets = sorted(v for v in ev if v in reg and ev[v])

        def market_inputs(v):
            return prepare(v)
    out, errs_all, worst, neg_markets = [], [], {}, {}
    for m in markets:
        dl, de = market_inputs(m)
        d0 = min([start(m)] + list(dl))
        rows, errs, neg = borrower_rows(m, d0, end_day, dl, de, method, debt_of)
        out.extend(rows)
        if neg:
            neg_markets[m] = neg
        if errs:
            errs_all.extend(e for _d, e in errs)
            bad = [(d, e) for d, e in errs if e is not None and abs(e) > ERR_FLAG]
            if bad:
                worst[m] = {"days_off": len(bad), "days": len(errs),
                            "max_abs_rel_err": max(abs(e) for _d, e in bad),
                            "first_day_off": day_str(bad[0][0]),
                            "last_day_off": day_str(bad[-1][0]),
                            "final_rel_err": errs[-1][1]}
    out.sort(key=lambda r: (r[0], r[1]))
    finite = sorted(abs(e) for e in errs_all if e is not None and e != float("inf"))

    def q(p):
        return finite[min(len(finite) - 1, int(p * len(finite)))] if finite else None
    val = {"market_days_compared": len(errs_all),
           "dust_days": sum(1 for e in errs_all if e is None),
           "dust_note": f"both totals under {DUST_UNITS} raw units, or a panel residue "
                        "against zero shares; not compared",
           "infinite": sum(1 for e in errs_all if e == float("inf")),
           "exact": sum(1 for e in finite if e == 0),
           "abs_rel_err_quantiles": {"p50": q(.5), "p90": q(.9), "p99": q(.99),
                                     "max": finite[-1] if finite else None},
           "over_1e-6": sum(1 for e in finite if e > 1e-6),
           "over_1pct": sum(1 for e in finite if e > ERR_FLAG),
           "markets_over_1pct": worst,
           "markets_with_negative_holder_balance": neg_markets}
    issues = meta.pop("_issues", None)
    if issues:
        val["issues"] = dict(issues)
    if proto == "silo_v2":
        val["share_supply_vs_head"] = silo_head_check(cid, head, meta.pop("_final_supply"),
                                                      meta.pop("_event_supply"),
                                                      meta["debt_share_token"])
    write_csv(f"panel_ext/borrowers/{proto}/{cid}.csv",
              ["day", "market", "debt_total", "n_borrowers", "top1_share",
               "top5_share", "top1_borrower", "method"], out,
              {"protocol": proto, "chain_id": int(cid), "method": method,
               "scanned_to_block_exclusive": head, "last_day": day_str(end_day),
               "last_day_note": "the last day's row is the state at the scan head",
               "markets": len(markets), "markets_rule": "markets with at least one "
               "borrow; rows from the creation day to the last day",
               "units": "debt_total in loan-token raw units; shares of debt_total; "
                        "empty shares when nobody owes",
               "validation_debt_total_vs_panel": val, **meta,
               "rpc_calls": rpc.calls_made(), "build_seconds": round(time.time() - t0)})
    print(f"  {len(out):,} rows, {len(markets)} markets; abs rel err p50 {q(.5)} "
          f"p99 {q(.99)} max {val['abs_rel_err_quantiles']['max']}; "
          f"markets >1%: {len(worst)}", flush=True)
    return {"rows": len(out), "markets": len(markets), "validation": val}


def silo_head_check(cid, head, final, event_supply, dtok):
    """Tracked debt-share supply (Transfer) and Borrow - Repay share sums against totalSupply() of each debt share token at the last scanned block."""
    silos = sorted(final)
    res = multicall.aggregate3(logs.rpc_target(chain_cfg(cid)),
                               [(dtok[s], evmabi.selector("totalSupply()")) for s in silos],
                               block=hex(head - 1))
    out = {"tokens": len(silos), "transfer_equal": 0, "events_equal": 0,
           "failed_calls": 0, "differences": []}
    for s, (ok, ret) in zip(silos, res):
        if not ok or ret in ("0x", ""):
            out["failed_calls"] += 1
            continue
        ts = int(ret, 16)
        out["transfer_equal"] += final[s] == ts
        out["events_equal"] += event_supply[s] == ts
        if final[s] != ts or event_supply[s] != ts:
            out["differences"].append({"silo": s, "totalSupply": str(ts),
                                       "transfer_sum": str(final[s]),
                                       "borrow_repay_sum": str(event_supply[s])})
    return out


def collateral_tokens():
    """{chain: {token: set of 'protocol' that take it as collateral}}, and Euler collateral vaults without an asset (left out)."""
    tok = defaultdict(lambda: defaultdict(set))
    skipped = defaultdict(list)
    for proto in ("morpho", "moolah"):
        for cid in SCOPE[proto]:
            for r in jsonl(DATA / f"registry/markets/{proto}/{cid}.jsonl"):
                if r["collateral_token"] != ZERO:
                    tok[cid][r["collateral_token"]].add(proto)
    for cid in SCOPE["euler_v2"]:
        rows = jsonl(DATA / f"registry/markets/euler_v2/{cid}.jsonl")
        asset = {r["vault"]: r["asset"] for r in rows if r["type"] == "vault"}
        for r in rows:
            if r["type"] != "ltv":
                continue
            a = asset.get(r["collateral_vault"])
            if a:
                tok[cid][a].add("euler_v2")
            elif r["collateral_vault"] not in skipped[cid]:
                skipped[cid].append(r["collateral_vault"])
    for cid in SCOPE["silo_v2"]:
        for r in jsonl(DATA / f"panel/silo_v2/{cid}/configs.jsonl"):
            tok[cid][r["token"]].add("silo_v2")
    return tok, skipped


def chain_head(cid):
    """Latest panel end block over the protocols in scope on a chain."""
    return max(panel_head(p, cid) for p, cs in SCOPE.items() if cid in cs)


def build_token_supply(cid):
    t0 = time.time()
    print(f"token supply {cid}", flush=True)
    tok_all, skipped = collateral_tokens()
    toks = tok_all[cid]
    head = chain_head(cid)
    majors = MAJORS.get(cid, {})
    tokens = sorted(t for t in toks if t not in majors)
    ck_path = WORK / f"supply/{cid}.json"
    st = {"head": head, "done_chunks": [], "agg": {}}
    if ck_path.exists():
        st = json.loads(ck_path.read_text())
        if st["head"] != head or st.get("tokens") != tokens:
            raise RuntimeError(f"{ck_path} is for another head or token set; delete it")
    st["tokens"] = tokens
    agg = st["agg"]
    chunks = [tokens[i:i + TOKEN_CHUNK] for i in range(0, len(tokens), TOKEN_CHUNK)]
    for k, chunk in enumerate(chunks):
        key = str(k)
        if key in st["done_chunks"]:
            continue
        part = defaultdict(dict)

        def on_page(page, part=part):
            for lg in page:
                t = lg["topics"]
                if len(t) != 3:
                    continue
                cell = part[lg["address"].lower()].setdefault(
                    str(lg["timestamp"] // DAY), [0, 0, 0])
                v = words(lg["data"])[0]
                if t[1] == ZERO_TOPIC:
                    cell[0] += v
                elif t[2] == ZERO_TOPIC:
                    cell[1] += v
                cell[2] = max(cell[2], lg["block_number"])
        scan_multi_stream(slug(cid), [(chunk, [[T_TRANSFER], [ZERO_TOPIC]]),
                                      (chunk, [[T_TRANSFER], [], [ZERO_TOPIC]])],
                          0, head, on_page)
        for a, days in part.items():
            agg[a] = days
        st["done_chunks"].append(key)
        save_json(ck_path, st)
        print(f"  chunk {k + 1}/{len(chunks)}: {len(part)} tokens with logs", flush=True)
    end_day = block_ts(cid, head - 1) // DAY
    final = {t: sum(m - b for m, b, _ in agg.get(t, {}).values()) for t in tokens}
    target = logs.rpc_target(chain_cfg(cid))
    res = multicall.aggregate3(target, [(t, evmabi.selector("totalSupply()"))
                                        for t in tokens], block=hex(head - 1))
    check, excluded = {}, {}
    for t, (ok, ret) in zip(tokens, res):
        if not ok or ret in ("0x", "") or len(ret) != 66:
            check[t] = {"status": "no_totalSupply"}
            continue
        ts = int(ret, 16)
        gap = abs(final[t] - ts) / ts if ts else (0.0 if final[t] == 0 else float("inf"))
        check[t] = {"status": "exact" if final[t] == ts else
                    ("within_tol" if gap <= MISMATCH_TOL else "mismatch"),
                    "totalSupply": str(ts), "reconstructed": str(final[t]),
                    "rel_gap": gap}
    for t, c in check.items():
        if t in REBASING.get(cid, {}):
            excluded[t] = {"reason": "rebasing", "symbol": REBASING[cid][t], **c}
        elif c["status"] == "mismatch":
            excluded[t] = {"reason": "zero-address Transfer sum differs from "
                           "totalSupply() at the head (rebasing, or mints without "
                           "a zero-address Transfer)", **c}
        elif c["status"] == "no_totalSupply":
            excluded[t] = {"reason": "totalSupply() reverts or is not a uint256 "
                           "at the head; supply not verifiable", **c}
        elif not agg.get(t):
            excluded[t] = {"reason": "never minted: no zero-address Transfer logs"
                           + (" and totalSupply() 0" if c.get("totalSupply") == "0" else ""),
                           **c}
    rows = []
    for t in tokens:
        if t in excluded:
            continue
        days = agg[t]
        meth = "zero_transfer" if check[t]["status"] == "exact" else "zero_transfer_approx"
        d0 = min(int(d) for d in days)
        run = 0
        for d in range(d0, end_day + 1):
            m, b, _ = days.get(str(d), (0, 0, 0))
            run += m - b
            rows.append([day_str(d), t, run, m, b, meth])
    rows.sort(key=lambda r: (r[0], r[1]))
    spot = supply_spot_checks(cid, tokens, excluded, agg, target)
    for t, sym in majors.items():
        if t in toks:
            excluded[t] = {"reason": "major: wrapped native, fiat stablecoin, BTC "
                           "wrapper or bridged ETH; supply follows wrapping, bridging "
                           "or the issuer, not market risk", "symbol": sym}
    status = Counter(c["status"] for c in check.values())
    write_csv(f"panel_ext/token_supply/{cid}.csv",
              ["day", "token", "total_supply", "minted", "burned", "method"], rows,
              {"chain_id": int(cid), "scanned_to_block_exclusive": head,
               "from_block": 0, "last_day": day_str(end_day),
               "last_day_note": "the last day's row is the state at the scan head",
               "source": "hypersync ERC20 Transfer from / to the zero address",
               "units": "raw token units; minted and burned are the day's amounts",
               "methods": {"zero_transfer": "reconstructed supply equals "
                           "totalSupply() at the head",
                           "zero_transfer_approx": f"within {MISMATCH_TOL} relative "
                                                   "(gap per token in head_check_by_token)"},
               "tokens_in_scope": len(toks), "tokens_written": len(tokens) - len(
                   [t for t in excluded if t in tokens]),
               "head_check": dict(status), "head_check_by_token": check,
               "spot_checks_past_day_end": spot,
               "excluded": excluded,
               "euler_collateral_vaults_without_asset": skipped.get(cid, []),
               "protocols_by_token": {t: sorted(p) for t, p in toks.items()},
               "rpc_calls": rpc.calls_made(), "build_seconds": round(time.time() - t0)})
    print(f"  {len(rows):,} rows, {len(tokens) - len(set(excluded) & set(tokens))} tokens; "
          f"head check {dict(status)}; spot {spot['agree']}/{spot['checked']}", flush=True)
    return {"rows": len(rows), "head_check": dict(status), "spot": spot,
            "excluded": len(excluded)}


def supply_spot_checks(cid, tokens, excluded, agg, target):
    """totalSupply() at the block of a token's last mint or burn on a past day, against the reconstructed day-end supply of that day."""
    cand = sorted((t for t in tokens if t not in excluded and agg.get(t)),
                  key=lambda t: -len(agg[t]))[:SPOT_SAMPLE]
    out = {"checked": 0, "agree": 0, "rows": []}
    for t in cand:
        days = sorted(agg[t], key=int)
        d = days[len(days) // 2]
        run = sum(agg[t][x][0] - agg[t][x][1] for x in days if int(x) <= int(d))
        blk = agg[t][d][2]
        ok, ret = multicall.aggregate3(target, [(t, evmabi.selector("totalSupply()"))],
                                       block=hex(blk))[0]
        got = int(ret, 16) if ok and len(ret) == 66 else None
        out["checked"] += 1
        out["agree"] += got == run
        out["rows"].append({"token": t, "day": day_str(int(d)), "block": blk,
                            "reconstructed": str(run),
                            "totalSupply": None if got is None else str(got)})
    return out


def build_public_allocator(proto, cid):
    t0 = time.time()
    print(f"public allocator {proto}:{cid}", flush=True)
    pa = PUBLIC_ALLOCATOR.get((proto, cid))
    meta_in = json.loads((DATA / f"vaults/{proto}/{cid}/actions.jsonl.meta.json").read_text())
    head = meta_in["head_block"]
    header = ["day", "market", "assets_in", "assets_out", "n_tx"]
    rel = f"panel_ext/public_allocator/{proto}/{cid}.csv"
    if pa is None:
        n, _ = hypersync_client.scan_logs(slug(cid), topics=[[T_PA_SUPPLY, T_PA_WITHDRAW]],
                                          to_block=head)
        write_csv(rel, header, [], {
            "protocol": proto, "chain_id": int(cid), "public_allocator": None,
            "note": "no PublicReallocateTo or PublicWithdrawal log from any address "
                    f"on this chain before block {head}: no public allocator in use",
            "census_logs": len(n), "scanned_to_block_exclusive": head})
        print(f"  no allocator; census logs {len(n)}", flush=True)
        return {"rows": 0, "public_allocator": None}
    agg = defaultdict(lambda: [0, 0, set()])
    other_layout = Counter()
    needle = pa.encode()
    with open(DATA / f"vaults/{proto}/{cid}/actions.jsonl", "rb") as fh:
        for line in fh:
            if needle not in line or b'"Reallocate' not in line:
                continue
            r = json.loads(line)
            ev, f = r["event"], r["fields"]
            if ev not in ("ReallocateSupply", "ReallocateWithdraw") or f.get("caller") != pa:
                continue
            if r["block"] >= head:
                continue
            m = f.get("id")
            if m is None:
                other_layout[ev] += 1
                continue
            cell = agg[(r["timestamp"] // DAY, m)]
            if ev == "ReallocateSupply":
                cell[0] += int(f["suppliedAssets"])
            else:
                cell[1] += int(f["withdrawnAssets"])
            cell[2].add(r["tx_hash"])
    rows = sorted([day_str(d), m, a, b, len(tx)] for (d, m), (a, b, tx) in agg.items())
    raw, _ = hypersync_client.scan_logs(slug(cid), addresses=[pa],
                                        topics=[[T_PA_SUPPLY, T_PA_WITHDRAW]], to_block=head)
    own = defaultdict(lambda: [0, 0])
    for lg in raw:
        k = (lg["timestamp"] // DAY, lg["topics"][3])
        own[k][0 if lg["topics"][0] == T_PA_SUPPLY else 1] += words(lg["data"])[0]
    keys = set(own) | set(agg)
    diff = [k for k in keys if own.get(k, [0, 0]) != agg.get(k, [0, 0, None])[:2]]
    val = {"allocator_logs": len(raw), "market_days": len(keys),
           "market_days_equal": len(keys) - len(diff),
           "differences": [{"day": day_str(d), "market": m, "vault_logs": agg.get(
               (d, m), [0, 0])[:2], "allocator_logs": own.get((d, m), [0, 0])}
               for d, m in sorted(diff)[:20]],
           "total_in_vault_logs": str(sum(c[0] for c in agg.values())),
           "total_in_allocator_logs": str(sum(c[0] for c in own.values())),
           "total_out_vault_logs": str(sum(c[1] for c in agg.values())),
           "total_out_allocator_logs": str(sum(c[1] for c in own.values()))}
    write_csv(rel, header, rows, {
        "protocol": proto, "chain_id": int(cid), "public_allocator": pa,
        "source": f"data/vaults/{proto}/{cid}/actions.jsonl (ReallocateSupply, "
                  "ReallocateWithdraw with caller = public allocator)",
        "scanned_to_block_exclusive": head,
        "rows_rule": "only (day, market) pairs with a flow",
        "units": "loan-token raw units", "events_with_address_market_skipped":
            dict(other_layout), "validation_against_allocator_logs": val,
        "build_seconds": round(time.time() - t0)})
    print(f"  {len(rows):,} rows; allocator logs {len(raw):,}; market-days equal "
          f"{val['market_days_equal']}/{val['market_days']}", flush=True)
    return {"rows": len(rows), "validation": val}


def pairs(proto, cid, protos):
    for p in protos:
        if proto and p != proto:
            continue
        for c in SCOPE[p]:
            if cid and c != cid:
                continue
            yield p, c


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("what", choices=["borrowers", "token-supply", "public-allocator", "all"])
    ap.add_argument("--protocol")
    ap.add_argument("--chain")
    a = ap.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    summary = {}
    if a.what in ("borrowers", "all"):
        for p, c in pairs(a.protocol, a.chain, SCOPE):
            summary[f"borrowers {p}:{c}"] = build_borrowers(p, c)["rows"]
    if a.what in ("token-supply", "all"):
        chains = sorted({c for cs in SCOPE.values() for c in cs}, key=int)
        for c in chains:
            if a.chain and c != a.chain:
                continue
            summary[f"token_supply {c}"] = build_token_supply(c)["rows"]
    if a.what in ("public-allocator", "all"):
        for p, c in pairs(a.protocol, a.chain, ("morpho", "moolah")):
            summary[f"public_allocator {p}:{c}"] = build_public_allocator(p, c)["rows"]
    print(json.dumps({"rows": summary, "rpc_calls": rpc.calls_made(),
                      "written_utc": now()}, indent=1))


if __name__ == "__main__":
    main()
