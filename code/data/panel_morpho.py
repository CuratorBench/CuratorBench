# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""E1, market state panel for Morpho Blue and its fork Moolah."""
import argparse
import bisect
import csv
import json
import os
import pathlib
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from itertools import accumulate, takewhile, zip_longest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common import evmabi, hypersync_client, io_utils, logs, multicall, rpc

GENERATOR = "code/data/panel_morpho.py"
CONFIG = HERE.parent / "config"
SINGLETON_KEY = {"morpho": "morpho", "moolah": "moolah"}
OLD_PANEL = io_utils.DATA / "markets"

EVENTS = {
    "create": "CreateMarket(bytes32,(address,address,address,address,uint256))",
    "supply": "Supply(bytes32,address,address,uint256,uint256)",
    "withdraw": "Withdraw(bytes32,address,address,address,uint256,uint256)",
    "borrow": "Borrow(bytes32,address,address,address,uint256,uint256)",
    "repay": "Repay(bytes32,address,address,uint256,uint256)",
    "accrue": "AccrueInterest(bytes32,uint256,uint256,uint256)",
    "liquidate": "Liquidate(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)",
    "supply_collateral": "SupplyCollateral(bytes32,address,address,uint256)",
    "withdraw_collateral": "WithdrawCollateral(bytes32,address,address,address,uint256)",
    "set_fee": "SetFee(bytes32,uint256)",
}
KIND = {evmabi.topic0(sig): kind for kind, sig in EVENTS.items()}
T_BROKER = evmabi.topic0("SetMarketBroker(bytes32,address,bool)")
T_BRU = evmabi.topic0("BorrowRateUpdate(bytes32,uint256,uint256)")
IRM_PARAM = {
    evmabi.topic0("BorrowRateCapUpdate(bytes32,uint256,uint256)"): "rate_cap",
    evmabi.topic0("BorrowRateFloorUpdate(bytes32,uint256,uint256)"): "rate_floor",
    evmabi.topic0("MinCapUpdate(uint256,uint256)"): "min_cap",
}
TOPICS = list(KIND) + [T_BROKER, T_BRU] + list(IRM_PARAM)
FEE_AT_CREATE = {"moolah"}
TOUCHES_LAST_UPDATE = {"create", "supply", "withdraw", "borrow", "repay",
                       "accrue", "liquidate", "withdraw_collateral",
                       "set_fee"}
OLD_KINDS = {"create", "supply", "withdraw", "borrow", "repay", "accrue",
             "liquidate"}

HEADER = ("block,log_index,timestamp,kind,total_supply_assets,"
          "total_supply_shares,total_borrow_assets,total_borrow_shares,"
          "total_collateral,borrow_rate,rate_at_target,fee\n")
SIDE_HEADERS = {
    "liquidations.csv": (
        "market_id,block,log_index,timestamp,tx_hash,caller,borrower,type,"
        "repaid_assets,repaid_shares,seized_assets,bad_debt_assets,"
        "bad_debt_shares\n"),
    "irm_params.csv": (
        "irm,market_id,block,log_index,timestamp,param,old,new\n"),
    "brokers.csv": (
        "market_id,block,log_index,timestamp,broker,is_addition\n"),
}
FLUSH_ROWS = 1_000_000
FLUSH_SECS = 60
BYTES_PER_ROW = 140
EST_WINDOWS = 8
EST_FRACTION = 0.01
HEAD_CHUNK = 300
SAMPLE_PER_CHAIN = 200


class Stop(Exception):
    """Raised from the page callback to end a run cleanly at a deadline."""


def words(data: str):
    h = data[2:] if data.startswith("0x") else data
    return [int(h[i:i + 64], 16) for i in range(0, len(h), 64)]


def setup(protocol: str, cid: str):
    """Chain config, singleton, registry markets and their IRMs."""
    ccfg = logs.load_chains()[cid]
    entry = json.loads((CONFIG / "deployments.json").read_text())[
        "protocols"][protocol]["chains"][cid]
    singleton = entry[SINGLETON_KEY[protocol]].lower()
    rel = f"registry/markets/{protocol}/{cid}.jsonl"
    markets = io_utils.read_jsonl(rel)
    reg_meta = json.loads((io_utils.DATA / (rel + ".meta.json")).read_text())
    irms = sorted({m["irm"] for m in markets} - {"0x" + "0" * 40})
    return ccfg, singleton, markets, irms, reg_meta["scanned_to_block"]


def out_dir(protocol, cid) -> pathlib.Path:
    return io_utils.DATA / "panel" / protocol / cid


def estimate(protocol, cid):
    """Event count by uniform window sampling and the implied CSV size."""
    ccfg, singleton, markets, irms, to_block = setup(protocol, cid)
    lo = min(m["created_block"] for m in markets)
    span = to_block - lo
    width = max(1, int(span * EST_FRACTION / EST_WINDOWS))
    step = span // EST_WINDOWS
    sel = [([singleton] + irms, [TOPICS])]
    hits = 0
    for i in range(EST_WINDOWS):
        a = lo + i * step + (step - width) // 2
        hits += len(hypersync_client.scan_logs_multi(
            ccfg["hypersync"], sel, a, a + width))
        print(f"  window {i + 1}/{EST_WINDOWS}: {hits:,} logs", flush=True)
    events = int(hits * span / (width * EST_WINDOWS))
    gb = events * BYTES_PER_ROW / 1e9
    print(f"{protocol}:{cid} markets={len(markets)} blocks {lo}..{to_block} "
          f"est_logs={events:,} est_size={gb:.2f}GB "
          f"(sampled {hits:,} logs in {EST_WINDOWS} windows of {width})",
          flush=True)
    return events, gb


class Builder:
    def __init__(self, protocol, cid):
        self.protocol, self.cid = protocol, cid
        (self.ccfg, self.singleton, markets, self.irms,
         self.to_block) = setup(protocol, cid)
        self.irm_set = set(self.irms)
        self.dir = out_dir(protocol, cid)
        self.ckpt_path = self.dir / "_checkpoint.json"
        self.registry = {m["market_id"]: m for m in markets}
        self.buf = {}
        self.buffered = 0
        self.last_flush = time.time()
        self.pending = {}
        self.created = {}

    def fresh(self):
        self.dir.mkdir(parents=True, exist_ok=True)
        self.ck = {
            "protocol": self.protocol, "chain_id": int(self.cid),
            "singleton": self.singleton, "irms": self.irms,
            "from_block": min(m["created_block"]
                              for m in self.registry.values()),
            "to_block": self.to_block, "done": False,
            "state": {}, "rows": {}, "bytes": {},
            "counts": {k: 0 for k in EVENTS},
            "issues": {"unknown_market": 0, "negative_state": 0,
                       "accrue_without_bru": 0, "bru_without_accrue": 0,
                       "bru_rate_differs": 0, "missing_timestamp": 0,
                       "broker_liquidation_ambiguous": 0,
                       "create_fee_unread": 0},
            "examples": [],
            "bru_pairing": {},
            "brokers": {},
            "create_fee": (self.creation_fees()
                           if self.protocol in FEE_AT_CREATE else {}),
        }
        self.ck["next_block"] = self.ck["from_block"]
        for f in self.dir.glob("*.csv"):
            f.unlink()
        self._save_ckpt()

    def creation_fees(self):
        """market(id).fee at the end of each market's creation block, for protocols whose createMarket sets a fee without emitting SetFee."""
        by_block = {}
        for mid, m in self.registry.items():
            by_block.setdefault(m["created_block"], []).append(mid)
        target = logs.rpc_target(self.ccfg)

        def read(item):
            b, mids = item
            got = multicall.aggregate3(
                target, [(self.singleton, market_call(m)) for m in mids],
                block=b)
            return [(m, decode_market(r)[5]) for m, (ok, r) in zip(mids, got)
                    if ok]
        fees = {}
        with ThreadPoolExecutor(max_workers=8) as ex:
            for res in ex.map(read, by_block.items()):
                fees.update(res)
        print(f"creation fees read for {len(fees)} markets in "
              f"{len(by_block)} blocks", flush=True)
        return fees

    def resume(self):
        self.ck = json.loads(self.ckpt_path.read_text())
        if self.ck["to_block"] != self.to_block:
            raise RuntimeError(
                f"checkpoint scans to {self.ck['to_block']} but the registry "
                f"is at {self.to_block}; delete {self.dir} to rebuild")
        for f in self.dir.glob("*.csv"):
            size = self.ck["bytes"].get(f.name)
            if size is None:
                f.unlink()
            elif f.stat().st_size != size:
                with f.open("r+b") as fh:
                    fh.truncate(size)

    def _save_ckpt(self):
        tmp = self.ckpt_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.ck))
        os.replace(tmp, self.ckpt_path)

    def _emit(self, fname, line):
        self.buf.setdefault(fname, []).append(line)
        self.buffered += 1

    def flush(self, next_block):
        for fname, lines in self.buf.items():
            path = self.dir / fname
            new = fname not in self.ck["bytes"]
            with path.open("w" if new else "a") as fh:
                if new:
                    fh.write(SIDE_HEADERS.get(fname, HEADER))
                fh.writelines(lines)
            self.ck["bytes"][fname] = path.stat().st_size
        self.buf, self.buffered = {}, 0
        self.ck["next_block"] = next_block
        self._save_ckpt()
        self.last_flush = time.time()

    def _issue(self, key, lg, note=""):
        self.ck["issues"][key] += 1
        if self.ck["issues"][key] <= 10:
            self.ck["examples"].append({
                "issue": key, "block": lg["block_number"],
                "log_index": lg["log_index"], "tx": lg["tx_hash"],
                "note": note})

    def handle(self, lg):
        topics = lg["topics"]
        addr = lg["address"].lower()
        t0 = topics[0]
        blk, li, ts = lg["block_number"], lg["log_index"], lg["timestamp"]
        if addr in self.irm_set:
            if t0 == T_BRU:
                mid = topics[1]
                if mid in self.created and self.created[mid][1] == blk:
                    self._patch_create(mid, words(lg["data"])[1])
                else:
                    self.pending[mid] = (blk, li, words(lg["data"]))
            elif t0 in IRM_PARAM:
                old, new = words(lg["data"])
                mid = topics[1] if len(topics) > 1 else ""
                self._emit("irm_params.csv", f"{addr},{mid},{blk},{li},{ts},"
                           f"{IRM_PARAM[t0]},{old},{new}\n")
            return
        if addr != self.singleton:
            return
        if t0 == T_BROKER:
            mid, broker = topics[1], evmabi.addr(topics[2])
            add = words(lg["data"])[0] == 1
            brokers = set(self.ck["brokers"].get(mid, []))
            brokers = brokers | {broker} if add else brokers - {broker}
            self.ck["brokers"][mid] = sorted(brokers)
            self._emit("brokers.csv", f"{mid},{blk},{li},{ts},{broker},"
                       f"{int(add)}\n")
            return
        kind = KIND.get(t0)
        if kind is None:
            return
        mid = topics[1]
        st = self.ck["state"].get(mid)
        if st is None:
            if kind != "create" or mid not in self.registry:
                self._issue("unknown_market", lg, mid)
                return
            st = self.ck["state"][mid] = [0, 0, 0, 0, 0, 0, 0]
            self.ck["rows"][mid] = 0
        if ts is None:
            self._issue("missing_timestamp", lg)
            ts = ""
        w = words(lg["data"]) if kind != "create" else []
        rate = rat = fee = ""
        if kind == "supply":
            st[0] += w[0]
            st[1] += w[1]
        elif kind == "withdraw":
            st[0] -= w[1]
            st[1] -= w[2]
        elif kind == "borrow":
            st[2] += w[1]
            st[3] += w[2]
        elif kind == "repay":
            st[2] = max(st[2] - w[0], 0)
            st[3] -= w[1]
        elif kind == "accrue":
            rate_v, interest, fee_shares = w
            st[0] += interest
            st[1] += fee_shares
            st[2] += interest
            rate = rate_v
            bru = self.pending.pop(mid, None)
            if bru and bru[0] == blk and bru[1] < li:
                rat = bru[2][1]
                if bru[2][0] != rate_v:
                    self._issue("bru_rate_differs", lg, mid)
            else:
                if bru:
                    self._issue("bru_without_accrue", lg, mid)
                self._issue("accrue_without_bru", lg, mid)
            pairing = self.ck["bru_pairing"].setdefault(
                self.registry[mid]["irm"], [0, 0])
            pairing[rat == ""] += 1
        elif kind == "liquidate":
            rep_a, rep_s, seized, bad_a, bad_s = w
            caller = evmabi.addr(topics[2])
            by_broker = caller in self.ck["brokers"].get(mid, [])
            pattern = seized == 0 and rep_a == bad_a and rep_s == bad_s
            if by_broker != pattern and (by_broker or bad_s):
                self._issue("broker_liquidation_ambiguous", lg, mid)
            if by_broker and pattern:
                kind = "broker_liquidate"
            else:
                st[2] = max(st[2] - rep_a, 0)
                st[3] -= rep_s
                st[4] -= seized
            st[2] -= bad_a
            st[0] -= bad_a
            st[3] -= bad_s
            self._emit("liquidations.csv", (
                f"{mid},{blk},{li},{ts},{lg['tx_hash']},{caller},"
                f"{evmabi.addr(topics[3])},"
                f"{'broker' if kind == 'broker_liquidate' else 'standard'},"
                f"{rep_a},{rep_s},{seized},{bad_a},{bad_s}\n"))
        elif kind == "supply_collateral":
            st[4] += w[0]
        elif kind == "withdraw_collateral":
            st[4] -= w[1]
        elif kind == "set_fee":
            st[5] = fee = w[0]
        elif kind == "create":
            st[:6] = [0, 0, 0, 0, 0, self.ck["create_fee"].get(mid, 0)]
            if self.protocol in FEE_AT_CREATE:
                fee = st[5]
                if mid not in self.ck["create_fee"]:
                    self._issue("create_fee_unread", lg, mid)
        if kind in TOUCHES_LAST_UPDATE and ts != "":
            st[6] = ts
        if min(st[:5]) < 0:
            self._issue("negative_state", lg, mid)
        self.ck["counts"][kind] = self.ck["counts"].get(kind, 0) + 1
        self.ck["rows"][mid] += 1
        if kind == "create":
            self.created[mid] = (len(self.buf.get(f"{mid}.csv", [])), blk, li,
                                 ts, fee)
        self._emit(f"{mid}.csv", (
            f"{blk},{li},{ts},{kind},{st[0]},{st[1]},{st[2]},{st[3]},"
            f"{st[4]},{rate},{rat},{fee}\n"))

    def _patch_create(self, mid, rate_at_target):
        """Put the IRM's initial rate at target on the market's create row, still in the buffer because both events are in the same block."""
        i, blk, li, ts, fee = self.created.pop(mid)
        self.buf[f"{mid}.csv"][i] = (f"{blk},{li},{ts},create,0,0,0,0,0,,"
                                     f"{rate_at_target},{fee}\n")

    def on_page(self, page):
        page.sort(key=lambda lg: (lg["block_number"], lg["log_index"]))
        for lg in page:
            self.handle(lg)
        for mid, bru in self.pending.items():
            self._issue("bru_without_accrue", {
                "block_number": bru[0], "log_index": bru[1], "tx_hash": ""},
                mid)
        self.pending, self.created = {}, {}
        next_block = page[-1]["block_number"] + 1
        if (self.buffered >= FLUSH_ROWS
                or time.time() - self.last_flush >= FLUSH_SECS):
            self.flush(next_block)
            done = next_block - self.ck["from_block"]
            span = self.to_block - self.ck["from_block"]
            print(f"{self.protocol}:{self.cid} block {next_block:,} "
                  f"({100 * done / span:.1f}%) events "
                  f"{sum(self.ck['counts'].values()):,}", flush=True)
            if self.deadline and datetime.now(timezone.utc) >= self.deadline:
                raise Stop

    def run(self, deadline=None):
        self.deadline = deadline
        if self.ckpt_path.exists():
            self.resume()
            if self.ck["done"]:
                print(f"{self.protocol}:{self.cid} already complete", flush=True)
                return True
            print(f"resuming at block {self.ck['next_block']:,}", flush=True)
        else:
            self.fresh()
        t0 = time.time()
        try:
            hypersync_client.scan_logs_stream(
                self.ccfg["hypersync"],
                addresses=[self.singleton] + self.irms,
                topics=[TOPICS],
                from_block=self.ck["next_block"], to_block=self.to_block,
                on_page=self.on_page)
        except Stop:
            print(f"stopped at deadline; checkpoint at block "
                  f"{self.ck['next_block']:,}", flush=True)
            return False
        self.ck["done"] = True
        self.flush(self.to_block)
        self.write_manifest(time.time() - t0)
        print(f"{self.protocol}:{self.cid} complete: "
              f"{sum(self.ck['rows'].values()):,} rows in "
              f"{len(self.ck['rows'])} markets; issues {self.ck['issues']}",
              flush=True)
        return True

    def write_manifest(self, seconds):
        ck = self.ck
        missing = sorted(set(self.registry) - set(ck["rows"]))
        io_utils.write_json(
            f"panel/{self.protocol}/{self.cid}/manifest.json",
            {"markets": len(ck["rows"]), "registry_markets": len(self.registry),
             "registry_markets_without_rows": missing,
             "rows": sum(ck["rows"].values()), "events_by_kind": ck["counts"],
             "issues": ck["issues"], "issue_examples": ck["examples"],
             "bru_pairing_by_irm": ck["bru_pairing"],
             "bytes": sum(ck["bytes"].values()),
             "per_market": {m: {"rows": ck["rows"][m],
                                "final_state": [str(x) for x in ck["state"][m]]}
                            for m in sorted(ck["rows"])},
             "state_fields": ["total_supply_assets", "total_supply_shares",
                              "total_borrow_assets", "total_borrow_shares",
                              "total_collateral", "fee", "last_update"]},
            generator=GENERATOR,
            meta={"protocol": self.protocol, "chain_id": int(self.cid),
                  "singleton": self.singleton, "irms": self.irms,
                  "from_block": ck["from_block"],
                  "scanned_to_block": ck["to_block"],
                  "scan_head_note": "to_block is exclusive; state is as of "
                                    "the end of block to_block - 1",
                  "source": "hypersync", "build_seconds": round(seconds)})


def read_rows(path, kinds=None):
    with path.open() as fh:
        for r in csv.DictReader(fh):
            if kinds is None or r["kind"] in kinds:
                yield r


def last_csv_block(path) -> int:
    """Block of the last row, read from the file's tail."""
    with path.open("rb") as fh:
        fh.seek(0, os.SEEK_END)
        fh.seek(max(0, fh.tell() - 4096))
        tail = fh.read().decode().strip().splitlines()
    first = tail[-1].split(",")[0]
    return int(first) if first.isdigit() else -1


def check_old(protocol, cid, d):
    """Rows of the first-pass panel against ours, up to its last block."""
    old_files = sorted(OLD_PANEL.glob(f"{cid}-0x*.csv"))
    res = {"markets_compared": 0, "rows_compared": 0, "markets_differing": 0,
           "differences": 0, "examples": [], "old_markets_missing_here": []}
    keys = ("block", "log_index", "kind", "total_supply_assets",
            "total_supply_shares", "total_borrow_assets",
            "total_borrow_shares")
    for f in old_files:
        mid = f.stem.split("-", 1)[1]
        new_path = d / f"{mid}.csv"
        if not new_path.exists():
            res["old_markets_missing_here"].append(mid)
            continue
        last_block = last_csv_block(f)
        old_it = (tuple(r[k] for k in keys) for r in read_rows(f))
        new_it = (tuple(r[k] for k in keys) for r in takewhile(
            lambda r: int(r["block"]) <= last_block,
            read_rows(new_path, OLD_KINDS)))
        diffs = n_old = 0
        for i, (a, b) in enumerate(zip_longest(old_it, new_it)):
            n_old += a is not None
            if a != b:
                diffs += 1
                if len(res["examples"]) < 100:
                    res["examples"].append({"market": mid, "row": i,
                                            "old": a, "new": b})
        res["markets_compared"] += 1
        res["rows_compared"] += n_old
        res["differences"] += diffs
        res["markets_differing"] += diffs > 0
    return res


def market_call(mid):
    return evmabi.encode_call("market(bytes32)", ["bytes32"],
                              [bytes.fromhex(mid[2:])])


def decode_market(ret):
    return [int(x) for x in evmabi.decode(["uint128"] * 6, ret)]


def check_head(protocol, cid, d, ck, target, singleton):
    """Final replayed state against market(id) at the last scanned block."""
    block = ck["to_block"] - 1
    mids = sorted(ck["rows"])
    got = multicall.aggregate3(target, [(singleton, market_call(m))
                                        for m in mids],
                               block=block, chunk=HEAD_CHUNK)
    res = {"block": block, "markets": len(mids), "totals_match": 0,
           "totals_mismatch": [], "fee_mismatch": [],
           "last_update_mismatch": [], "call_failed": []}
    for mid, (ok, ret) in zip(mids, got):
        if not ok:
            res["call_failed"].append(mid)
            continue
        chain = decode_market(ret)
        st = ck["state"][mid]
        if chain[:4] == st[:4]:
            res["totals_match"] += 1
        else:
            res["totals_mismatch"].append({"market": mid, "chain": chain[:4],
                                           "replay": st[:4]})
        if chain[5] != st[5]:
            res["fee_mismatch"].append({"market": mid, "chain": chain[5],
                                        "replay": st[5]})
        if chain[4] != st[6]:
            res["last_update_mismatch"].append(
                {"market": mid, "chain": chain[4], "replay": st[6]})
    res["fee_match"] = res["markets"] - len(res["fee_mismatch"]) - len(
        res["call_failed"])
    res["last_update_match"] = res["markets"] - len(
        res["last_update_mismatch"]) - len(res["call_failed"])
    return res


def check_sample(protocol, cid, d, ck, target, singleton, registry):
    """Archive market(id) at random (market, block) points."""
    rng = random.Random(int(cid) * 7919 + 1)
    mids = sorted(ck["rows"])
    cum = list(accumulate(ck["rows"][m] for m in mids))
    blocks_by = {}
    rows_by = {}
    for _ in range(SAMPLE_PER_CHAIN // 2):
        m = rng.choice(mids)
        b = rng.randrange(registry[m]["created_block"], ck["to_block"])
        blocks_by.setdefault(m, []).append((b, "uniform_block"))
    for _ in range(SAMPLE_PER_CHAIN - SAMPLE_PER_CHAIN // 2):
        k = rng.randrange(cum[-1])
        j = bisect.bisect_right(cum, k)
        rows_by.setdefault(mids[j], set()).add(k - (cum[j - 1] if j else 0))
    for m, idxs in rows_by.items():
        for i, r in enumerate(read_rows(d / f"{m}.csv")):
            if i in idxs:
                blocks_by.setdefault(m, []).append(
                    (int(r["block"]), "uniform_event"))
            if i >= max(idxs):
                break
    points = []
    for m, targets in blocks_by.items():
        targets.sort()
        k, last = 0, None
        for r in read_rows(d / f"{m}.csv"):
            while k < len(targets) and int(r["block"]) > targets[k][0]:
                points.append((m, *targets[k], last))
                k += 1
            if k == len(targets):
                break
            last = r
        points.extend((m, *t, last) for t in targets[k:])
    res = {"points": len(points), "match": 0, "mismatch": [],
           "call_failed": 0, "by_how": {}}
    for m, b, how, last in points:
        replay = ([int(last[k]) for k in ("total_supply_assets",
                                          "total_supply_shares",
                                          "total_borrow_assets",
                                          "total_borrow_shares")]
                  if last else [0, 0, 0, 0])
        try:
            chain = decode_market(rpc.eth_call(target, singleton,
                                               market_call(m), block=b))[:4]
        except Exception as exc:
            res["call_failed"] += 1
            res["mismatch"].append({"market": m, "block": b,
                                    "error": str(exc)[:200]})
            continue
        h = res["by_how"].setdefault(how, {"points": 0, "match": 0})
        h["points"] += 1
        if chain == replay:
            res["match"] += 1
            h["match"] += 1
        else:
            res["mismatch"].append({"market": m, "block": b, "how": how,
                                    "chain": chain, "replay": replay})
    return res


def check_files(d, ck):
    """Each market CSV holds exactly the rows the replay counted, in strictly increasing (block, log_index) order."""
    res = {"files": 0, "rows": 0, "count_mismatch": [], "order_breaks": []}
    for mid, n in sorted(ck["rows"].items()):
        prev, rows = (-1, -1), 0
        with (d / f"{mid}.csv").open() as fh:
            next(fh)
            for line in fh:
                b, li, _ = line.split(",", 2)
                key = (int(b), int(li))
                if key <= prev and len(res["order_breaks"]) < 20:
                    res["order_breaks"].append({"market": mid, "row": rows,
                                                "block": key[0]})
                prev, rows = key, rows + 1
        res["files"] += 1
        res["rows"] += rows
        if rows != n:
            res["count_mismatch"].append({"market": mid, "file": rows,
                                          "replay": n})
    return res


def check_collateral(ck, target, singleton, registry):
    """Token balances of the singleton against the replay."""
    expect = {}
    for mid, st in ck["state"].items():
        m = registry[mid]
        loan, coll = m["loan_token"], m["collateral_token"]
        expect[loan] = expect.get(loan, 0) + st[0] - st[2]
        if coll != "0x" + "0" * 40:
            expect[coll] = expect.get(coll, 0) + st[4]
    tokens = sorted(expect)
    calldata = evmabi.encode_call("balanceOf(address)", ["address"],
                                  [singleton])
    got = multicall.aggregate3(target, [(t, calldata) for t in tokens],
                               block=ck["to_block"] - 1, chunk=HEAD_CHUNK)
    res = {"block": ck["to_block"] - 1, "tokens": len(tokens), "exact": 0,
           "residual_positive": 0, "residual_negative": [],
           "call_failed": [], "largest_positive": []}
    positives = []
    for t, (ok, ret) in zip(tokens, got):
        if not ok or ret in ("0x", ""):
            res["call_failed"].append(t)
            continue
        bal = evmabi.decode(["uint256"], ret)[0]
        r = bal - expect[t]
        if r == 0:
            res["exact"] += 1
        elif r > 0:
            res["residual_positive"] += 1
            positives.append((r / max(bal, 1), t, bal, expect[t]))
        else:
            res["residual_negative"].append(
                {"token": t, "balance": str(bal), "replay": str(expect[t])})
    res["largest_positive"] = [
        {"token": t, "share_of_balance": round(f, 6), "balance": str(b),
         "replay": str(e)} for f, t, b, e in sorted(positives)[-10:]]
    return res


def last_rate_at_target(path):
    """(block, rate_at_target) of the last row carrying one."""
    with path.open("rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        span = 1 << 16
        while True:
            fh.seek(max(0, size - span))
            lines = fh.read().decode().splitlines()
            if size > span:
                lines = lines[1:]
            for line in reversed(lines):
                f = line.split(",")
                if len(f) == 12 and f[10] and f[0].isdigit():
                    return int(f[0]), int(f[10])
            if span >= size:
                return None
            span *= 4


def code_body_hash(code_hex, immutable_word):
    """sha256 of runtime code with one immutable word masked and the CBOR metadata tail stripped, so builds of the same source."""
    import hashlib
    c = code_hex.removeprefix("0x").lower()
    if immutable_word:
        c = c.replace(immutable_word.removeprefix("0x").lower(), "00" * 32)
    b = bytes.fromhex(c)
    n = int.from_bytes(b[-2:], "big") + 2 if len(b) >= 2 else 0
    body = b[:-n] if 2 < n < len(b) else b
    return {"code_bytes": len(b), "body_bytes": len(body),
            "body_sha256": hashlib.sha256(body).hexdigest()}


def check_irm(ck, d, target, singleton, registry):
    """The interest rate side."""
    block = ck["to_block"] - 1
    zero = "0x" + "0" * 40
    res = {"block": block, "irms": {}, "markets": 0, "rate_at_target_match": 0,
           "rate_at_target_mismatch": [], "no_rate_in_panel": [],
           "call_failed": [], "irm_param_rows": 0}
    try:
        ds = rpc.eth_call(target, singleton,
                          evmabi.selector("DOMAIN_SEPARATOR()"), block=block)
    except RuntimeError:
        ds = None
    res["singleton_code"] = code_body_hash(
        rpc.get_code(target, singleton, block=block), ds)
    for irm in ck["irms"]:
        mo, mo_addr = None, None
        for getter in ("MORPHO()", "MOOLAH()"):
            try:
                mo = rpc.eth_call(target, irm, evmabi.selector(getter),
                                  block=block)
                mo_addr = evmabi.addr(mo)
                break
            except Exception:
                continue
        res["irms"][irm] = {
            "MORPHO": mo_addr, "MORPHO_is_singleton": mo_addr == singleton,
            "markets": sum(registry[m]["irm"] == irm for m in ck["rows"]),
            **code_body_hash(rpc.get_code(target, irm, block=block), mo)}
    mids = sorted(m for m in ck["rows"] if registry[m]["irm"] != zero)
    calls = [(registry[m]["irm"],
              evmabi.encode_call("rateAtTarget(bytes32)", ["bytes32"],
                                 [bytes.fromhex(m[2:])])) for m in mids]
    got = multicall.aggregate3(target, calls, block=block, chunk=HEAD_CHUNK)
    for m, (ok, ret) in zip(mids, got):
        res["markets"] += 1
        if not ok or ret in ("0x", ""):
            res["call_failed"].append(m)
            continue
        chain = evmabi.decode(["int256"], ret)[0]
        last = last_rate_at_target(d / f"{m}.csv")
        if last is None:
            res["no_rate_in_panel"].append({"market": m, "chain": chain})
        elif last[1] == chain:
            res["rate_at_target_match"] += 1
        else:
            res["rate_at_target_mismatch"].append(
                {"market": m, "chain": chain, "panel": last[1],
                 "panel_block": last[0]})
    p = d / "irm_params.csv"
    if p.exists():
        with p.open() as fh:
            res["irm_param_rows"] = sum(1 for _ in fh) - 1
    return res


def validate(protocol, cid, checks, force=False):
    ccfg, singleton, markets, _irms, _ = setup(protocol, cid)
    d = out_dir(protocol, cid)
    ck = json.loads((d / "_checkpoint.json").read_text())
    if not ck["done"]:
        raise RuntimeError(f"{protocol}:{cid} build is not complete")
    target = logs.rpc_target(ccfg)
    vpath = d / "validation.json"
    out = json.loads(vpath.read_text()) if vpath.exists() else {}
    if out.get("to_block") != ck["to_block"]:
        out = {"to_block": ck["to_block"]}
    registry = {m["market_id"]: m for m in markets}
    for name in checks:
        if name in out and not force:
            print(f"{name}: done already", flush=True)
            continue
        t0, calls0 = time.time(), rpc.calls_made()
        if name == "old":
            if not any(OLD_PANEL.glob(f"{cid}-0x*.csv")) or protocol != "morpho":
                continue
            out[name] = check_old(protocol, cid, d)
        elif name == "head":
            out[name] = check_head(protocol, cid, d, ck, target, singleton)
        elif name == "sample":
            out[name] = check_sample(protocol, cid, d, ck, target, singleton,
                                     registry)
        elif name == "files":
            out[name] = check_files(d, ck)
        elif name == "collateral":
            out[name] = check_collateral(ck, target, singleton, registry)
        elif name == "irm":
            out[name] = check_irm(ck, d, target, singleton, registry)
        out[name]["seconds"] = round(time.time() - t0)
        out[name]["rpc_calls"] = rpc.calls_made() - calls0
        summary = {k: v for k, v in out[name].items()
                   if not isinstance(v, (list, dict))}
        print(f"{name}: {summary}", flush=True)
        io_utils.write_json(f"panel/{protocol}/{cid}/validation.json", out,
                            generator=GENERATOR,
                            meta={"protocol": protocol, "chain_id": int(cid),
                                  "scanned_to_block": ck["to_block"]})
    print(f"rpc calls this run: {rpc.calls_made()}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["estimate", "build", "validate"])
    ap.add_argument("--protocol", required=True, choices=list(SINGLETON_KEY))
    ap.add_argument("--chain", required=True)
    ap.add_argument("--deadline", help="UTC HH:MM: stop at the first "
                    "checkpoint after this time")
    ap.add_argument("--checks",
                    default="files,old,head,sample,collateral,irm")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    if args.cmd == "estimate":
        estimate(args.protocol, args.chain)
    elif args.cmd == "build":
        deadline = None
        if args.deadline:
            h, m = map(int, args.deadline.split(":"))
            deadline = datetime.now(timezone.utc).replace(
                hour=h, minute=m, second=0, microsecond=0)
        Builder(args.protocol, args.chain).run(deadline)
    else:
        validate(args.protocol, args.chain, args.checks.split(","),
                 args.force)


if __name__ == "__main__":
    main()
