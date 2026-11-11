# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""Rebuild Euler curator holdings and borrower collateral from verified events."""
import argparse
import bisect
import csv
import fcntl
import hashlib
import io
import json
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
CODE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
sys.path.insert(0, str(ROOT / "code"))
from common import env, evmabi, hypersync_client, logs, multicall, rpc

ZERO = "0x" + "0" * 40
GEN = "code/data/vaults_euler.py"
getcontext().prec = 70


STATE_GAPS = ("metadata is not found", "header not found", "missing trie node")


def retry_state_gap(fn, *args, **kwargs):
    """Call fn, retrying up to five times while the provider reports a state gap."""
    for attempt in range(6):
        try:
            return fn(*args, **kwargs)
        except RuntimeError as exc:
            if attempt == 5 or not any(s in str(exc) for s in STATE_GAPS):
                raise
            time.sleep(1 + attempt)


def atomic(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(value if isinstance(value, str) else json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def read_jsonl(path):
    if path.exists():
        with path.open() as fh:
            for line in fh:
                yield json.loads(line)


def abi_type(field):
    if field["type"].startswith("tuple"):
        return "(" + ",".join(abi_type(x) for x in field["components"]) + ")" + field["type"][5:]
    return field["type"]


def event_map(abi):
    return {evmabi.topic0(e["name"] + "(" + ",".join(abi_type(i) for i in e["inputs"]) + ")"): e
            for e in abi if e.get("type", "event") == "event"}


def clean(value):
    if isinstance(value, bytes):
        return "0x" + value.hex()
    if isinstance(value, (tuple, list)):
        return [clean(x) for x in value]
    return value.lower() if isinstance(value, str) and value.startswith("0x") else value


def decode(lg, event):
    indexed = iter(lg["topics"][1:])
    data = iter(evmabi.decode([abi_type(i) for i in event["inputs"] if not i["indexed"]], lg["data"]))
    fields = {}
    for field in event["inputs"]:
        value = evmabi.decode([abi_type(field)], next(indexed))[0] if field["indexed"] else next(data)
        fields[field["name"]] = clean(value)
    return {"vault": lg["address"].lower(), "event": event["name"], "block": lg["block_number"],
            "log_index": lg["log_index"], "timestamp": lg["timestamp"], "tx_hash": lg["tx_hash"], **fields}


def csv_text(columns, rows):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def read_csv(path):
    with path.open() as fh:
        yield from csv.DictReader(fh)


class Series:
    """Ordered states at individual log positions, including same-block changes."""
    def __init__(self, rows):
        self.rows = rows
        self.keys = [(int(r["block"]), int(r["log_index"])) for r in rows]

    def at(self, key):
        i = bisect.bisect_right(self.keys, key) - 1
        return self.rows[i] if i >= 0 else None

    def following(self, key):
        i = bisect.bisect_left(self.keys, key)
        return self.rows[i] if i < len(self.rows) else None


def resilient_scan(fn):
    """Back off on throttling, resuming from the last durable page."""
    delay = 10
    for attempt in range(8):
        try:
            return fn()
        except Exception as exc:
            if not any(s in str(exc).lower() for s in ["429", "rate limit", "too many", "timeout", "timed out"]):
                raise
            if attempt == 7:
                raise RuntimeError("HyperSync unavailable after exponential backoff") from None
            print(f"HyperSync throttled or timed out; sleeping {delay}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 300)


def stream(job, name, addresses, topics, decoder, keep=lambda r: True, start=None, chain_wide=False):
    """Serialize writers to each stream when stages run in separate processes."""
    key = hashlib.sha256(str(job.out).encode()).hexdigest()[:16]
    with Path(f"/tmp/vaults-euler-{key}-{name}.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        return stream_locked(job, name, addresses, topics, decoder, keep, start, chain_wide)


def stream_locked(job, name, addresses, topics, decoder, keep, start, chain_wide):
    """Keep a complete-block prefix and its byte length after every page."""
    path = job.out / "events" / f"{name}.jsonl"
    ckpath = job.out / "checkpoints" / f"{name}.json"
    start = job.start if start is None else start
    identity = hashlib.sha256(json.dumps([sorted(addresses), topics, start, job.head]).encode()).hexdigest()
    if ckpath.exists():
        ck = json.loads(ckpath.read_text())
        if ck["query_sha256"] != identity:
            raise RuntimeError(f"Changed query for {name}; existing data preserved")
    else:
        if path.exists():
            raise RuntimeError(f"Uncheckpointed data at {path}; refusing to overwrite")
        atomic(path, "")
        ck = {"next_block": start, "to_block": job.head, "complete": False,
              "query_sha256": identity, "file_bytes": {str(path.relative_to(job.out)): 0}, "rows": 0}
        atomic(ckpath, ck)
    if ck["complete"]:
        recorded = ck["file_bytes"][str(path.relative_to(job.out))]
        if path.stat().st_size < recorded:
            raise RuntimeError(f"Checkpointed file is shorter than recorded: {path}")
        if path.stat().st_size > recorded:
            with path.open("r+b") as fh:
                fh.truncate(recorded)
        return

    def run():
        state = json.loads(ckpath.read_text())
        rel = str(path.relative_to(job.out))
        if path.stat().st_size < state["file_bytes"][rel]:
            raise RuntimeError(f"Checkpointed file is shorter than recorded: {path}")
        with path.open("r+b") as fh:
            fh.truncate(state["file_bytes"][rel])
        pending = []

        def append(rows, boundary, complete=False):
            with path.open("ab") as fh:
                fh.write("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows).encode())
                fh.flush()
                os.fsync(fh.fileno())
                state["file_bytes"][rel] = fh.tell()
            state.update(next_block=boundary, complete=complete, rows=state["rows"] + len(rows))
            atomic(ckpath, state)
            print(f"{job.cid} {name}: block < {boundary:,}, {state['rows']:,} rows, {state['file_bytes'][rel]:,} bytes", flush=True)

        def on_page(page):
            nonlocal pending
            hi = max(x["block_number"] for x in page)
            decoded = []
            for lg in page:
                if chain_wide and lg["address"].lower() not in addresses:
                    continue
                row = decoder(lg)
                if row is not None and keep(row):
                    if row["timestamp"] is None:
                        raise RuntimeError("HyperSync log missing timestamp")
                    decoded.append(row)
            merged = {(r["block"], r["log_index"]): r for r in pending + decoded}
            ordered = [merged[k] for k in sorted(merged)]
            append([r for r in ordered if r["block"] < hi], hi)
            pending = [r for r in ordered if r["block"] >= hi]

        query_class = hypersync_client.Query
        def query_with_page_size(**kwargs):
            return query_class(**({"max_num_logs": 50_000, "max_num_blocks": 50_000} | kwargs))
        hypersync_client.Query = query_with_page_size
        try:
            hypersync_client.scan_logs_stream(job.cfg["hypersync"], addresses=None if chain_wide else sorted(addresses),
                                             topics=topics, from_block=state["next_block"],
                                             to_block=job.head, on_page=on_page)
        finally:
            hypersync_client.Query = query_class
        append(pending, job.head, True)

    resilient_scan(run)


def fetch_source(cid, address):
    """Cache explorer source verbatim with flattened filenames and provenance."""
    address = address.lower()
    folder = ROOT / "data/contracts" / f"{cid}-{address}"
    cache = folder / "_explorer.json"
    if cache.exists():
        body = json.loads(cache.read_text())
        if isinstance(body, list):
            body = body[0]
        if "result" in body:
            body = body["result"][0]
        if "ABI" in body:
            return folder, body
    delay = 2
    for attempt in range(8):
        try:
            response = requests.get("https://api.etherscan.io/v2/api", timeout=60, params={
                "chainid": cid, "module": "contract", "action": "getsourcecode",
                "address": address, "apikey": env.require("ETHERSCAN_API_KEY")})
            result = response.json()
            if result.get("status") == "1":
                body = result["result"][0]
                break
        except (requests.RequestException, ValueError):
            pass
        if attempt == 7:
            raise RuntimeError(f"Etherscan source unavailable: chain {cid}, {address}")
        time.sleep(delay)
        delay *= 2
    source = body.get("SourceCode", "")
    if not source:
        atomic(cache, body)
        atomic(folder / "_meta.json", {"address": address, "chain_id": int(cid),
               "contract_name": body.get("ContractName"), "n_files": 0,
               "source": "Etherscan v2 getsourcecode", "verified": False})
        return folder, body
    try:
        parsed = json.loads(source[1:-1] if source.startswith("{{") else source)
        sources = parsed.get("sources", parsed)
    except ValueError:
        sources = {body["ContractName"] + ".sol": {"content": source}}
    for name, content in sources.items():
        text = content.get("content", "") if isinstance(content, dict) else content
        atomic(folder / name.replace("/", "__").replace("\\", "__"), text)
    atomic(cache, body)
    atomic(folder / "_meta.json", {"address": address, "chain_id": int(cid),
           "contract_name": body["ContractName"], "compiler": body["CompilerVersion"],
           "n_files": len(sources), "source": "Etherscan v2 getsourcecode"})
    time.sleep(0.25)
    return folder, body


class Job:
    def __init__(self, cid):
        self.cid = str(cid)
        self.out = ROOT / "data/vaults/euler_v2" / self.cid
        self.out.mkdir(parents=True, exist_ok=True)
        self.panel = ROOT / "data/panel/euler_v2" / self.cid
        self.cfg = logs.load_chains()[self.cid]
        self.target = logs.rpc_target(self.cfg)
        self.deployment = json.loads((ROOT / "code/config/deployments.json").read_text())["protocols"]["euler_v2"]["chains"][self.cid]
        self.earn = {r["vault"]: r for r in read_jsonl(ROOT / f"data/registry/vaults/euler_v2/{cid}.jsonl")}
        self.markets = {r["vault"]: r for r in read_jsonl(ROOT / f"data/registry/markets/euler_v2/{cid}.jsonl") if r["type"] == "vault"}
        self.lending = {r["vault"]: r for r in read_jsonl(self.panel / "vaults.jsonl") if r["ever_borrowed"]}
        self.head = json.loads((self.panel / "checkpoint.json").read_text())["to_block"]
        self.start = min(r["created_block"] for r in self.markets.values())

    def sources(self):
        manifest = {}
        addresses = set(self.earn) | {r["factory"] for r in self.earn.values() if r.get("factory")}
        addresses.add(self.deployment["evc"].lower())
        panel_meta = json.loads((self.panel / "evault_abi.json.meta.json").read_text())
        addresses.update(panel_meta["implementations"])
        for n, address in enumerate(sorted(addresses)):
            folder, body = fetch_source(self.cid, address)
            if body["ABI"] == "Contract source code not verified":
                manifest[address] = {"source_dir": str(folder.relative_to(ROOT)), "version": "unresolved",
                                     "explorer": body, "abi": [], "role_names": {}}
                print(f"sources {self.cid}: {address} unverified; resolving after factory inspection", flush=True)
                continue
            abi = json.loads(body["ABI"])
            functions = {e.get("name") for e in abi if e["type"] == "function"}
            version = "2025_caps_queues" if {"curator", "supplyQueue", "withdrawQueue"} <= functions else "other"
            if body["ContractName"] == "AmpleEarn":
                version = "2025_caps_queues_ample_extension"
            source = "\n".join(p.read_text() for p in folder.glob("*.sol"))
            if version == "other" and "allocationPoints" in source:
                version = "2024_strategies_allocation_points"
            role_names = {}
            if re.search(r"DEFAULT_ADMIN_ROLE\s*=\s*0x00\s*;", source):
                role_names["0x" + "00" * 32] = "DEFAULT_ADMIN_ROLE"
            for name, literal in re.findall(r'bytes32\s+(?:public\s+)?constant\s+(\w+)\s*=\s*keccak256\("([^"]+)"\)', source):
                role_names[evmabi.topic0(literal)] = name
            manifest[address] = {"source_dir": str(folder.relative_to(ROOT)), "contract_name": body["ContractName"],
                                 "version": version, "abi": abi, "role_names": role_names,
                                 "source_sha256": hashlib.sha256(source.encode()).hexdigest()}
            print(f"sources {self.cid}: {n + 1}/{len(addresses)} {address} {body['ContractName']} {version}", flush=True)
        atomic(self.out / "sources.json", manifest)

    def load_sources(self):
        self.src = json.loads((self.out / "sources.json").read_text())
        changed = False
        cross_refs = {}
        ethereum_sources = ROOT / "data/vaults/euler_v2/1/sources.json"
        if ethereum_sources.exists():
            cross_refs = {a: r for a, r in json.loads(ethereum_sources.read_text()).items()
                          if r.get("contract_name") == "AmpleEarn" and not r.get("explorer")}
        for address, record in self.src.items():
            if address not in self.earn or record["version"] == "2025_caps_queues_ample_extension_inferred" or not record.get("explorer") or not (self.out / "events/earn.jsonl").exists():
                continue
            for donor, verified in cross_refs.items():
                payout_topics = {t for t, e in event_map(verified["abi"]).items() if e["name"] == "SetIsPayoutManager"}
                has_payout = any(r["vault"] == address and (r["event"] == "SetIsPayoutManager" or
                                 (r["event"] == "Unknown" and r["topics"][0] in payout_topics))
                                 for r in read_jsonl(self.out / "events/earn.jsonl"))
                if has_payout and record["version"] != "2025_caps_queues_ample_extension_inferred":
                    record.update(version="2025_caps_queues_ample_extension_inferred", abi=verified["abi"],
                                  abi_reference=donor, abi_reference_chain=1,
                                  provenance="Shared payout-manager and fee event signatures from verified Ethereum AmpleEarn source; getters validated on this chain",
                                  uncertainty="Vault source and full runtime match unavailable. AmpleEarn extension inferred from emitted signatures and checked getters.")
                    changed = True
                    break
        for record in self.src.values():
            if record.get("role_names") and not any(e.get("type") == "function" and e.get("name") == "hasRole" for e in record["abi"]):
                record["role_names"] = {}
                changed = True
            if record.get("contract_name") == "AmpleEarn" and record["version"] != "2025_caps_queues_ample_extension":
                record["version"] = "2025_caps_queues_ample_extension"
                changed = True
            if record.get("contract_name") == "EulerEarnFactory" and record["version"] == "other":
                source_text = "\n".join(p.read_text() for p in (ROOT / record["source_dir"]).glob("*.sol"))
                if "new EulerEarn{salt: salt}" in source_text and "supplyQueue" in source_text:
                    record["version"] = "2025_caps_queues_factory"
                    changed = True
        if changed:
            atomic(self.out / "sources.json", self.src)
        family = {}
        self.family_sources = {}
        for address, record in self.src.items():
            if record["version"].startswith("2025") and not record.get("explorer"):
                for topic, event in event_map(record["abi"]).items():
                    family[topic] = event
                    self.family_sources[topic] = address
        for address, record in cross_refs.items():
            for topic, event in event_map(record["abi"]).items():
                if topic not in family:
                    family[topic] = event
                    self.family_sources[topic] = "1:" + address
        self.edec = {v: family | event_map(self.src[v]["abi"]) for v in self.earn}
        atomic(self.out / "event_abi.json", {"verified_shared_events": {topic: {"event": event, "source_contract": self.family_sources[topic]}
                                                                       for topic, event in family.items()},
               "note": "Per-vault verified ABI takes precedence. Shared family ABI decodes delegatecalled-library events omitted from a vault ABI. Unverified vaults are flagged separately in sources.json."})
        self.vdec = event_map(json.loads((self.panel / "evault_abi.json").read_text())["events"])

    def earn_rows(self):
        for row in read_jsonl(self.out / "events/earn.jsonl"):
            if row["event"] == "Unknown" and row["topics"][0] in self.edec[row["vault"]]:
                topic = row["topics"][0]
                raw = {"address": row["vault"], "block_number": row["block"], "log_index": row["log_index"],
                       "timestamp": row["timestamp"], "tx_hash": row["tx_hash"], "topics": row["topics"], "data": row["data"]}
                row = decode(raw, self.edec[row["vault"]][topic]) | {"abi_reference": self.family_sources[topic]}
            yield row

    def resolve(self):
        """Resolve missing explorer verification without claiming it exists."""
        self.load_sources()
        references = [v for v in self.earn if self.src[v]["abi"]]
        reference = references[0]
        for v in self.earn:
            if self.src[v]["abi"]:
                continue
            code = rpc.get_code(self.target, v, self.head - 1)
            atomic(self.out / "runtime" / f"{v}.json", {"block": self.head - 1, "code": code})
            match = re.search(r"363d3d373d3d3d363d73([0-9a-f]{40})5af43d82803e903d91602b57fd5bf3", code)
            if match:
                implementation = "0x" + match.group(1)
                folder, body = fetch_source(self.cid, implementation)
                if body.get("SourceCode"):
                    self.src[v].update(abi=json.loads(body["ABI"]), version="2025_caps_queues",
                                       verified_implementation=implementation, source_dir=str(folder.relative_to(ROOT)),
                                       provenance="EIP-1167 runtime implementation, verified source")
                    continue
            factory = self.earn[v].get("factory")
            if factory:
                source = "\n".join(p.read_text() for p in (ROOT / self.src[factory]["source_dir"]).glob("*.sol"))
                if "new EulerEarn{salt: salt}" in source:
                    candidates = [a for a in references if self.earn[a].get("factory") == factory]
                    donor = candidates[0]
                    self.src[v].update(abi=self.src[donor]["abi"], version=self.src[donor]["version"],
                                       abi_reference=donor, verified_factory=factory,
                                       provenance="Registry CreateEulerEarn and verified factory new EulerEarn source; vault itself unverified")
                    continue
            def normalized(raw):
                data = bytearray.fromhex(raw.removeprefix("0x"))
                metadata = int.from_bytes(data[-2:], "big")
                if metadata < len(data):
                    data = data[:-metadata-2]
                i = 0
                while i < len(data):
                    op = data[i]
                    width = op - 0x5f if 0x60 <= op <= 0x7f else 0
                    if width == 32:
                        data[i+1:i+33] = b"\x00" * 32
                    i += width + 1
                return bytes(data)
            matched = None
            for donor in references:
                p = self.out / "runtime" / f"{donor}.json"
                if not p.exists():
                    atomic(p, {"block": self.head - 1, "code": rpc.get_code(self.target, donor, self.head - 1)})
                if normalized(code) == normalized(json.loads(p.read_text())["code"]):
                    matched = donor
                    break
            if matched:
                self.src[v].update(abi=self.src[matched]["abi"], version=self.src[matched]["version"],
                                   abi_reference=matched, provenance="Inferred runtime opcode match after stripping CBOR and masking PUSH32 operands; not source verification",
                                   uncertainty="Immutable and constant PUSH32 operands not distinguished; ABI inference validated with events and getters")
            else:
                self.src[v].update(abi=self.src[reference]["abi"], version="2025_caps_queues_inferred",
                                   abi_reference=reference,
                                   provenance="Verified reference ABI for shared event signatures only; vault source unavailable",
                                   uncertainty="Version inferred from registry SetCap and shared ABI, not verified source or runtime match. Unknown events retained raw; head getters checked separately.")
                print(f"UNVERIFIED source {v}, runtime bytes {(len(code)-2)//2}; shared-signature decoding only", flush=True)
        atomic(self.out / "sources.json", self.src)

    def estimate(self):
        self.load_sources()
        path = self.out / "estimate.json"
        if path.exists():
            print(json.loads(path.read_text()), flush=True)
            return
        names = {"Transfer", "Deposit", "Withdraw", "Borrow", "Repay", "InterestAccrued", "PullDebt", "Liquidate", "DebtSocialized"}
        topics = [t for t, e in self.vdec.items() if e["name"] in names]
        addresses = sorted(set(self.markets) | set(self.earn))
        width = max(1, (self.head - self.start) // 600)
        counts, size = Counter(), 0
        for i in range(6):
            lo = self.start + (self.head - self.start) * i // 6
            page, _ = resilient_scan(lambda: hypersync_client.scan_logs(self.cfg["hypersync"], addresses=addresses,
                                         topics=[topics], from_block=lo, to_block=lo + width))
            for lg in page:
                counts[self.vdec[lg["topics"][0]]["name"]] += 1
                size += len(json.dumps(lg)) + 1
        scale = (self.head - self.start) / (6 * width)
        estimate = {"sample_fraction": 1 / scale, "events": {k: round(v * scale) for k, v in counts.items()},
                    "raw_bytes_before_account_filter": round(size * scale),
                    "derived_bytes_rough_upper_bound": round(size * scale * 2),
                    "note": "Six evenly spaced 1/600-span windows; sparse governance, EVC and external strategies additional."}
        atomic(path, estimate)
        print(f"{self.cid} estimate: {json.dumps(estimate)}", flush=True)

    def scan(self):
        self.load_sources()
        unresolved = [v for v in self.earn if not self.edec[v]]
        if unresolved:
            raise RuntimeError(f"Resolve verified ABI provenance first: {unresolved}")
        self.estimate()
        def earn_decode(lg):
            event = self.edec[lg["address"].lower()].get(lg["topics"][0])
            if event:
                return decode(lg, event)
            return {"vault": lg["address"].lower(), "event": "Unknown", "block": lg["block_number"],
                    "log_index": lg["log_index"], "timestamp": lg["timestamp"], "tx_hash": lg["tx_hash"],
                    "topics": lg["topics"], "data": lg["data"]}
        stream(self, "earn", set(self.earn), None, earn_decode,
               start=min(r["created_block"] for r in self.earn.values()))
        debt_names = {"Borrow", "Repay", "InterestAccrued", "PullDebt", "Liquidate", "DebtSocialized"}
        debt_dec = {t: e for t, e in self.vdec.items() if e["name"] in debt_names}
        stream(self, "debt", set(self.lending), [sorted(debt_dec)], lambda lg: decode(lg, debt_dec[lg["topics"][0]]), chain_wide=True)
        self.accounts = defaultdict(set)
        for row in read_jsonl(self.out / "events/debt.jsonl"):
            if row["event"] == "Borrow" and row["assets"] > 0:
                self.accounts[row["vault"]].add(row["account"])
        borrowers = set().union(*self.accounts.values())
        atomic(self.out / "accounts.json", {v: sorted(a) for v, a in self.accounts.items()})
        strategies = set()
        for row in self.earn_rows():
            if "id" in row:
                strategies.add(row["id"])
            for key in ("newSupplyQueue", "newWithdrawQueue"):
                strategies.update(row.get(key, []))
        self.collaterals = defaultdict(set)
        for row in read_jsonl(self.panel / "governance.jsonl"):
            if row["event"] == "GovSetLTV" and row["vault"] in self.lending:
                self.collaterals[row["vault"]].add(row["collateral"])
        atomic(self.out / "collateral_sets.json", {v: sorted(a) for v, a in self.collaterals.items()})
        tokens = set(self.markets) | strategies | set().union(*self.collaterals.values())
        token_dec = {t: e for t, e in self.vdec.items() if e["name"] in {"Transfer", "Deposit", "Withdraw"}}
        interested = borrowers | set(self.earn)
        stream(self, "tokens", tokens, [sorted(token_dec)], lambda lg: decode(lg, token_dec[lg["topics"][0]]),
               keep=lambda r: any(r.get(k) in interested for k in ("from", "to", "owner", "receiver")), start=0)
        self.evc()

    def evc(self):
        """Scan collateral enablement independently once borrower identities exist."""
        self.load_sources()
        borrowers = set().union(*(set(a) for a in json.loads((self.out / "accounts.json").read_text()).values()))
        evc = self.deployment["evc"].lower()
        evc_dec = {t: e for t, e in event_map(self.src[evc]["abi"]).items() if e["name"] == "CollateralStatus"}
        stream(self, "evc", {evc}, [sorted(evc_dec)], lambda lg: decode(lg, evc_dec[lg["topics"][0]]),
               keep=lambda r: r["account"] in borrowers, start=0)

    def build(self):
        """Materialize final tables from immutable, checkpointed decoded logs."""
        self.load_sources()
        for stage in ("earn", "debt", "tokens", "evc"):
            if not json.loads((self.out / "checkpoints" / f"{stage}.json").read_text())["complete"]:
                raise RuntimeError(f"Incomplete {stage} scan")
        self.accounts = {v: set(a) for v, a in json.loads((self.out / "accounts.json").read_text()).items()}
        self.collaterals = {v: set(a) for v, a in json.loads((self.out / "collateral_sets.json").read_text()).items()}
        base = ["block", "log_index", "timestamp", "tx_hash"]
        positions, flows, summaries = defaultdict(list), defaultdict(list), {}
        roles, actions, fees = [], [], []
        state = {v: defaultdict(set) for v in self.earn}
        counts = {v: Counter() for v in self.earn}
        extent, strategies = defaultdict(list), defaultdict(set)
        role_events = {"OwnershipTransferred": ("owner", "newOwner"), "SetCurator": ("curator", "newCurator"),
                       "SetGuardian": ("guardian", "guardian")}
        for r in self.earn_rows():
            v, name = r["vault"], r["event"]
            counts[v][name] += 1
            extent[v].append(r["block"])
            if "id" in r:
                strategies[v].add(r["id"])
            for key in ("newSupplyQueue", "newWithdrawQueue"):
                strategies[v].update(r.get(key, []))
            if name in ("Deposit", "Withdraw"):
                flows[v].append({**r, "kind": name})
            if name == "AccrueInterest":
                fees.append({**r, "decoded_fields": json.dumps({k: val for k, val in r.items() if k not in base + ["vault", "event"]})})
            if name not in {"Transfer", "Approval", "Deposit", "Withdraw", "AccrueInterest"}:
                actions.append(r)
            changes = []
            if name in role_events:
                role, key = role_events[name]
                changes += [(role, a, "removed") for a in sorted(state[v][role])]
                if r[key] != ZERO:
                    changes.append((role, r[key], "added"))
            elif name == "SetIsAllocator":
                changes.append(("allocator", r["allocator"], "added" if r["isAllocator"] else "removed"))
            elif name == "SetIsPayoutManager":
                changes.append(("payout_manager", r["payoutManager"], "added" if r["isPayoutManager"] else "removed"))
            elif name in {"RoleGranted", "RoleRevoked"}:
                role = self.src[v]["role_names"].get(r["role"], r["role"])
                changes.append((role, r["account"], "added" if name == "RoleGranted" else "removed"))
            for role, address, action in changes:
                if action == "added":
                    state[v][role].add(address)
                else:
                    state[v][role].discard(address)
                roles.append({k: r[k] for k in ["vault"] + base} | {"role": role, "address": address, "action": action, "event": name})
        balances = Counter()
        collateral_rows = defaultdict(list)
        subscribers = defaultdict(set)
        for v, accounts in self.accounts.items():
            for account in accounts:
                subscribers[account].add(v)
        cbalance = Counter()
        earn_set, subscriber_set = set(self.earn), set(subscribers)
        for r in read_jsonl(self.out / "events/tokens.jsonl"):
            token, name = r["vault"], r["event"]
            touched = {r[k] for k in ("from", "to", "owner", "receiver") if k in r} & earn_set
            for v in touched:
                delta = 0
                if name == "Transfer":
                    delta = r["value"] * ((r["to"] == v) - (r["from"] == v))
                balances[v, token] += delta
                strategies[v].add(token)
                extent[v].append(r["block"])
                positions[v].append({**r, "strategy": token, "kind": name, "shares_delta": delta,
                                     "event_shares": r.get("shares", r.get("value")), "cum_shares": balances[v, token]})
            if name == "Transfer":
                for account in {r["from"], r["to"]} & subscriber_set:
                    delta = r["value"] * ((r["to"] == account) - (r["from"] == account))
                    cbalance[account, token] += delta
                    for v in subscribers[account]:
                        if token in self.collaterals.get(v, set()):
                            collateral_rows[v].append({**r, "account": account, "collateral_vault": token,
                                                       "shares_delta": delta, "cum_shares": cbalance[account, token]})
        for v, vr in self.earn.items():
            atomic(self.out / "positions" / f"{v}.csv", csv_text(base + ["strategy", "kind", "assets", "event_shares", "shares_delta", "cum_shares"], positions[v]))
            atomic(self.out / "flows" / f"{v}.csv", csv_text(base + ["kind", "sender", "owner", "receiver", "assets", "shares"], flows[v]))
            summaries[v] = {"vault": v, "version": self.src[v]["version"], "source_dir": self.src[v]["source_dir"],
                            "events": dict(counts[v]), "position_rows": len(positions[v]), "flow_rows": len(flows[v]),
                            "strategies_touched": sorted(strategies[v]), "first_block": min(extent[v]) if extent[v] else None,
                            "last_block": max(extent[v]) if extent[v] else None,
                            "roles_at_end": {role: sorted(a) for role, a in state[v].items()}}
        atomic(self.out / "vaults.jsonl", "".join(json.dumps(r, sort_keys=True) + "\n" for r in summaries.values()))
        atomic(self.out / "vaults.jsonl.meta.json", {"generator": GEN, "chain_id": int(self.cid), "to_block_exclusive": self.head,
               "earn_vaults": len(self.earn), "positions": "Transfer alone changes cum_shares. Deposit and Withdraw carry assets and event_shares, with shares_delta=0 to avoid counting their paired Transfer twice.",
               "roles": "Explicit role holders; source modifiers also permit owner as curator/allocator/guardian and curator as allocator.",
               "source_manifest": "sources.json"})
        for name, rows in [("roles", roles), ("actions", actions)]:
            atomic(self.out / f"{name}.jsonl", "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows))
        atomic(self.out / "fees.csv", csv_text(["vault"] + base + ["event", "newTotalAssets", "feeShares", "protocolFeeShares", "decoded_fields"], fees))
        debt_rows, cumulative = defaultdict(list), Counter()
        for r in read_jsonl(self.out / "events/debt.jsonl"):
            v, name = r["vault"], r["event"]
            if name in {"Borrow", "Repay", "InterestAccrued"}:
                account = r["account"]
                delta = -r["assets"] if name == "Repay" else r["assets"]
                cumulative[v, account] += delta
                debt_rows[v].append({**r, "kind": name, "debt_delta_assets": delta, "cum_event_assets": cumulative[v, account], "accounting_event": 1})
            else:
                affected = [r["account"]] if name == "DebtSocialized" else ([r["from"], r["to"]] if name == "PullDebt" else [r["violator"], r["liquidator"]])
                for account in dict.fromkeys(affected):
                    debt_rows[v].append({**r, "account": account, "kind": name, "assets": r.get("assets", r.get("repayAssets")),
                                         "debt_delta_assets": 0, "cum_event_assets": cumulative[v, account], "accounting_event": 0})
        for v in self.lending:
            atomic(self.out / "borrows" / f"{v}.csv", csv_text(base + ["account", "kind", "assets", "debt_delta_assets", "cum_debt_assets", "cum_event_assets", "accounting_event"], debt_rows[v]))
            atomic(self.out / "collateral" / f"{v}.csv", csv_text(base + ["account", "collateral_vault", "shares_delta", "cum_shares"], collateral_rows[v]))
        atomic(self.out / "borrows.meta.json", {"generator": GEN, "exact_continuous_debt": False,
               "cum_debt_assets": "Intentionally blank: continuous interest cannot be reconstructed exactly from integer event assets alone.",
               "cum_event_assets": "Sum Borrow + InterestAccrued - Repay. Equals rounded stored account debt after each completed accounting operation. Between account operations it excludes pending interest.",
               "nonaccounting_events": "PullDebt, Liquidate, DebtSocialized repeat debt changes already emitted as Borrow/Repay; retained with zero delta.",
               "source": "Verified EVault shared/BorrowUtils.sol logBorrow/logRepay/transferBorrow and modules/Liquidation.sol executeLiquidation"})
        print(f"{self.cid} tables built: {sum(len(x) for x in positions.values()):,} position, {sum(len(x) for x in debt_rows.values()):,} debt, {sum(len(x) for x in collateral_rows.values()):,} collateral rows", flush=True)
        self.checkpoint()

    def checkpoint(self):
        stages = {p.stem: json.loads(p.read_text()) for p in (self.out / "checkpoints").glob("*.json")}
        complete = len(stages) == 4 and all(s["complete"] for s in stages.values())
        atomic(self.out / "checkpoint.json", {"generator": GEN, "to_block": self.head,
               "next_block": self.head if complete else 0, "all_event_streams_complete": complete,
               "streams": stages, "derived_tables_atomic": True,
               "file_bytes": {str(p.relative_to(self.out)): p.stat().st_size for p in self.out.rglob("*")
                              if p.is_file() and p.suffix in {".csv", ".jsonl"}},
               "recovery": "Resume each event stream at its own complete-block boundary after truncating to its recorded bytes. Derived tables are atomic rewrites from complete event streams."})

    def load_panel(self):
        self.status = {}
        for path in self.panel.glob("0x*.csv"):
            self.status[path.stem] = Series([{k: int(v) for k, v in r.items()} for r in read_csv(path)])

    def prices(self):
        """Cache daily DefiLlama prices by on-chain asset address, without peg assumptions."""
        self.load_panel()
        metadata_path = self.out / "asset_metadata.json"
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text())
        else:
            collaterals = {r["collateral"] for r in read_jsonl(self.panel / "governance.jsonl")
                           if r["event"] == "GovSetLTV" and r["vault"] in self.lending}
            metadata = {v: {"asset": self.markets[v]["asset"]} for v in collaterals | set(self.lending) if v in self.markets}
            absent = sorted(collaterals - set(metadata))
            results = multicall.try_view(self.target, absent, "asset()", ["address"], block=self.head - 1)
            for v, result in zip(absent, results):
                metadata[v] = {"asset": result[0].lower() if result else None}
            assets = sorted({r["asset"] for r in metadata.values() if r["asset"]})
            results = multicall.try_view(self.target, assets, "decimals()", ["uint8"], block=self.head - 1)
            decimals = {a: result[0] if result else None for a, result in zip(assets, results)}
            for r in metadata.values():
                r["decimals"] = decimals.get(r["asset"])
            atomic(metadata_path, metadata)
        self.decimals_history(metadata)
        start = min(s.rows[0]["timestamp"] for s in self.status.values() if s.rows) // 86400 * 86400
        end = max(s.rows[-1]["timestamp"] for s in self.status.values() if s.rows) // 86400 * 86400
        assets = sorted({r["asset"] for r in metadata.values() if r["asset"]})
        for i, asset in enumerate(assets):
            path = self.out / "prices" / f"{asset}.json"
            previous = json.loads(path.read_text()) if path.exists() else {}
            if previous and not previous.get("request_failed"):
                continue
            coin = f"{self.cfg['defillama']}:{asset}"
            url = f"https://coins.llama.fi/chart/{coin}"
            params = {"start": start, "span": (end-start)//86400 + 1, "period": "1d", "searchWidth": "12h"}
            chunks = []
            for offset in range(0, params["span"], 500):
                part_params = params | {"start": start+offset*86400, "span": min(500, params["span"]-offset)}
                delay, body, error = 2, None, None
                for attempt in range(6):
                    try:
                        response = requests.get(url, params=part_params, timeout=90)
                        if response.status_code == 200:
                            body = response.json()
                            break
                        error = {"http_status": response.status_code, "message": response.text[:500]}
                        if 400 <= response.status_code < 500 and response.status_code != 429:
                            break
                    except (requests.RequestException, ValueError):
                        error = {"message": "Network or JSON error"}
                    if attempt < 5:
                        time.sleep(delay)
                        delay *= 2
                chunks.append({"params": part_params, "response": body, "error": error})
            coins = {}
            for chunk in chunks:
                for key, value in (chunk["response"] or {}).get("coins", {}).items():
                    if key not in coins:
                        coins[key] = value | {"prices": []}
                    coins[key]["prices"].extend(value.get("prices", []))
            body = {"coins": coins}
            atomic(path, {"url": url, "params": params, "retrieved_utc": datetime.now(timezone.utc).isoformat(),
                          "response": body, "chunks": chunks, "previous_failed_request": previous if previous.get("request_failed") else None,
                          "request_failed": any(c["response"] is None for c in chunks)})
            print(f"{self.cid} prices {i+1}/{len(assets)} {asset}: {len((body or {}).get('coins', {}).get(coin, {}).get('prices', []))} points", flush=True)

    def decimals_history(self, metadata):
        """Record every asset whose decimals() changed during the panel period."""
        path = self.out / "asset_decimals_history.json"
        if path.exists():
            return
        assets = sorted({r["asset"] for r in metadata.values() if r["asset"]})
        grid = sorted({self.start + (self.head - 1 - self.start) * i // 7 for i in range(8)})

        def read(block):
            results = retry_state_gap(multicall.try_view, self.target, assets, "decimals()", ["uint8"], block=block)
            return {a: r[0] for a, r in zip(assets, results) if r}

        by_block = {block: read(block) for block in grid}
        history = {}
        for asset in assets:
            seen = [(b, by_block[b][asset]) for b in grid if asset in by_block[b]]
            steps = seen[:1]
            for (lo, before), (hi, after) in zip(seen, seen[1:]):
                if before == after:
                    continue
                while hi - lo > 1:
                    mid = (lo + hi) // 2
                    value = retry_state_gap(multicall.try_view, self.target, [asset], "decimals()", ["uint8"], block=mid)[0]
                    if value and value[0] == before:
                        lo = mid
                    else:
                        hi = mid
                steps.append((hi, after))
            if len(steps) > 1:
                history[asset] = [list(x) for x in steps]
        atomic(path, {"grid_blocks": grid, "assets": history,
                      "note": "Assets listed only if decimals() changed between grid blocks; each step is [first block, decimals]. Assets absent here kept the asset_metadata.json value at every grid block where they existed."})
        print(f"{self.cid} decimals history: {len(history)} of {len(assets)} assets changed: {history}", flush=True)

    def distribution(self):
        """Attribute each day's last panel debt using active account collateral."""
        self.load_panel()
        metadata = json.loads((self.out / "asset_metadata.json").read_text())
        decimal_steps = json.loads((self.out / "asset_decimals_history.json").read_text())["assets"]
        collaterals = json.loads((self.out / "collateral_sets.json").read_text())

        def decimals_at(info, block):
            steps = decimal_steps.get(info.get("asset"))
            if not steps:
                return info.get("decimals")
            i = bisect.bisect_right([b for b, _ in steps], block) - 1
            return steps[max(i, 0)][1]
        prices = {}
        for path in (self.out / "prices").glob("*.json"):
            body = json.loads(path.read_text()).get("response") or {}
            info = body.get("coins", {}).get(f"{self.cfg['defillama']}:{path.stem}", {})
            points = sorted(info.get("prices", []), key=lambda x: x["timestamp"])
            prices[path.stem] = ([int(p["timestamp"]) for p in points], points)
        balances, enabled = defaultdict(list), defaultdict(list)
        seen = set()
        for path in (self.out / "collateral").glob("*.csv"):
            for r in read_csv(path):
                identity = (r["account"], r["collateral_vault"], r["block"], r["log_index"])
                if identity not in seen:
                    seen.add(identity)
                    balances[r["account"], r["collateral_vault"]].append(r)
        balance_series = {k: Series(sorted(rows, key=lambda r: (int(r["block"]), int(r["log_index"])))) for k, rows in balances.items()}
        for r in read_jsonl(self.out / "events/evc.jsonl"):
            enabled[r["account"], r["collateral"]].append(r)
        enable_series = {k: Series(rows) for k, rows in enabled.items()}
        ltv_rows = defaultdict(list)
        for r in read_jsonl(self.panel / "governance.jsonl"):
            if r["event"] == "GovSetLTV" and r["vault"] in self.lending:
                ltv_rows[r["vault"], r["collateral"]].append(r)
        ltv_series = {k: Series(rows) for k, rows in ltv_rows.items()}
        price_coverage = Counter()
        used_prices = set()

        def accepted(v, collateral, cutoff, ts):
            """Effective liquidation LTV > 0 at the snapshot (EVault LTVConfig.getLTV)."""
            series = ltv_series.get((v, collateral))
            row = series.at(cutoff) if series else None
            if row is None:
                return False
            target, initial, final = row["targetTimestamp"], row["initialLiquidationLTV"], row["liquidationLTV"]
            if ts >= target or final >= initial:
                return final > 0
            return final + (initial - final) * (target - ts) // row["rampDuration"] > 0

        def price(asset, day):
            stamps, points = prices.get(asset, ([], []))
            if not stamps:
                return None
            j = bisect.bisect_left(stamps, day)
            candidates = [k for k in (j-1, j) if 0 <= k < len(stamps)]
            index = min(candidates, key=lambda k: abs(stamps[k]-day))
            point = points[index]
            if abs(stamps[index]-day) > 43200 or point["price"] <= 0:
                return None
            used_prices.add((asset, day))
            return Decimal(str(point["price"]))

        head_ts_path = self.out / "head.json"
        if not head_ts_path.exists():
            block = retry_state_gap(rpc.get_block, self.target, self.head - 1)
            atomic(head_ts_path, {"block": self.head - 1, "timestamp": int(block["timestamp"], 16)})
        head_ts = json.loads(head_ts_path.read_text())["timestamp"]
        end = head_ts // 86400 * 86400
        output, diagnostics = [], []
        usd_totals = {k: Decimal(0) for k in ("debt", "attributed", "multi_collateral")}
        usd_reasons = defaultdict(Decimal)
        loan_price_coverage = Counter()
        for index, v in enumerate(sorted(self.lending)):
            status = self.status[v]
            raw_debt = [r for r in read_csv(self.out / "borrows" / f"{v}.csv") if r["accounting_event"] == "1"]
            events, pos = [], 0
            for r in raw_debt:
                key = (int(r["block"]), int(r["log_index"]))
                st = status.following(key)
                if st is None or st["block"] != key[0]:
                    raise RuntimeError(f"No same-block VaultStatus after account update {v} {key}")
                events.append((key, r["account"], int(r["cum_event_assets"]), st["interestAccumulator"]))
            stored = {}
            status_times = [r["timestamp"] for r in status.rows]
            start = status.rows[0]["timestamp"] // 86400 * 86400
            for day in range(start, end+1, 86400):
                si = bisect.bisect_right(status_times, min(day+86399, head_ts)) - 1
                if si < 0:
                    continue
                st = status.rows[si]
                cutoff = (st["block"], st["log_index"])
                while pos < len(events) and events[pos][0] <= cutoff:
                    _, account, amount, acc = events[pos]
                    if amount > 0:
                        stored[account] = (amount, acc)
                    else:
                        stored.pop(account, None)
                    pos += 1
                total = Decimal(st["totalBorrows"])
                estimates = {a: Decimal(amount)*st["interestAccumulator"]/acc for a, (amount, acc) in stored.items()}
                reconstructed = sum(estimates.values(), Decimal(0))
                scale = min(Decimal(1), total/reconstructed) if reconstructed else Decimal(1)
                buckets = defaultdict(Decimal)
                unattributed = max(Decimal(0), total-reconstructed)
                reasons = defaultdict(Decimal)
                if unattributed:
                    reasons["debt_reconstruction_residual"] += unattributed
                multi = Decimal(0)
                for account, debt in estimates.items():
                    debt *= scale
                    if not debt:
                        continue
                    active, delisted = [], False
                    for collateral in collaterals.get(v, []):
                        en = enable_series.get((account, collateral))
                        if en is None or not (en.at(cutoff) or {}).get("enabled"):
                            continue
                        series = balance_series.get((account, collateral))
                        row = series.at(cutoff) if series else None
                        if row and int(row["cum_shares"]) > 0:
                            if accepted(v, collateral, cutoff, st["timestamp"]):
                                active.append((collateral, int(row["cum_shares"])))
                            else:
                                delisted = True
                    if len(active) == 1:
                        buckets[active[0][0]] += debt
                    elif not active:
                        unattributed += debt
                        reason = "only_collateral_at_zero_liquidation_ltv" if delisted else "no_positive_enabled_accepted_collateral"
                        reasons[reason] += debt
                    else:
                        multi += debt
                        values, missing = {}, False
                        for collateral, shares in active:
                            info = metadata.get(collateral, {})
                            cst = self.status[collateral].at(cutoff) if collateral in self.status else None
                            usd = price(info.get("asset"), day)
                            price_coverage["multi_collateral_value_requests"] += 1
                            if usd is not None:
                                price_coverage["with_price"] += 1
                            decimals = decimals_at(info, st["block"])
                            if cst is None or cst["totalShares"] <= 0 or usd is None or decimals is None:
                                missing = True
                                continue
                            values[collateral] = Decimal(shares)*(cst["cash"]+cst["totalBorrows"])/cst["totalShares"]*usd/(Decimal(10)**decimals)
                        value_sum = sum(values.values(), Decimal(0))
                        if missing or value_sum <= 0:
                            unattributed += debt
                            reasons["missing_price_panel_or_decimals"] += debt
                        else:
                            for collateral, value in values.items():
                                buckets[collateral] += debt*value/value_sum
                integer_buckets = {c: int(x) for c, x in buckets.items() if x > 0}
                remainder = int(total) - sum(integer_buckets.values())
                context = {"vault": v, "day": datetime.fromtimestamp(day, timezone.utc).date().isoformat(),
                           "timestamp": day, "panel_block": st["block"], "panel_log_index": st["log_index"], "totalBorrows": int(total)}
                for collateral, amount in sorted(integer_buckets.items()):
                    output.append({**context, "collateral_vault": collateral, "attributed": amount, "unattributed": 0})
                output.append({**context, "collateral_vault": "", "attributed": 0, "unattributed": remainder})
                loan_info = metadata[v]
                loan_price = price(loan_info["asset"], day)
                loan_decimals = decimals_at(loan_info, st["block"])
                if total:
                    loan_price_coverage["positive_debt_vault_days"] += 1
                    if loan_price is not None and loan_decimals is not None:
                        loan_price_coverage["priced_vault_days"] += 1
                        factor = loan_price / Decimal(10)**loan_decimals
                        usd_totals["debt"] += total*factor
                        usd_totals["attributed"] += sum(integer_buckets.values())*factor
                        usd_totals["multi_collateral"] += multi*factor
                        for reason, amount in reasons.items():
                            usd_reasons[reason] += amount*factor
                diagnostics.append({**context, "reconstructed_debt": str(reconstructed), "overestimate_scaled": str(max(Decimal(0), reconstructed-total)),
                                    "multi_collateral_debt": str(multi), "attributed": sum(integer_buckets.values()), "unattributed": remainder,
                                    "reasons": {k: str(x) for k, x in reasons.items()}})
            print(f"{self.cid} distribution {index+1}/{len(self.lending)} {v}", flush=True)
        atomic(self.out / "borrow_distribution.csv", csv_text(["vault", "day", "timestamp", "panel_block", "panel_log_index", "totalBorrows", "collateral_vault", "attributed", "unattributed"], output))
        atomic(self.out / "distribution_diagnostics.jsonl", "".join(json.dumps(r) + "\n" for r in diagnostics))
        atomic(self.out / "borrow_distribution.csv.meta.json", {"generator": GEN, "to_block_exclusive": self.head,
               "rule": "At each UTC day's last VaultStatus (carry forward on inactive days), use account debt estimated from Borrow + InterestAccrued - Repay scaled by the last panel interestAccumulator divided by its accumulator at the account's last update. Candidate collateral is positive share balance, EVC-enabled, and accepted by the lending vault at the snapshot: its latest GovSetLTV at or before the snapshot gives an effective liquidation LTV above zero (EVault LTVConfig.getLTV, including ramp-downs). Debt whose only enabled positive collateral sits at zero liquidation LTV is unattributed under its own reason. One collateral receives all debt without requiring a price. Multiple collaterals split in proportion to shares * (cash+totalBorrows)/totalShares * USD price / 10**asset_decimals, using latest collateral VaultStatus at or before the lending snapshot and the asset's decimals() in force at the snapshot block (asset_decimals_history.json). If any active collateral lacks price, panel, or decimals, the whole account is unattributed. Missing reconstructed debt is unattributed; overestimated debt is proportionally scaled down. Floor each attributed bucket to asset units and place remainder in unattributed.",
               "price_source": "https://coins.llama.fi/chart/{chain}:{asset_address}", "price_documentation": "https://github.com/DefiLlama/api-sdk#getchart",
               "price_sampling": "Daily UTC midnight, nearest DefiLlama observation within 12 hours; no stablecoin peg or cross-chain price substitution.",
               "price_coverage": dict(price_coverage), "distinct_asset_days_used": len(used_prices),
               "loan_price_coverage": dict(loan_price_coverage), "usd_debt_day_totals": {k: str(x) for k, x in usd_totals.items()},
               "usd_unattributed_day_totals_by_reason": {k: str(x) for k, x in sorted(usd_reasons.items())},
               "priced_debt_attributed_share": float(usd_totals["attributed"]/usd_totals["debt"]) if usd_totals["debt"] else None,
               "priced_debt_multi_collateral_share": float(usd_totals["multi_collateral"]/usd_totals["debt"]) if usd_totals["debt"] else None,
               "debt_note": "Estimated account weights, not an exact reproduction of fractional Owed storage. Conservation is a construction invariant, independently checked against panel CSVs."})

    def validate(self):
        """Save every archive sample and independently check all daily totals."""
        self.load_sources()
        self.load_panel()
        validation_path = self.out / "validation.json"
        fingerprint = CODE_SHA256
        result = json.loads(validation_path.read_text()) if validation_path.exists() else {"head_block": self.head - 1, "code_sha256": fingerprint}
        if result.get("code_sha256") != fingerprint:
            atomic(self.out / "validation_history" / f"{result.get('code_sha256', 'unversioned')}.json", result)
            result = {"head_block": self.head - 1, "code_sha256": fingerprint}
        if result["head_block"] != self.head - 1:
            raise RuntimeError("Validation head changed")
        summaries = list(read_jsonl(self.out / "vaults.jsonl"))
        positions = defaultdict(list)
        for v in self.earn:
            for r in read_csv(self.out / "positions" / f"{v}.csv"):
                positions[v, r["strategy"]].append(r)
        position_series = {k: Series(rows) for k, rows in positions.items()}

        def summarize_checks(rows):
            return {"checks": len(rows), "exact": sum(r.get("actual") == r["expected"] for r in rows),
                    "rpc_failures": sum(r.get("actual") is None for r in rows),
                    "mismatches": [r for r in rows if r.get("actual") != r["expected"]]}

        def persist(name, summary, rows):
            atomic(self.out / "validation_samples" / f"{name}.json", rows)
            result[name] = {**summary, "samples_file": f"validation_samples/{name}.json"}
            atomic(validation_path, result)
            print(f"{self.cid} validation {name}: {json.dumps({k: v for k, v in summary.items() if k != 'mismatches'})}", flush=True)

        def archive(rows, signature):
            def one(row):
                try:
                    raw = retry_state_gap(rpc.eth_call, self.target, row["target"], evmabi.encode_call(signature, ["address"], [row["account"]]), row["block"])
                    actual = int(raw, 16) if raw not in ("0x", "") else None
                    return {**row, "actual": actual}
                except Exception:
                    return {**row, "actual": None, "error": "archive eth_call failed; provider details omitted to protect credentials"}
            with ThreadPoolExecutor(max_workers=6) as pool:
                return list(pool.map(one, rows))

        if "earn_head" not in result:
            rows = []
            for summary in summaries:
                v = summary["vault"]
                for token in summary["strategies_touched"]:
                    series = position_series.get((v, token))
                    rows.append({"vault": v, "target": token, "account": v, "block": self.head-1,
                                 "expected": int(series.rows[-1]["cum_shares"]) if series else 0})
            calls = [(r["target"], evmabi.encode_call("balanceOf(address)", ["address"], [r["account"]])) for r in rows]
            actuals = retry_state_gap(multicall.aggregate3, self.target, calls, block=self.head-1, chunk=100)
            rows = [r | {"actual": int(raw, 16) if ok and raw != "0x" else None, "returndata": raw}
                    for r, (ok, raw) in zip(rows, actuals)]
            for row in rows:
                raw = row.pop("returndata")
                if row["actual"] is None:
                    code = retry_state_gap(rpc.get_code, self.target, row["target"], self.head-1)
                    row["failure_reason"] = "No deployed code at head" if code == "0x" else "balanceOf failed despite nonempty deployed code"
                    row["runtime_bytes"] = (len(code)-2)//2
                    if raw.startswith("0x08c379a0"):
                        row["revert_reason"] = evmabi.decode(["string"], raw[10:])[0]
                    row["position_events"] = len(position_series[row["vault"], row["target"]].rows) if (row["vault"], row["target"]) in position_series else 0
                row["is_registry_evault"] = row["target"] in self.markets
            evault_rows = [r for r in rows if r["is_registry_evault"]]
            persist("earn_head", summarize_checks(rows) | {"vaults": len(self.earn)}, rows)
            result["earn_head"]["registry_evault_checks"] = len(evault_rows)
            result["earn_head"]["registry_evault_exact"] = sum(r["actual"] == r["expected"] for r in evault_rows)
            atomic(validation_path, result)
        if "earn_random" not in result:
            rng = random.Random(20261005 + int(self.cid) + 1)
            keys = sorted(position_series)
            rows, taken = [], set()
            while len(rows) < 200:
                v, token = rng.choice(keys)
                block = rng.randint(max(self.earn[v]["created_block"], int(position_series[v, token].rows[0]["block"])), self.head-1)
                if (v, token, block) in taken:
                    continue
                taken.add((v, token, block))
                row = position_series[v, token].at((block, 1 << 60))
                rows.append({"vault": v, "target": token, "account": v, "block": block,
                             "expected": int(row["cum_shares"]) if row else 0})
            rows = archive(rows, "balanceOf(address)")
            persist("earn_random", summarize_checks(rows) | {"seed": 20261006 + int(self.cid), "sampling": "Uniform observed vault-strategy pairs, uniform block from first position event to scan head; block-end state"}, rows)
        if "roles" not in result:
            rows, calls = [], []
            candidates = defaultdict(set)
            for row in read_jsonl(self.out / "roles.jsonl"):
                if row["role"] in {"allocator", "payout_manager"}:
                    candidates[row["vault"], row["role"]].add(row["address"])
            for summary in summaries:
                v, state = summary["vault"], summary["roles_at_end"]
                functions = {e.get("name") for e in self.src[v]["abi"] if e.get("type") == "function"}
                for role in ["owner", "curator", "guardian"]:
                    if role not in functions:
                        continue
                    calls.append((v, evmabi.selector(role + "()")))
                    rows.append({"vault": v, "role": role, "expected": next(iter(state.get(role, [])), ZERO), "return_type": "address"})
                for role, getter in [("allocator", "isAllocator"), ("payout_manager", "isPayoutManager")]:
                    if getter not in functions:
                        continue
                    for address in sorted(candidates[v, role] | {ZERO}):
                        calls.append((v, evmabi.encode_call(getter + "(address)", ["address"], [address])))
                        rows.append({"vault": v, "role": role, "address": address, "expected": address in state.get(role, []), "return_type": "bool"})
                if "hasRole" in functions:
                    all_addresses = {r["address"] for r in read_jsonl(self.out / "roles.jsonl") if r["vault"] == v}
                    for role_id, role in self.src[v]["role_names"].items():
                        for address in all_addresses:
                            calls.append((v, evmabi.encode_call("hasRole(bytes32,address)", ["bytes32", "address"], [bytes.fromhex(role_id[2:]), address])))
                            rows.append({"vault": v, "role": role, "address": address, "expected": address in state.get(role, []), "return_type": "bool"})
            answers = retry_state_gap(multicall.aggregate3, self.target, calls, block=self.head-1, chunk=100)
            for row, (ok, raw) in zip(rows, answers):
                row["actual"] = clean(evmabi.decode([row["return_type"]], raw)[0]) if ok and raw != "0x" else None
            failed_vaults = {r["vault"] for r in rows if r["actual"] != r["expected"]}
            persist("roles", summarize_checks(rows) | {"vaults": len(self.earn), "vaults_passed": len(self.earn)-len(failed_vaults)}, rows)
        if "debt_random" not in result:
            rng = random.Random(20261005 + int(self.cid) + 2)
            debts = defaultdict(list)
            for v in self.lending:
                for r in read_csv(self.out / "borrows" / f"{v}.csv"):
                    if r["accounting_event"] == "1":
                        debts[v, r["account"]].append(r)
            series = {k: Series(rows) for k, rows in debts.items()}
            keys, samples, taken = sorted(series), [], set()
            while len(samples) < 200:
                v, account = rng.choice(keys)
                block = rng.randint(int(series[v, account].rows[0]["block"]), self.head-1)
                if (v, account, block) in taken:
                    continue
                taken.add((v, account, block))
                row = series[v, account].at((block, 1 << 60))
                samples.append({"vault": v, "target": v, "account": account, "block": block, "expected": int(row["cum_event_assets"])})
            samples = archive(samples, "debtOf(address)")
            differences = []
            for row in samples:
                if row["actual"] is not None:
                    row["relative_difference"] = float(Decimal(abs(row["expected"]-row["actual"]))/max(1, row["actual"]))
                    differences.append(row["relative_difference"])
            differences.sort()
            within = sum(x <= 1e-6 for x in differences)
            summary = summarize_checks(samples) | {"within_1e_6": within, "share_within_1e_6": within/len(differences) if differences else None,
                       "nonzero_actual_debt_samples": sum(bool(r.get("actual")) for r in samples),
                       "nonzero_within_1e_6": sum(bool(r.get("actual")) and r.get("relative_difference", 1) <= 1e-6 for r in samples),
                       "relative_difference": {str(p): differences[round((len(differences)-1)*p)] if differences else None for p in [0, .5, .9, .95, .99, 1]},
                       "explanation": "Compared cum_event_assets, including emitted InterestAccrued, to debtOf at a later random block. Pending interest accrues without account events; integer event amounts do not expose fractional Owed storage. These are not expected to match exactly.",
                       "sampling": "Uniform vault-account pairs with accounting events, uniform block from first account event through head; block-end state"}
            persist("debt_random", summary, samples)
        if "collateral_random" not in result:
            rng = random.Random(20261005 + int(self.cid) + 3)
            collateral = defaultdict(dict)
            for path in (self.out / "collateral").glob("*.csv"):
                for r in read_csv(path):
                    collateral[r["account"], r["collateral_vault"]][int(r["block"]), int(r["log_index"])] = r
            series = {k: Series([rows[t] for t in sorted(rows)]) for k, rows in collateral.items()}
            keys, samples, taken = sorted(series), [], set()
            while len(samples) < 200:
                account, token = rng.choice(keys)
                block = rng.randint(int(series[account, token].rows[0]["block"]), self.head-1)
                if (account, token, block) in taken:
                    continue
                taken.add((account, token, block))
                row = series[account, token].at((block, 1 << 60))
                samples.append({"target": token, "account": account, "block": block, "expected": int(row["cum_shares"])})
            samples = archive(samples, "balanceOf(address)")
            persist("collateral_random", summarize_checks(samples), samples)
        def positive_points(series, value, seed):
            """200 distinct block-end points where the reconstructed value is positive."""
            rng, spans = random.Random(seed), {}
            for key, s in series.items():
                found = []
                for i, row in enumerate(s.rows):
                    end = int(s.rows[i+1]["block"]) - 1 if i + 1 < len(s.rows) else self.head - 1
                    if value(row) > 0 and end >= int(row["block"]):
                        found.append((int(row["block"]), end, row))
                if found:
                    spans[key] = found
            keys, points, taken = sorted(spans), [], set()
            while len(points) < 200:
                key = rng.choice(keys)
                lo, hi, row = rng.choice(spans[key])
                block = rng.randint(lo, hi)
                if (key, block) not in taken:
                    taken.add((key, block))
                    points.append((key, block, row))
            return points, {"keys_with_positive_interval": len(keys), "positive_intervals": sum(len(x) for x in spans.values())}

        sampling_positive = "Uniform key with a positive interval, uniform positive interval, uniform block inside it; block-end state"
        if "earn_positive" not in result:
            seed = 20261005 + int(self.cid) + 4
            points, population = positive_points(position_series, lambda r: int(r["cum_shares"]), seed)
            rows = archive([{"vault": v, "target": token, "account": v, "block": block, "expected": int(row["cum_shares"])}
                            for (v, token), block, row in points], "balanceOf(address)")
            persist("earn_positive", summarize_checks(rows) | population | {"seed": seed, "sampling": sampling_positive}, rows)
        if "collateral_positive" not in result:
            seed = 20261005 + int(self.cid) + 5
            collateral = defaultdict(dict)
            for path in (self.out / "collateral").glob("*.csv"):
                for r in read_csv(path):
                    collateral[r["account"], r["collateral_vault"]][int(r["block"]), int(r["log_index"])] = r
            series = {k: Series([rows[t] for t in sorted(rows)]) for k, rows in collateral.items()}
            points, population = positive_points(series, lambda r: int(r["cum_shares"]), seed)
            rows = archive([{"target": token, "account": account, "block": block, "expected": int(row["cum_shares"])}
                            for (account, token), block, row in points], "balanceOf(address)")
            persist("collateral_positive", summarize_checks(rows) | population | {"seed": seed, "sampling": sampling_positive}, rows)
        if "debt_positive" not in result:
            seed = 20261005 + int(self.cid) + 6
            debts = defaultdict(list)
            for v in self.lending:
                for r in read_csv(self.out / "borrows" / f"{v}.csv"):
                    if r["accounting_event"] == "1":
                        debts[v, r["account"]].append(r)
            points, population = positive_points({k: Series(rows) for k, rows in debts.items()},
                                                 lambda r: int(r["cum_event_assets"]), seed)

            def one(point):
                (v, account), block, row = point
                status = self.status[v].following((int(row["block"]), int(row["log_index"])))
                sample = {"vault": v, "account": account, "block": block, "event_block": int(row["block"]),
                          "event_log_index": int(row["log_index"]), "cum_event_assets": int(row["cum_event_assets"]),
                          "event_accumulator": status["interestAccumulator"]}
                calls = [(v, evmabi.encode_call("debtOf(address)", ["address"], [account])),
                         (v, evmabi.selector("interestAccumulator()"))]
                try:
                    (ok_debt, debt), (ok_acc, acc) = retry_state_gap(multicall.aggregate3, self.target, calls, block=block)
                except Exception:
                    return sample | {"actual": None, "error": "archive multicall failed; provider details omitted to protect credentials"}
                if not (ok_debt and ok_acc):
                    return sample | {"actual": None, "error": "debtOf or interestAccumulator reverted"}
                sample.update(actual=int(debt, 16), block_accumulator=int(acc, 16))
                accrued = Decimal(sample["cum_event_assets"]) * sample["block_accumulator"] / sample["event_accumulator"]
                expected = int(accrued.to_integral_value(rounding="ROUND_CEILING"))
                sample.update(expected=expected, wei_difference_accrued=sample["actual"] - expected,
                              relative_difference_event=abs(sample["cum_event_assets"] - sample["actual"]) / max(1, sample["actual"]),
                              relative_difference_accrued=abs(expected - sample["actual"]) / max(1, sample["actual"]))
                return sample

            with ThreadPoolExecutor(max_workers=6) as pool:
                samples = list(pool.map(one, points))
            ok = [r for r in samples if r.get("actual") is not None]

            def quantiles(key):
                xs = sorted(r[key] for r in ok)
                return {str(q): xs[round((len(xs)-1)*q)] for q in [0, .5, .9, .95, .99, 1]} if xs else None
            summary = {"checks": len(samples), "rpc_failures": len(samples) - len(ok),
                       "exact_after_accrual": sum(r["wei_difference_accrued"] == 0 for r in ok),
                       "within_1_wei_after_accrual": sum(abs(r["wei_difference_accrued"]) <= 1 for r in ok),
                       "within_1e_6_after_accrual": sum(r["relative_difference_accrued"] <= 1e-6 for r in ok),
                       "within_1e_6_event_only": sum(r["relative_difference_event"] <= 1e-6 for r in ok),
                       "relative_difference_event_only": quantiles("relative_difference_event"),
                       "relative_difference_after_accrual": quantiles("relative_difference_accrued"),
                       "mismatches": [r for r in ok if abs(r["wei_difference_accrued"]) > 1],
                       "method": "expected = ceil(cum_event_assets * interestAccumulator() at the sampled block / panel interestAccumulator of the same-block VaultStatus after the account's last accounting event); actual = debtOf(account) at the sampled block. cum_event_assets is the owed amount rounded up at the last event, so the accrued expectation can exceed debtOf by 1 wei; a mismatch is a difference above 1 wei. Relative differences divide by debtOf, so 1 wei on dust debts is large. Event-only compares cum_event_assets directly, so its gap is the interest accrued since the last account event.",
                       **population, "seed": seed, "sampling": sampling_positive}
            persist("debt_positive", summary, samples)
        if "distribution" not in result:
            totals = Counter()
            for row in read_csv(self.out / "borrow_distribution.csv"):
                totals[row["vault"], row["day"]] += int(row["attributed"])+int(row["unattributed"])
            diagnostics = list(read_jsonl(self.out / "distribution_diagnostics.jsonl"))
            head_day = json.loads((self.out / "head.json").read_text())["timestamp"]//86400
            expected_pairs = {(v, datetime.fromtimestamp(day*86400, timezone.utc).date().isoformat())
                              for v in self.lending for day in range(self.status[v].rows[0]["timestamp"]//86400, head_day+1)}
            missing_pairs = sorted(expected_pairs-set(totals))
            extra_pairs = sorted(set(totals)-expected_pairs)
            failed = []
            per_vault = defaultdict(lambda: {"total": Decimal(0), "attributed": Decimal(0), "multi": Decimal(0)})
            for row in diagnostics:
                v = row["vault"]
                timestamp = row["timestamp"] + 86399
                rows = self.status[v].rows
                idx = bisect.bisect_right([r["timestamp"] for r in rows], timestamp)-1
                expected = rows[idx]["totalBorrows"]
                observed = totals[v, row["day"]]
                if expected != observed:
                    failed.append({"vault": v, "day": row["day"], "expected": expected, "actual": observed})
                per_vault[v]["total"] += expected
                per_vault[v]["attributed"] += row["attributed"]
                per_vault[v]["multi"] += Decimal(row["multi_collateral_debt"])
            metadata = json.loads((self.out / "asset_metadata.json").read_text())
            units = defaultdict(lambda: {"total": Decimal(0), "attributed": Decimal(0), "multi": Decimal(0)})
            for v, metrics in per_vault.items():
                asset = metadata[v]["asset"]
                for k, value in metrics.items():
                    units[asset][k] += value
            shares = [float(m["attributed"]/m["total"]) for m in per_vault.values() if m["total"]]
            summary = {"checks": len(diagnostics), "exact": len(diagnostics)-len(failed), "mismatches": failed,
                       "expected_vault_days": len(expected_pairs), "missing_vault_days": missing_pairs, "extra_vault_days": extra_pairs,
                       "mean_per_vault_attributed_share": sum(shares)/len(shares) if shares else None,
                       "asset_unit_day_totals": {a: {**{k: str(x) for k, x in m.items()}, "attributed_share": float(m["attributed"]/m["total"]) if m["total"] else None,
                                                      "multi_collateral_share": float(m["multi"]/m["total"]) if m["total"] else None} for a, m in units.items()},
                       "note": "Conservation is guaranteed by explicit residual accounting; this check is not an accuracy test of account-level weights. Amounts are loan-asset-unit-days, grouped by loan asset."}
            distribution_meta = json.loads((self.out / "borrow_distribution.csv.meta.json").read_text())
            summary.update({k: distribution_meta[k] for k in ["priced_debt_attributed_share", "priced_debt_multi_collateral_share", "usd_debt_day_totals", "loan_price_coverage", "price_coverage"]})
            persist("distribution", summary, failed)

    def report(self):
        """Write exact counts, sizes, provenance gaps, and accumulated RPC usage."""
        self.checkpoint()
        summaries = list(read_jsonl(self.out / "vaults.jsonl"))
        counts = {}
        for directory in ["positions", "flows", "borrows", "collateral"]:
            files = list((self.out / directory).glob("*.csv"))
            nrows = {p.stem: sum(1 for _ in p.open())-1 for p in files}
            counts[directory] = {"files": len(files), "nonempty_files": sum(n > 0 for n in nrows.values()),
                                 "rows": sum(nrows.values()), "bytes": sum(p.stat().st_size for p in files)}
        for filename in ["roles.jsonl", "actions.jsonl", "fees.csv", "borrow_distribution.csv"]:
            path = self.out / filename
            counts[filename] = {"rows": sum(1 for _ in path.open()) - int(path.suffix == ".csv"), "bytes": path.stat().st_size}
        self.load_sources()
        account_sets = json.loads((self.out / "accounts.json").read_text())
        report = {"chain_id": int(self.cid), "head_block": self.head-1, "earn_vaults": len(self.earn),
                  "strategies_touched": len(set().union(*(set(r["strategies_touched"]) for r in summaries))),
                  "evault_strategies_touched": len(set().union(*(set(r["strategies_touched"]) for r in summaries)) & set(self.markets)),
                  "non_evault_strategies": sorted(set().union(*(set(r["strategies_touched"]) for r in summaries)) - set(self.markets)),
                  "vault_strategy_pairs": sum(len(r["strategies_touched"]) for r in summaries),
                  "lending_vaults": len(self.lending), "borrowing_accounts": len(set().union(*(set(a) for a in account_sets.values()))),
                  "vault_account_pairs": sum(len(a) for a in account_sets.values()), "files": counts,
                  "raw_event_counts": {p.stem: json.loads(p.read_text())["rows"] for p in (self.out / "checkpoints").glob("*.json")},
                  "earn_event_counts": dict(sum((Counter(r["events"]) for r in summaries), Counter())),
                  "source_uncertainties": {v: r["uncertainty"] for v, r in self.src.items() if r.get("uncertainty")},
                  "version_counts": dict(Counter(self.src[v]["version"] for v in self.earn)),
                  "verified_source_vaults": sum(not self.src[v].get("explorer") for v in self.earn),
                  "unverified_factories": [a for a in {r.get("factory") for r in self.earn.values()} if a and self.src[a].get("explorer")],
                  "rpc_requests": sum(r["rpc_requests"] for r in read_jsonl(self.out / "runs.jsonl")) + rpc.calls_made(),
                  "recorded_stage_runtime_seconds": sum(r["runtime_seconds"] for r in read_jsonl(self.out / "runs.jsonl")),
                  "file_sizes": {str(p.relative_to(self.out)): p.stat().st_size for p in self.out.rglob("*") if p.is_file() and p.name not in {"report.json", "runs.jsonl"}},
                  "limitations": ["No 2024-design vault was found in the supplied registry; only observed, verified 2025 events are implemented and tested.",
                                  "Unverified vaults and factory are identified in sources.json; classification for unmatched runtimes is inferred.",
                                  "Distribution uses estimated accrued account balances. See diagnostics for debt residuals and scaling."]}
        report["output_bytes_excluding_report_and_runs"] = sum(report["file_sizes"].values())
        source_dirs = {ROOT / r["source_dir"] for r in self.src.values()}
        source_files = [p for folder in source_dirs for p in folder.iterdir() if p.is_file()]
        report["contract_source_files"] = len(source_files)
        report["contract_source_bytes"] = sum(p.stat().st_size for p in source_files)
        first_source = min(p.stat().st_mtime for p in source_files)
        report["first_source_cache_file_mtime_utc"] = datetime.fromtimestamp(first_source, timezone.utc).isoformat()
        report["wall_seconds_since_first_source_cache_file"] = time.time()-first_source
        report["price_cache"] = {"assets": len(list((self.out / "prices").glob("*.json"))),
                                "assets_with_any_price": sum(bool(next(iter((json.loads(p.read_text()).get("response") or {}).get("coins", {}).values()), {}).get("prices")) for p in (self.out / "prices").glob("*.json"))}
        atomic(self.out / "report.json", report)
        print(json.dumps({k: v for k, v in report.items() if k != "file_sizes"}, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain", required=True, choices=["1", "9745", "59144"])
    parser.add_argument("--stage", default="all", choices=["all", "finalize", "sources", "resolve", "estimate", "scan", "evc", "build", "prices", "distribution", "validate", "report"])
    args = parser.parse_args()
    job = Job(args.chain)
    started = time.time()
    try:
        if args.stage in {"all", "finalize"}:
            stages = ["sources", "resolve"] if args.stage == "all" else []
            for stage in stages + ["scan", "build", "prices", "distribution", "validate", "report"]:
                getattr(job, stage)()
        else:
            getattr(job, args.stage)()
    except Exception as exc:
        message = str(exc)
        for key in ("ETHERSCAN_API_KEY", "DRPC_API_KEY", "HYPERSYNC_API_TOKEN"):
            secret = env.get(key)
            if secret:
                message = message.replace(secret, "[redacted]")
        raise SystemExit(f"{type(exc).__name__}: {message}") from None
    finally:
        path = job.out / "runs.jsonl"
        with path.open("a") as fh:
            fh.write(json.dumps({"stage": args.stage, "started_utc": datetime.fromtimestamp(started, timezone.utc).isoformat(),
                                 "runtime_seconds": time.time() - started, "rpc_requests": rpc.calls_made()}) + "\n")


if __name__ == "__main__":
    main()
