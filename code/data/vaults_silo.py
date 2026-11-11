# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""Reconstruct Silo curator vault histories from verified ABIs and HyperSync."""
import argparse
import csv
import hashlib
import io
import json
import os
import pathlib
import random
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from eth_abi.exceptions import DecodingError

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common import env, evmabi, hypersync_client, io_utils, logs, multicall, rpc

GEN = "code/data/vaults_silo.py"
ZERO = "0x" + "0" * 40


def atomic(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    io_utils._atomic_text(path, json.dumps(obj, indent=2, sort_keys=True) + "\n")


def read_rows(path):
    with path.open() as handle:
        return [json.loads(line) for line in handle if line.strip()]


def explorer_source(cid, address):
    """Persist Etherscan's verified source, ABI and provenance, without keys."""
    directory = io_utils.DATA / f"contracts/{cid}-{address}"
    cache = directory / "_explorer.json"
    if cache.exists():
        row = json.loads(cache.read_text())
        if isinstance(row, list):
            row = row[0]
        if row.get("SourceCode"):
            return row
    for attempt in range(6):
        try:
            response = requests.get("https://api.etherscan.io/v2/api", params={
                "chainid": cid, "module": "contract", "action": "getsourcecode",
                "address": address, "apikey": env.require("ETHERSCAN_API_KEY")}, timeout=60)
            body = response.json()
        except (requests.RequestException, ValueError):
            if attempt == 5:
                raise RuntimeError(f"Etherscan transport failed: {cid} {address}") from None
            time.sleep(2 ** attempt)
            continue
        if body.get("status") == "1" and isinstance(body.get("result"), list):
            row = body["result"][0]
            break
        if attempt == 5:
            raise RuntimeError(f"Etherscan source unavailable: {cid} {address}")
        time.sleep(2 ** attempt)
    source = row.get("SourceCode", "")
    if not source:
        directory.mkdir(parents=True, exist_ok=True)
        atomic(cache, row)
        atomic(directory / "_meta.json", {"address": address, "chain_id": cid,
            "contract_name": row.get("ContractName"), "source_verified": False,
            "source": "Etherscan v2 getsourcecode", "n_files": 0})
        raise RuntimeError(f"Unverified contract: {cid} {address}")
    directory.mkdir(parents=True, exist_ok=True)
    if source.startswith("{{"):
        source = source[1:-1]
    if source.startswith("{"):
        parsed = json.loads(source)
        sources = parsed.get("sources", parsed)
    else:
        sources = {row["ContractName"] + ".sol": {"content": source}}
    for name, content in sources.items():
        flat = name.replace("/", "__").replace("\\", "__")
        path = directory / flat
        text = content["content"] if isinstance(content, dict) else content
        if not path.exists():
            io_utils._atomic_text(path, text)
    atomic(cache, row)
    atomic(directory / "_meta.json", {
        "address": address, "chain_id": cid, "contract_name": row["ContractName"],
        "compiler": row["CompilerVersion"], "n_files": len(sources),
        "source": "Etherscan v2 getsourcecode", "fetched_utc": datetime.now(timezone.utc).isoformat()})
    time.sleep(0.25)
    return row


def discover(cid, out, registry, target):
    path = out / "implementations.json"
    state = json.loads(path.read_text()) if path.exists() else {"vaults": {}, "sources": {}}
    todo = [r["vault"] for r in registry if r["vault"] not in state["vaults"]]
    def code_at(address):
        return address, rpc.get_code(target, address)
    with ThreadPoolExecutor(max_workers=4) as pool:
        for address, code in pool.map(code_at, todo):
            match = re.search(r"363d3d373d3d3d363d73([0-9a-fA-F]{40})5af43d82803e903d91602b57fd5bf3", code)
            implementation = "0x" + match[1].lower() if match else address
            state["vaults"][address] = {"implementation": implementation,
                "proxy_kind": "EIP1167" if match else "direct_or_other",
                "runtime_code": code,
                "runtime_sha256": hashlib.sha256(bytes.fromhex(code[2:])).hexdigest()}
            atomic(path, state)
    for implementation in sorted({v["implementation"] for v in state["vaults"].values()}):
        if implementation in state["sources"] or implementation in state.get("unverified", []):
            continue
        print(f"[{cid}] fetching verified implementation {implementation}", flush=True)
        try:
            row = explorer_source(cid, implementation)
        except RuntimeError as exc:
            if not str(exc).startswith("Unverified contract:"):
                raise
            state.setdefault("unverified", []).append(implementation)
            atomic(path, state)
            print(f"[{cid}] unverified {implementation}; retaining explicit source gap", flush=True)
            continue
        state["sources"][implementation] = {"contract_name": row["ContractName"],
            "abi": json.loads(row["ABI"]), "directory": f"data/contracts/{cid}-{implementation}"}
        atomic(path, state)
    libraries = set()
    for source in state["sources"].values():
        row = json.loads((HERE.parents[1] / source["directory"] / "_explorer.json").read_text())
        text = row["SourceCode"]
        if text.startswith("{{"):
            text = text[1:-1]
        if text.startswith("{"):
            for linked in json.loads(text).get("settings", {}).get("libraries", {}).values():
                libraries.update(a.lower() for a in linked.values())
    state.setdefault("libraries", {})
    for address in sorted(libraries):
        if address in state["libraries"] or address in state.get("unverified_libraries", []):
            continue
        print(f"[{cid}] fetching linked library {address}", flush=True)
        try:
            row = explorer_source(cid, address)
        except RuntimeError as exc:
            if not str(exc).startswith("Unverified contract:"):
                raise
            state.setdefault("unverified_libraries", []).append(address)
            atomic(path, state)
            continue
        state["libraries"][address] = {"contract_name": row["ContractName"],
            "abi": json.loads(row["ABI"]), "directory": f"data/contracts/{cid}-{address}"}
        atomic(path, state)
    print(f"[{cid}] {len(registry)} vaults, {len(state['sources'])} verified implementations", flush=True)
    return state


def abi_type(item):
    kind = item["type"]
    if kind.startswith("tuple"):
        return "(" + ",".join(abi_type(x) for x in item["components"]) + ")" + kind[5:]
    return kind


def resolve_claiming_clones(cid, out, target):
    """Resolve unverified EIP-1167 claiming proxies to verified implementations."""
    checkpoint = out / "_vaults_checkpoint.json"
    if not checkpoint.exists():
        return
    state = json.loads(checkpoint.read_text())
    if state["next_block"] != state["head_exclusive"]:
        return
    checked = state.setdefault("claiming_source_checks", {})
    path = out / "supplemental_sources.json"
    sources = json.loads(path.read_text()) if path.exists() else {}
    for address in list(state.get("unverified_claiming_logics", [])):
        if address in checked:
            continue
        code = rpc.get_code(target, address, state["head_exclusive"] - 1)
        match = re.search(r"363d3d373d3d3d363d73([0-9a-fA-F]{40})5af43d82803e903d91602b57fd5bf3", code)
        checked[address] = {"runtime_sha256": hashlib.sha256(bytes.fromhex(code[2:])).hexdigest(),
                            "eip1167_implementation": "0x" + match[1].lower() if match else None}
        if match:
            implementation = "0x" + match[1].lower()
            try:
                source = explorer_source(cid, implementation)
            except RuntimeError as exc:
                if not str(exc).startswith("Unverified contract:"):
                    raise
            else:
                sources[address] = {"contract_name": source["ContractName"], "implementation": implementation,
                    "abi": json.loads(source["ABI"]), "directory": f"data/contracts/{cid}-{implementation}"}
                state["unverified_claiming_logics"].remove(address)
                atomic(path, sources)
        atomic(checkpoint, state)


def event_catalog(sources):
    """Include library events omitted from the main contract's explorer ABI."""
    catalog = {}
    def add(event, origin):
        sig = event["name"] + "(" + ",".join(abi_type(x) for x in event["inputs"]) + ")"
        topic = evmabi.topic0(sig)
        if topic not in catalog:
            catalog[topic] = {**event, "signature": sig, "source": origin}
    for source in sources.values():
        for event in source["abi"]:
            if event["type"] == "event":
                add(event, source["directory"] + "/_explorer.json")
        directory = HERE.parents[1] / source["directory"]
        texts = {p: re.sub(r"/\*.*?\*/|//[^\n]*", "", p.read_text(), flags=re.DOTALL)
                 for p in directory.glob("*.sol")}
        structs = {}
        for text in texts.values():
            for name, body in re.findall(r"struct\s+(\w+)\s*\{(.*?)\}", text, re.DOTALL):
                structs[name] = body
        def argument(declaration, structs=structs):
            parts = declaration.split()
            kind, name = parts[0], parts[-1]
            indexed = "indexed" in parts
            suffix = kind[kind.index("["):] if "[" in kind else ""
            base = kind.split("[")[0].split(".")[-1]
            item = {"name": name, "indexed": indexed}
            if base in structs:
                item.update(type="tuple" + suffix, components=[argument(x.strip())
                    for x in structs[base].split(";") if x.strip()])
            elif re.fullmatch(r"u?int\d*|bytes\d*|address|bool|string", base):
                item["type"] = ({"uint": "uint256", "int": "int256"}.get(base, base)) + suffix
            else:
                if not (base.startswith("I") or base == "SiloVault"):
                    raise ValueError(f"Unresolved Solidity event type {base}")
                item["type"] = "address" + suffix
            return item
        for path, text in texts.items():
            if not (path.name.endswith("__EventsLib.sol") or path.name.endswith("__IVaultIncentivesModule.sol")):
                continue
            for name, args in re.findall(r"event\s+(\w+)\s*\((.*?)\)\s*;", text, re.DOTALL):
                add({"type": "event", "name": name, "anonymous": False,
                    "inputs": [argument(a.strip()) for a in args.split(",") if a.strip()]},
                    str(path.relative_to(HERE.parents[1])))
    return catalog


def json_value(value):
    if isinstance(value, bytes):
        return "0x" + value.hex()
    if isinstance(value, (list, tuple)):
        return [json_value(x) for x in value]
    return value.lower() if isinstance(value, str) and value.startswith("0x") else value


def decode_event(event, log):
    fields = event["inputs"]
    plain = [x for x in fields if not x["indexed"]]
    decoded = iter(evmabi.decode([abi_type(x) for x in plain], log["data"]))
    topics = iter(log["topics"][1:])
    result = {}
    for item in fields:
        if item["indexed"]:
            word = next(topics)
            kind = abi_type(item)
            value = word if kind in ("string", "bytes") or "[" in kind or "(" in kind else evmabi.decode([kind], word)[0]
        else:
            value = next(decoded)
        result[item["name"]] = json_value(value)
    return result


def base_row(log):
    return {"block": log["block_number"], "log_index": log["log_index"],
            "timestamp": log["timestamp"], "tx_hash": log["tx_hash"]}


def initial_vault():
    return {"roles": {"owner": ZERO, "pending_owner": ZERO, "curator": ZERO, "guardian": ZERO},
            "allocators": {}, "markets": [], "idle_vaults": [], "caps": {},
            "supply_queue": [], "withdraw_queue": [], "events": {}, "first_block": None,
            "last_block": None, "flow_shares": 0, "fee_shares": 0, "transfer_supply": 0,
            "timelock": 0, "fee_recipient": ZERO}


POSITION_COLUMNS = ["block", "log_index", "timestamp", "tx_hash", "market", "kind", "assets",
                    "shares_delta", "cum_shares", "event_shares", "sender", "owner", "receiver", "from", "to"]
FLOW_COLUMNS = ["block", "log_index", "timestamp", "tx_hash", "kind", "sender", "owner", "receiver", "assets", "shares"]
FEE_COLUMNS = ["vault", "block", "log_index", "timestamp", "tx_hash", "kind", "newTotalAssets", "feeShares", "fee_recipient"]
VALIDATION_WHAT = ("State checks at the pinned head and at archive blocks: (a) balanceOf for every vault-market pair "
    "at the head, (b) balanceOf at 200 seeded random (vault, market, block) points, (c) owner, curator, guardian, "
    "pendingOwner and isAllocator at the head, (d) caps and both queues of 30 random vaults at the head, (e) vault "
    "share flows plus fee shares against totalSupply at the head; plus a replay of the position CSVs")
FILE_NOTES = [
    ("positions/*.csv", "The vault's holdings in each market share token (silo, idle vault or another Silo vault): "
     "one row per Deposit, Withdraw or Transfer log of that token naming the vault. Only Transfer changes "
     "shares_delta and cum_shares; Deposit and Withdraw keep assets and event_shares with shares_delta 0",
     "HyperSync logs of the share tokens in scan.json position_markets"),
    ("flows/*.csv", "Depositor-side Deposit and Withdraw logs of the vault; kind gives the sign", "HyperSync logs of the vault"),
    ("roles.jsonl", "Owner, pending owner, curator, guardian and allocator additions and removals in log order",
     "Vault OwnershipTransferred, OwnershipTransferStarted, SetCurator, SetGuardian and SetIsAllocator logs from HyperSync"),
    ("actions.jsonl", "Every decoded log of the vault and its incentives module except the vault's Deposit, Withdraw, "
     "Transfer and AccrueInterest: roles, caps, queues, timelocks, fees, reallocations, approvals, incentives",
     "HyperSync logs decoded with event_catalog.json"),
    ("fees.csv", "AccrueInterest logs: new total assets, fee shares minted and the fee recipient at that log",
     "HyperSync logs of the vault"),
    ("unknown_events.jsonl", "Raw vault or module logs that had no decoder when first read; rows decoded later are "
     "listed in resolved_unknown_ids of _vaults_checkpoint.json", "HyperSync logs of the vault and module"),
    ("vaults.jsonl", "One row per registry vault: implementation, event counts, markets, role holders, final shares "
     "per market, flow and fee share totals, queues and caps at the scan head",
     "data/registry/vaults/silo_v2 joined with the replay state of both event streams"),
    ("implementations.json", "Runtime code and hash per vault, its implementation, verified ABIs of vault "
     "implementations and linked libraries, and unverified addresses",
     "dRPC eth_getCode; Etherscan v2 getsourcecode, files under data/contracts/<chain>-<address>/"),
    ("supplemental_sources.json", "Verified ABIs of incentives-module and claiming-logic contracts used for decoding",
     "Etherscan v2 getsourcecode"),
    ("event_catalog.json", "topic0 to event name, layout, signature and the verified file declaring it",
     "Verified explorer ABIs plus EventsLib and IVaultIncentivesModule source declarations"),
    ("scan.json", "Pinned scan head, first scanned block, incentives module per vault and the market universe of the "
     "position scan", "HyperSync height; dRPC Multicall3 INCENTIVES_MODULE() at the head; registry markets, vault "
     "actions and CreateIdleVault logs"),
    ("estimates.json", "Pre-scan event counts extrapolated from three 0.1% block slices", "HyperSync"),
    ("statistics.json", "Measured row counts, file sizes, cumulative dRPC requests and runtime", "This directory and runs.json"),
    ("runs.json", "One row per script run: stage, start time, runtime and dRPC requests", "Script run records"),
    ("validation.json", VALIDATION_WHAT, "dRPC archive eth_call through Multicall3, compared with this directory's files"),
    ("_checkpoint.json", "Block below which both event streams are complete, and the byte length of every output file",
     "Resume state of this script"),
    ("_vaults_checkpoint.json", "Resume state of the vault-event stream with per-vault replay state", "Resume state of this script"),
    ("_positions_checkpoint.json", "Resume state of the position stream with per-vault share balances", "Resume state of this script"),
    ("_idle_checkpoint.json", "Resume state of the CreateIdleVault scan with the idle vaults of each vault",
     "Resume state of this script"),
]


def global_checkpoint(out):
    """Record the common safe block and file lengths across the two event streams."""
    manifest_path = out / "scan.json"
    if not manifest_path.exists():
        return
    manifest = json.loads(manifest_path.read_text())
    safe, sizes, stages = [], {}, {}
    for stage in ["vaults", "positions"]:
        path = out / f"_{stage}_checkpoint.json"
        state = json.loads(path.read_text()) if path.exists() else {"next_block": manifest["lo"], "sizes": {}}
        safe.append(state["next_block"])
        sizes.update(state["sizes"])
        stages[stage] = {"next_block": state["next_block"], "state_file": path.name}
    atomic(out / "_checkpoint.json", {"complete_below_block": min(safe),
        "head_exclusive": manifest["head_exclusive"], "sizes": sizes, "stages": stages})


class PageWriter:
    """Append and fsync outputs before atomically advancing the safe block."""
    def __init__(self, out, path, state):
        self.out, self.path, self.state = out, path, state
        self.buffers = defaultdict(list)

    def emit(self, filename, row, columns=None):
        self.buffers[filename].append((row, columns))

    def checkpoint(self, next_block):
        sizes = self.state.setdefault("sizes", {})
        for name, entries in self.buffers.items():
            path = self.out / name
            path.parent.mkdir(parents=True, exist_ok=True)
            buf = io.StringIO()
            columns = entries[0][1]
            if columns:
                writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
                if sizes.get(name, 0) == 0:
                    writer.writeheader()
                writer.writerows(row for row, _ in entries)
            else:
                buf.write("".join(json.dumps(row, sort_keys=True) + "\n" for row, _ in entries))
            with path.open("ab") as handle:
                handle.write(buf.getvalue().encode())
                handle.flush()
                os.fsync(handle.fileno())
            sizes[name] = path.stat().st_size
        self.buffers.clear()
        self.state["next_block"] = next_block
        atomic(self.path, self.state)
        global_checkpoint(self.out)


def truncate_outputs(out, state, stage):
    if stage == "idle":
        return
    paths = (list((out / "positions").glob("*.csv")) if stage == "positions" else
             list((out / "flows").glob("*.csv")) + [out / x for x in
                 ["roles.jsonl", "actions.jsonl", "fees.csv", "unknown_events.jsonl"]])
    for path in paths:
        if path.exists():
            size = state.get("sizes", {}).get(str(path.relative_to(out)), 0)
            if path.stat().st_size < size:
                raise RuntimeError(f"Checkpoint output shorter than recorded: {path}")
            if path.stat().st_size != size:
                with path.open("r+b") as handle:
                    handle.truncate(size)


def stream(cid, cfg, out, stage, state, addresses, topics, handler):
    """Keep a page's final block pending because the wrapper lacks next_block."""
    path = out / f"_{stage}_checkpoint.json"
    for attempt in range(8):
        if path.exists():
            state = json.loads(path.read_text())
        truncate_outputs(out, state, stage)
        if state["next_block"] >= state["head_exclusive"]:
            return state
        writer = PageWriter(out, path, state)
        pending = []
        def page(logs_page, state=state, writer=writer):
            nonlocal pending
            combined = pending + logs_page
            combined.sort(key=lambda x: (x["block_number"], x["log_index"]))
            end = combined[-1]["block_number"]
            complete = [x for x in combined if x["block_number"] < end]
            pending = [x for x in combined if x["block_number"] == end]
            handler(complete, writer)
            state["pages"] = state.get("pages", 0) + 1
            writer.checkpoint(end)
            print(f"[{cid}] {stage} page {state['pages']}: complete below {end}; "
                  f"{state.get('events', 0):,} retained events", flush=True)
        try:
            hypersync_client.scan_logs_stream(cfg["hypersync"], addresses=addresses,
                topics=topics, from_block=state["next_block"], to_block=state["head_exclusive"], on_page=page)
            handler(pending, writer)
            writer.checkpoint(state["head_exclusive"])
            return state
        except Exception as exc:
            if attempt == 7 or not any(s in str(exc).lower() for s in ["429", "too many", "rate limit"]):
                raise
            pause = 15 * 2 ** attempt
            print(f"[{cid}] HyperSync throttled; sleeping {pause}s before resuming checkpoint", flush=True)
            time.sleep(pause)
    raise RuntimeError("HyperSync retry budget exhausted")


def estimate(cid, cfg, out, lo, head, addresses, topics, stage):
    path = out / "estimates.json"
    estimates = json.loads(path.read_text()) if path.exists() else {}
    if stage not in estimates:
        width = max(1, (head - lo) // 1000)
        count = 0
        for i in range(3):
            start = lo + (head - lo - width) * i // 2
            for attempt in range(8):
                try:
                    rows, _ = hypersync_client.scan_logs(cfg["hypersync"], addresses=addresses,
                        topics=topics, from_block=start, to_block=start + width)
                    count += len(rows)
                    break
                except Exception as exc:
                    if attempt == 7 or not any(s in str(exc).lower() for s in ["429", "too many", "rate limit"]):
                        raise
                    pause = 15 * 2 ** attempt
                    print(f"[{cid}] estimate throttled; sleeping {pause}s", flush=True)
                    time.sleep(pause)
        estimate_count = round(count * (head - lo) / (width * 3))
        estimates[stage] = {"sample_events": count, "sample_blocks": width * 3,
            "estimated_input_events": estimate_count, "estimated_output_bytes_upper": estimate_count * 500,
            "method": "Three equally spaced 0.1% block slices; size assumes 500 bytes per input log before filtering"}
        atomic(path, estimates)
    item = estimates[stage]
    print(f"[{cid}] {stage} estimate: {item['estimated_input_events']:,} input events; "
          f"up to about {item['estimated_output_bytes_upper']:,} output bytes before participant filtering", flush=True)


def vault_handler(catalog, modules, factories):
    def handle(page, writer):
        state = writer.state
        for log in page:
            emitter = log["address"].lower()
            event = catalog.get(log["topics"][0])
            if emitter in factories:
                if not event or event["name"] != "CreateIdleVault":
                    continue
                fields = decode_event(event, log)
                vault = fields["vault"]
                if vault not in state["vaults"]:
                    continue
            else:
                vault = modules.get(emitter, emitter)
                if vault not in state["vaults"]:
                    continue
                if event is None:
                    writer.emit("unknown_events.jsonl", {"vault": vault, **log})
                    state["unknown_events"] = state.get("unknown_events", 0) + 1
                    continue
                fields = decode_event(event, log)
            name = event["name"]
            s = state["vaults"][vault]
            base = {"vault": vault, **base_row(log), "emitter": emitter, "event": name}
            state["events"] += 1
            s["events"][name] = s["events"].get(name, 0) + 1
            s["first_block"] = min(log["block_number"], s["first_block"] or log["block_number"])
            s["last_block"] = max(log["block_number"], s["last_block"] or 0)
            def role(role_name, address, s=s, base=base):
                old = s["roles"][role_name]
                if old != ZERO:
                    writer.emit("roles.jsonl", {**base, "role": role_name, "address": old, "action": "removed"})
                if address != ZERO:
                    writer.emit("roles.jsonl", {**base, "role": role_name, "address": address, "action": "added"})
                s["roles"][role_name] = address
            if emitter == vault:
                if name in ("Deposit", "Withdraw"):
                    sign = 1 if name == "Deposit" else -1
                    s["flow_shares"] += sign * fields["shares"]
                    writer.emit(f"flows/{vault}.csv", {**base, "kind": name, **fields}, FLOW_COLUMNS)
                    continue
                if name == "Transfer":
                    s["transfer_supply"] += fields["value"] * ((fields["from"] == ZERO) - (fields["to"] == ZERO))
                    continue
                if name == "AccrueInterest":
                    s["fee_shares"] += fields["feeShares"]
                    writer.emit("fees.csv", {**base, "kind": name, **fields,
                        "fee_recipient": s["fee_recipient"]}, FEE_COLUMNS)
                    continue
                if name == "OwnershipTransferred":
                    role("owner", fields["newOwner"])
                    role("pending_owner", ZERO)
                elif name == "OwnershipTransferStarted":
                    role("pending_owner", fields["newOwner"])
                elif name == "SetCurator":
                    role("curator", fields["newCurator"])
                elif name == "SetGuardian":
                    role("guardian", fields["guardian"])
                elif name == "SetIsAllocator":
                    address, active = fields["allocator"], fields["isAllocator"]
                    s["allocators"][address] = active
                    writer.emit("roles.jsonl", {**base, "role": "allocator", "address": address,
                        "action": "added" if active else "removed"})
                elif name == "SetCap":
                    s["caps"][fields["market"]] = fields["cap"]
                elif name == "SetSupplyQueue":
                    s["supply_queue"] = fields["newSupplyQueue"]
                elif name == "SetWithdrawQueue":
                    new = fields["newWithdrawQueue"]
                    for market in set(s["withdraw_queue"]) - set(new):
                        s["caps"][market] = 0
                    s["withdraw_queue"] = new
                elif name == "SetTimelock":
                    s["timelock"] = fields["newTimelock"]
                elif name == "SetFeeRecipient":
                    s["fee_recipient"] = fields["newFeeRecipient"]
            if name == "CreateIdleVault":
                s["idle_vaults"].append(fields["idleVault"])
            for key in ["market", "idleVault"]:
                if key in fields and fields[key] not in s["markets"]:
                    s["markets"].append(fields[key])
            for key in ["newSupplyQueue", "newWithdrawQueue"]:
                for market in fields.get(key, []):
                    if market not in s["markets"]:
                        s["markets"].append(market)
            writer.emit("actions.jsonl", {**base, "fields": fields})
    return handle


def position_handler(catalog, vaults):
    def handle(page, writer):
        state = writer.state
        for log in page:
            event = catalog[log["topics"][0]]
            fields = decode_event(event, log)
            name = event["name"]
            participants = {fields[k] for k in ["owner", "receiver", "from", "to"] if k in fields} & vaults
            for vault in sorted(participants):
                market = log["address"].lower()
                balances = state["balances"].setdefault(vault, {})
                delta = fields["value"] * ((fields["to"] == vault) - (fields["from"] == vault)) if name == "Transfer" else 0
                balances[market] = balances.get(market, 0) + delta
                if balances[market] < 0:
                    raise ValueError(f"Negative shares {vault} {market} at {log['block_number']}")
                row = {**base_row(log), **fields, "market": market, "kind": name,
                    "shares_delta": delta, "cum_shares": balances[market],
                    "event_shares": fields.get("shares", fields.get("value"))}
                writer.emit(f"positions/{vault}.csv", row, POSITION_COLUMNS)
                state["events"] += 1
                counts = state.setdefault("event_counts", {})
                counts[name] = counts.get(name, 0) + 1
    return handle


def scan_chain(cid, out, cfg, registry, implementations):
    manifest_path = out / "scan.json"
    vaults = sorted(r["vault"] for r in registry)
    target = logs.rpc_target(cfg)
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
    else:
        head = hypersync_client.get_height(cfg["hypersync"])
        results = multicall.try_view(target, vaults, "INCENTIVES_MODULE()", ["address"], block=head - 1)
        modules = {}
        for vault, value in zip(vaults, results):
            if value and value[0] != ZERO:
                modules[value[0].lower()] = vault
        manifest = {"head_exclusive": head, "head_block": head - 1,
                    "lo": min(r["created_block"] for r in registry), "modules": modules,
                    "module_getter_failures": [v for v, r in zip(vaults, results) if r is None]}
        atomic(manifest_path, manifest)
    supplemental_path = out / "supplemental_sources.json"
    supplemental = json.loads(supplemental_path.read_text()) if supplemental_path.exists() else {}
    if not supplemental and manifest["modules"]:
        attempted = set()
        for module in manifest["modules"]:
            code = rpc.get_code(target, module, manifest["head_block"])
            match = re.search(r"363d3d373d3d3d363d73([0-9a-fA-F]{40})5af43d82803e903d91602b57fd5bf3", code)
            implementation = "0x" + match[1].lower() if match else module
            if implementation in attempted:
                continue
            attempted.add(implementation)
            try:
                source = explorer_source(cid, implementation)
            except RuntimeError as exc:
                if not str(exc).startswith("Unverified contract:"):
                    raise
                print(f"[{cid}] incentive implementation {implementation} unverified; checking another deployment", flush=True)
                continue
            supplemental[implementation] = {"contract_name": source["ContractName"],
                "abi": json.loads(source["ABI"]), "directory": f"data/contracts/{cid}-{implementation}"}
            atomic(supplemental_path, supplemental)
            break
        if not supplemental:
            raise RuntimeError(f"No verified incentive module source found on chain {cid}")
    catalog = event_catalog({**implementations["sources"], **implementations.get("libraries", {}), **supplemental})
    atomic(out / "event_catalog.json", catalog)
    modules = manifest["modules"]
    idle_topics = [[t for t, e in catalog.items() if e["name"] == "CreateIdleVault"]]
    print(f"[{cid}] idle discovery estimate: about {len(vaults)} linked creation events, "
          f"about {len(vaults) * 500:,} bytes of retained metadata", flush=True)
    def idle_handler(page, writer):
        for log in page:
            fields = decode_event(catalog[log["topics"][0]], log)
            if fields["vault"] in vaults:
                writer.state["idle_vaults"].setdefault(fields["vault"], {})[fields["idleVault"]] = base_row(log)
                writer.state["events"] += 1
    idle_state = {**manifest, "next_block": manifest["lo"], "sizes": {}, "events": 0, "idle_vaults": {}}
    idle_state = stream(cid, cfg, out, "idle", idle_state, None, idle_topics, idle_handler)
    factories = {r["factory"] for r in registry}
    addresses = sorted(set(vaults) | set(modules) | factories)
    estimate(cid, cfg, out, manifest["lo"], manifest["head_exclusive"], addresses, None, "vaults")
    state = {**manifest, "next_block": manifest["lo"], "sizes": {}, "events": 0,
             "vaults": {v: initial_vault() for v in vaults}}
    vault_state = stream(cid, cfg, out, "vaults", state, addresses, None,
                         vault_handler(catalog, modules, factories))
    for vault, idles in idle_state["idle_vaults"].items():
        vault_state["vaults"][vault]["idle_vaults"] = sorted(idles)
    unknown_path = out / "unknown_events.jsonl"
    if unknown_path.exists():
        unknown_rows = read_rows(unknown_path)
        if any(r["topics"][0] not in catalog for r in unknown_rows):
            claiming_logics = set()
            with (out / "actions.jsonl").open() as handle:
                for line in handle:
                    row = json.loads(line)
                    if "logic" in row["fields"]:
                        claiming_logics.add(row["fields"]["logic"])
            for address in sorted(claiming_logics):
                if address in supplemental or address in vault_state.get("unverified_claiming_logics", []):
                    continue
                print(f"[{cid}] fetching delegatecalled claiming logic {address}", flush=True)
                try:
                    source = explorer_source(cid, address)
                except RuntimeError as exc:
                    if not str(exc).startswith("Unverified contract:"):
                        raise
                    vault_state.setdefault("unverified_claiming_logics", []).append(address)
                    continue
                supplemental[address] = {"contract_name": source["ContractName"],
                    "abi": json.loads(source["ABI"]), "directory": f"data/contracts/{cid}-{address}"}
                atomic(supplemental_path, supplemental)
            catalog = event_catalog({**implementations["sources"], **implementations.get("libraries", {}), **supplemental})
            atomic(out / "event_catalog.json", catalog)
        resolved = set(vault_state.get("resolved_unknown_ids", []))
        known = [r for r in unknown_rows if r["topics"][0] in catalog
                 and f"{r['block_number']}:{r['log_index']}" not in resolved]
        if known:
            writer = PageWriter(out, out / "_vaults_checkpoint.json", vault_state)
            vault_handler(catalog, modules, factories)(known, writer)
            resolved.update(f"{r['block_number']}:{r['log_index']}" for r in known)
            vault_state["resolved_unknown_ids"] = sorted(resolved)
            vault_state["unknown_events"] -= len(known)
            writer.checkpoint(vault_state["head_exclusive"])
            action_rows = read_rows(out / "actions.jsonl")
            action_rows.sort(key=lambda r: (r["block"], r["log_index"]))
            io_utils._atomic_text(out / "actions.jsonl", "".join(json.dumps(r, sort_keys=True) + "\n" for r in action_rows))
    markets = {r["silo"] for r in io_utils.read_jsonl(f"registry/markets/silo_v2/{cid}.jsonl")}
    markets.update(m for s in vault_state["vaults"].values() for m in s["markets"])
    markets.update(m for idles in idle_state["idle_vaults"].values() for m in idles)
    manifest["position_markets"] = sorted(markets)
    atomic(manifest_path, manifest)
    topics = [[t for t, e in catalog.items() if e["name"] in ("Deposit", "Withdraw", "Transfer")]]
    estimate(cid, cfg, out, manifest["lo"], manifest["head_exclusive"], sorted(markets), topics, "positions")
    state = {**manifest, "next_block": manifest["lo"], "sizes": {}, "events": 0, "balances": {},
             "scan_addresses": sorted(markets), "scanned_markets": sorted(markets)}
    position_checkpoint = out / "_positions_checkpoint.json"
    if position_checkpoint.exists():
        state = json.loads(position_checkpoint.read_text())
        previous = set(state.get("scanned_markets", state["position_markets"]))
        extra = markets - previous
        if extra and state["next_block"] == state["head_exclusive"]:
            state.update(next_block=manifest["lo"], scan_addresses=sorted(extra), scanned_markets=sorted(markets))
            atomic(position_checkpoint, state)
    positions = stream(cid, cfg, out, "positions", state, state.get("scan_addresses", sorted(markets)), topics,
                       position_handler(catalog, set(vaults)))
    if set(positions.get("scan_addresses", markets)) != markets:
        for p in (out / "positions").glob("*.csv"):
            with p.open() as handle:
                reader = csv.DictReader(handle)
                columns, rows = reader.fieldnames, list(reader)
            rows.sort(key=lambda r: (int(r["block"]), int(r["log_index"])))
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=columns, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
            io_utils._atomic_text(p, buf.getvalue())
    finish(cid, out, registry, implementations, vault_state, positions)


def write_sidecars(cid, out):
    """Give every data file of a chain a sidecar."""
    manifest = json.loads((out / "scan.json").read_text())
    checkpoint_path = out / "_checkpoint.json"
    common = {"chain_id": cid, "scanned_from_block": manifest["lo"], "head_block": manifest["head_block"],
              "complete_below_block": json.loads(checkpoint_path.read_text())["complete_below_block"]
              if checkpoint_path.exists() else None}
    covered = set()
    for pattern, what, source in FILE_NOTES:
        for path in sorted(out.glob(pattern)):
            covered.add(path)
            stat = path.stat()
            meta = {**common, "what": what, "source": source, "bytes": stat.st_size,
                    "built_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec="seconds")}
            if path.suffix in (".csv", ".jsonl"):
                with path.open("rb") as handle:
                    meta["rows"] = sum(1 for _ in handle) - (path.suffix == ".csv")
            io_utils._meta_sidecar(path, GEN, meta)
    uncovered = [p for p in out.rglob("*") if p.is_file() and p not in covered
                 and not p.name.endswith((".meta.json", ".tmp"))]
    if uncovered:
        print(f"[{cid}] no sidecar note for {[str(p.relative_to(out)) for p in uncovered]}", flush=True)


def finish(cid, out, registry, implementations, vault_state, positions):
    rows = []
    for r in registry:
        v = r["vault"]
        s = vault_state["vaults"][v]
        markets = positions["balances"].get(v, {})
        first_position, last_position, position_count = None, None, 0
        held_markets = set()
        position_path = out / f"positions/{v}.csv"
        if position_path.exists():
            with position_path.open() as handle:
                for position in csv.DictReader(handle):
                    block = int(position["block"])
                    first_position = min(first_position or block, block)
                    last_position = max(last_position or block, block)
                    position_count += 1
                    if int(position["cum_shares"]) > 0:
                        held_markets.add(position["market"])
        blocks = [b for b in [s["first_block"], s["last_block"], first_position, last_position] if b is not None]
        rows.append({"vault": v, "chain_id": cid, "name": r["name"],
            "implementation": implementations["vaults"][v]["implementation"],
            "source_verified": implementations["vaults"][v]["implementation"] in implementations["sources"], "events": s["events"],
            "markets_touched": sorted(markets), "action_markets": sorted(s["markets"]),
            "idle_vaults": sorted(s["idle_vaults"]), "first_block": min(blocks) if blocks else None,
            "last_block": max(blocks) if blocks else None, "position_events": position_count,
            "first_position_block": first_position, "last_position_block": last_position,
            "markets_ever_held": sorted(held_markets),
            "roles_at_end": {**s["roles"], "allocators": sorted(a for a, yes in s["allocators"].items() if yes)},
            "final_shares": markets, "flow_shares": s["flow_shares"], "fee_shares": s["fee_shares"],
            "supply_queue": s["supply_queue"], "withdraw_queue": s["withdraw_queue"], "caps": s["caps"]})
        for stage, columns, checkpoint in [("positions", POSITION_COLUMNS, positions), ("flows", FLOW_COLUMNS, vault_state)]:
            name = f"{stage}/{v}.csv"
            p = out / name
            if not p.exists() or p.stat().st_size == 0:
                p.parent.mkdir(parents=True, exist_ok=True)
                io_utils._atomic_text(p, ",".join(columns) + "\n")
                checkpoint["sizes"][name] = p.stat().st_size
    for name in ["roles.jsonl", "actions.jsonl", "fees.csv"]:
        if not (out / name).exists():
            io_utils._atomic_text(out / name, ",".join(FEE_COLUMNS) + "\n" if name == "fees.csv" else "")
            vault_state["sizes"][name] = (out / name).stat().st_size
    atomic(out / "_vaults_checkpoint.json", vault_state)
    atomic(out / "_positions_checkpoint.json", positions)
    global_checkpoint(out)
    io_utils._atomic_text(out / "vaults.jsonl", "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows))
    def data_rows(path, csv_file=False):
        if not path.exists():
            return 0
        with path.open() as handle:
            return max(0, sum(1 for _ in handle) - int(csv_file))
    atomic(out / "statistics.json", {"vaults": len(rows), "vaults_with_positions": len(positions["balances"]),
        "vaults_with_positive_holdings_ever": sum(bool(r["markets_ever_held"]) for r in rows),
        "position_events": positions["events"], "position_event_counts": positions.get("event_counts", {}),
        "flow_events": sum(data_rows(p, True) for p in (out / "flows").glob("*.csv")),
        "fee_events": data_rows(out / "fees.csv", True), "role_rows": data_rows(out / "roles.jsonl"),
        "action_events": data_rows(out / "actions.jsonl"),
        "vault_and_module_events": vault_state["events"], "unknown_events": vault_state.get("unknown_events", 0),
        "markets_touched": len({m for b in positions["balances"].values() for m in b}),
        "vault_market_pairs": sum(len(b) for b in positions["balances"].values()),
        "file_sizes": {str(p.relative_to(out)): p.stat().st_size for p in out.rglob("*") if p.is_file() and p.name != "statistics.json"}})


def checked_reads(target, calls, block, types):
    result = []
    for (ok, raw), ret_types in zip(multicall.aggregate3(target, calls, block=block), types):
        try:
            result.append(json_value(evmabi.decode(ret_types, raw)) if ok and raw != "0x" else None)
        except (DecodingError, ValueError):
            result.append(None)
    return result


def comparison(expected, actual):
    return "unreadable" if actual is None else "exact" if expected == actual else "mismatch"


def totals(rows):
    counts = Counter(r["status"] for r in rows)
    return {"checked": len(rows), "exact": counts["exact"], "mismatch": counts["mismatch"],
            "unreadable": counts["unreadable"], "rows": rows}


def archive_sample(out, count, seed):
    """Reservoir sample distinct post-block trajectory points, never mid-block rows."""
    rng, reservoir, seen = random.Random(seed), [], 0
    def consider(vault, market, block, shares):
        nonlocal seen
        seen += 1
        row = {"vault": vault, "market": market, "block": block, "expected": shares}
        if len(reservoir) < count:
            reservoir.append(row)
        else:
            index = rng.randrange(seen)
            if index < count:
                reservoir[index] = row
    for path in sorted((out / "positions").glob("*.csv")):
        block, last = None, {}
        with path.open() as handle:
            for row in csv.DictReader(handle):
                b = int(row["block"])
                if block is not None and b != block:
                    for market, shares in last.items():
                        consider(path.stem, market, block, shares)
                    last = {}
                block = b
                last[row["market"]] = int(row["cum_shares"])
        if block is not None:
            for market, shares in last.items():
                consider(path.stem, market, block, shares)
    return reservoir, seen


def validate(cid, out, cfg, registry):
    manifest = json.loads((out / "scan.json").read_text())
    head, target = manifest["head_block"], logs.rpc_target(cfg)
    vs = json.loads((out / "_vaults_checkpoint.json").read_text())
    ps = json.loads((out / "_positions_checkpoint.json").read_text())
    if min(vs["next_block"], ps["next_block"]) < manifest["head_exclusive"]:
        raise RuntimeError("Both event streams must reach the pinned head before validation")
    path = out / "validation.json"
    validation = json.loads(path.read_text()) if path.exists() else {
        "chain_id": cid, "head_block": head, "random_seed": 20261005 + cid,
        "share_accounting": "Market holdings change only by the share token's Transfer logs. Vault shares change "
            "only by mint and burn: Deposit and Withdraw logs of the vault carry depositor shares, and "
            "SiloVault._accrueFee mints AccrueInterest.feeShares to the fee recipient by _mint, which emits Transfer "
            "from zero but no Deposit. Expected totalSupply = sum Deposit.shares - sum Withdraw.shares + sum "
            "fees.csv feeShares; transfer_ledger_exact repeats the check with the vault's Transfer mints minus burns"}
    if validation["head_block"] != head:
        raise RuntimeError("Validation head differs from scan head")
    vaults = sorted(vs["vaults"])
    markets = manifest["position_markets"]
    if "head_positions" in validation:
        recorded = {(r["vault"], r["market"]) for r in validation["head_positions"]["rows"]}
        if recorded != {(v, m) for v in vaults for m in markets}:
            for section in ["head_positions", "archive_positions", "caps_and_queues"]:
                validation.pop(section, None)
            validation["market_universe_refreshed"] = True
    final_balances, position_rows, integrity_errors = {}, 0, []
    for p in sorted((out / "positions").glob("*.csv")):
        balances, last_key = {}, None
        with p.open() as handle:
            for row in csv.DictReader(handle):
                position_rows += 1
                key = (int(row["block"]), int(row["log_index"]))
                market = row["market"]
                balances[market] = balances.get(market, 0) + int(row["shares_delta"])
                if (last_key is not None and key <= last_key) or balances[market] != int(row["cum_shares"]) or not row["timestamp"]:
                    integrity_errors.append({"vault": p.stem, "block": row["block"], "log_index": row["log_index"]})
                last_key = key
        if balances:
            final_balances[p.stem] = balances
    if final_balances != ps["balances"]:
        integrity_errors.append({"error": "CSV final balances differ from checkpoint"})
    validation["output_integrity"] = {"position_rows": position_rows, "errors": integrity_errors}
    if "head_block_hash" not in validation:
        block = rpc.get_block(target, head)
        validation["head_block_hash"] = block["hash"]
        validation["head_timestamp"] = int(block["timestamp"], 16)
    atomic(path, validation)
    if integrity_errors:
        raise RuntimeError(f"{len(integrity_errors)} output integrity failures")
    if "head_positions" not in validation:
        pairs = [(v, m) for v in vaults for m in markets]
        calls = [(m, evmabi.encode_call("balanceOf(address)", ["address"], [v])) for v, m in pairs]
        result = checked_reads(target, calls, head, [["uint256"]] * len(calls))
        unread = sorted({m for (_, m), actual in zip(pairs, result) if actual is None})
        codeless = {m for m in unread if rpc.get_code(target, m, head) in ("0x", "")}
        rows = []
        for (v, m), actual in zip(pairs, result):
            expected = final_balances.get(v, {}).get(m, 0)
            value = actual[0] if actual else None
            status = "no_code" if m in codeless and expected == 0 else comparison(expected, value)
            rows.append({"vault": v, "market": m, "expected": expected, "actual": value,
                         "status": status, "touched": m in ps["balances"].get(v, {})})
        validation["head_positions"] = {**totals(rows), "no_code": sum(r["status"] == "no_code" for r in rows),
            "no_code_markets": sorted(codeless),
            "no_code_rule": "A market without code at the head can hold no shares; expected 0, counted as no_code"}
        atomic(path, validation)
        print(f"[{cid}] head positions: {validation['head_positions']['exact']}/{len(rows)} exact, "
              f"{validation['head_positions']['no_code']} without code", flush=True)
    if "archive_positions" not in validation:
        samples, population = archive_sample(out, 200, validation["random_seed"])
        validation["archive_positions"] = {"seed": validation["random_seed"], "population": population,
            "sampling": "Uniform reservoir over distinct (vault, market, event block), compared after all logs in that block", "rows": samples}
        atomic(path, validation)
    archive = validation["archive_positions"]
    pending = [row for row in archive["rows"] if "status" not in row]
    def read_sample(row):
        call = (row["market"], evmabi.encode_call("balanceOf(address)", ["address"], [row["vault"]]))
        actual = checked_reads(target, [call], row["block"], [["uint256"]])[0]
        value = actual[0] if actual else None
        return {**row, "actual": value, "status": comparison(row["expected"], value)}
    with ThreadPoolExecutor(max_workers=4) as pool:
        for original, checked in zip(pending, pool.map(read_sample, pending)):
            original.update(checked)
            atomic(path, validation)
    archive.update(totals(archive["rows"]))
    atomic(path, validation)
    print(f"[{cid}] archive positions: {archive['exact']}/{archive['checked']} exact", flush=True)
    if "roles" not in validation:
        roles = {v: {k: ZERO for k in ["owner", "curator", "guardian", "pending_owner"]} for v in vaults}
        allocators = {v: {} for v in vaults}
        candidates = {v: {ZERO} for v in vaults}
        for row in read_rows(out / "roles.jsonl"):
            v, role, address = row["vault"], row["role"], row["address"]
            candidates[v].add(address)
            if role == "allocator":
                allocators[v][address] = row["action"] == "added"
            elif row["action"] == "added":
                roles[v][role] = address
            elif roles[v][role] == address:
                roles[v][role] = ZERO
        checks, calls, types = [], [], []
        for v in vaults:
            for role, func in [("owner", "owner()"), ("curator", "curator()"),
                               ("guardian", "guardian()"), ("pending_owner", "pendingOwner()")]:
                checks.append({"vault": v, "role": role, "expected": roles[v][role]})
                calls.append((v, evmabi.selector(func)))
                types.append(["address"])
            for address in sorted(candidates[v]):
                checks.append({"vault": v, "role": "allocator", "address": address,
                               "expected": allocators[v].get(address, False)})
                calls.append((v, evmabi.encode_call("isAllocator(address)", ["address"], [address])))
                types.append(["bool"])
        for row, actual in zip(checks, checked_reads(target, calls, head, types)):
            row["actual"] = actual[0] if actual else None
            row["status"] = comparison(row["expected"], row["actual"])
        validation["roles"] = {**totals(checks), "vaults": len(vaults),
            "allocator_scope": "Every address ever named by this vault's role events, plus zero as a negative control; mappings cannot be enumerated via getters"}
        atomic(path, validation)
    if "caps_and_queues" not in validation:
        selected = sorted(random.Random(validation["random_seed"]).sample(vaults, min(30, len(vaults))))
        caps, supply, withdraw = {v: {} for v in selected}, {v: [] for v in selected}, {v: [] for v in selected}
        for row in read_rows(out / "actions.jsonl"):
            v, name, f = row["vault"], row["event"], row["fields"]
            if v not in caps or row["emitter"] != v:
                continue
            if name == "SetCap":
                caps[v][f["market"]] = f["cap"]
            elif name == "SetSupplyQueue":
                supply[v] = f["newSupplyQueue"]
            elif name == "SetWithdrawQueue":
                for market in set(withdraw[v]) - set(f["newWithdrawQueue"]):
                    caps[v][market] = 0
                withdraw[v] = f["newWithdrawQueue"]
        pairs = [(v, m) for v in selected for m in markets]
        calls = [(v, evmabi.encode_call("config(address)", ["address"], [m])) for v, m in pairs]
        results = checked_reads(target, calls, head, [["uint184", "bool", "uint64"]] * len(calls))
        cap_rows = []
        for (v, m), result in zip(pairs, results):
            expected, actual = caps[v].get(m, 0), result[0] if result else None
            cap_rows.append({"vault": v, "market": m, "expected": expected, "actual": actual,
                             "config": result, "status": comparison(expected, actual)})
        queue_rows = []
        for queue_name, replay in [("supplyQueue", supply), ("withdrawQueue", withdraw)]:
            lengths = multicall.try_view(target, selected, queue_name + "Length()", ["uint256"], block=head)
            indices = [(v, i) for v, length in zip(selected, lengths) for i in range(length[0] if length else 0)]
            calls = [(v, evmabi.encode_call(queue_name + "(uint256)", ["uint256"], [i])) for v, i in indices]
            results = checked_reads(target, calls, head, [["address"]] * len(calls))
            actual_queues = {v: [] if length else None for v, length in zip(selected, lengths)}
            for (v, _), result in zip(indices, results):
                if actual_queues[v] is not None:
                    if result is None:
                        actual_queues[v] = None
                    else:
                        actual_queues[v].append(result[0])
            for v in selected:
                queue_rows.append({"vault": v, "queue": queue_name, "expected": replay[v],
                    "actual": actual_queues[v], "status": comparison(replay[v], actual_queues[v])})
        validation["caps_and_queues"] = {"sampled_vaults": selected, "vault_count": len(selected),
            "note": "All vaults sampled when chain has fewer than 30", "caps": totals(cap_rows), "queues": totals(queue_rows)}
        atomic(path, validation)
    if "flows" not in validation:
        flow_shares, fee_shares = Counter(), Counter()
        for p in (out / "flows").glob("*.csv"):
            with p.open() as handle:
                for row in csv.DictReader(handle):
                    flow_shares[p.stem] += int(row["shares"]) * (1 if row["kind"] == "Deposit" else -1)
        with (out / "fees.csv").open() as handle:
            for row in csv.DictReader(handle):
                fee_shares[row["vault"]] += int(row["feeShares"])
        results = multicall.try_view(target, vaults, "totalSupply()", ["uint256"], block=head)
        rows = []
        for v, actual in zip(vaults, results):
            expected = flow_shares[v] + fee_shares[v]
            value = actual[0] if actual else None
            rows.append({"vault": v, "deposit_minus_withdraw_shares": flow_shares[v],
                "minted_fee_shares": fee_shares[v], "expected": expected, "actual": value,
                "transfer_mints_minus_burns": vs["vaults"][v]["transfer_supply"],
                "transfer_ledger_exact": vs["vaults"][v]["transfer_supply"] == value,
                "status": comparison(expected, value)})
        validation["flows"] = totals(rows)
        atomic(path, validation)
    def counts(section):
        return {k: section[k] for k in ["checked", "exact", "mismatch", "unreadable", "no_code"] if k in section}
    ledger = [r["transfer_ledger_exact"] for r in validation["flows"]["rows"]]
    validation["summary"] = {"a_head_balances": counts(validation["head_positions"]),
        "b_archive_balances": counts(archive), "c_roles": counts(validation["roles"]),
        "d_caps": counts(validation["caps_and_queues"]["caps"]), "d_queues": counts(validation["caps_and_queues"]["queues"]),
        "e_flows_plus_fees": counts(validation["flows"]),
        "e_transfer_ledger": {"checked": len(ledger), "exact": sum(ledger)},
        "output_integrity_errors": len(validation["output_integrity"]["errors"])}
    head_check = validation["head_positions"]
    validation["passed"] = head_check["exact"] + head_check["no_code"] == head_check["checked"] and all(
        validation[k]["exact"] == validation[k]["checked"] for k in
        ["archive_positions", "roles", "flows"]) and archive["checked"] >= 200 and all(
        validation["caps_and_queues"][k]["exact"] == validation["caps_and_queues"][k]["checked"] for k in ["caps", "queues"]) and all(
        r["transfer_ledger_exact"] for r in validation["flows"]["rows"])
    atomic(path, validation)
    print(f"[{cid}] validation passed={validation['passed']}; roles {validation['roles']['exact']}/"
          f"{validation['roles']['checked']}; flows {validation['flows']['exact']}/{len(vaults)}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain", required=True, type=int, choices=[1, 42161, 43114, 146])
    parser.add_argument("--stage", default="all", choices=["sources", "scan", "validate", "all"])
    args = parser.parse_args()
    cid = args.chain
    out = io_utils.DATA / f"vaults/silo_v2/{cid}"
    out.mkdir(parents=True, exist_ok=True)
    registry = io_utils.read_jsonl(f"registry/vaults/silo_v2/{cid}.jsonl")
    cfg = logs.load_chains()[str(cid)]
    started = time.time()
    before = rpc.calls_made()
    try:
        implementations = discover(cid, out, registry, logs.rpc_target(cfg))
        if args.stage == "sources":
            resolve_claiming_clones(cid, out, logs.rpc_target(cfg))
        if args.stage in ("scan", "all"):
            scan_chain(cid, out, cfg, registry, implementations)
        if args.stage in ("validate", "all"):
            validate(cid, out, cfg, registry)
    finally:
        path = out / "runs.json"
        runs = json.loads(path.read_text()) if path.exists() else []
        runs.append({"stage": args.stage, "started_utc": datetime.fromtimestamp(started, timezone.utc).isoformat(),
            "runtime_seconds": round(time.time() - started, 3), "rpc_requests": rpc.calls_made() - before})
        atomic(path, runs)
        stats_path = out / "statistics.json"
        if stats_path.exists():
            stats = json.loads(stats_path.read_text())
            sizes = {str(p.relative_to(out)): p.stat().st_size for p in out.rglob("*")
                     if p.is_file() and p != stats_path}
            stats.update(file_sizes=sizes, rpc_requests=sum(r["rpc_requests"] for r in runs),
                         cumulative_runtime_seconds=round(sum(r["runtime_seconds"] for r in runs), 3))
            stats["output_bytes"] = sum(size for name, size in sizes.items() if
                name.startswith(("positions/", "flows/")) and name.endswith(".csv") or
                name in ["roles.jsonl", "actions.jsonl", "fees.csv", "vaults.jsonl"])
            stats["all_files_bytes_excluding_statistics"] = sum(sizes.values())
            atomic(stats_path, stats)
        if (out / "scan.json").exists():
            write_sidecars(cid, out)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        message = str(exc)
        for key in ["DRPC_API_KEY", "HYPERSYNC_API_TOKEN", "ETHERSCAN_API_KEY"]:
            secret = env.get(key)
            if secret:
                message = message.replace(secret, "[redacted]")
        print(f"{type(exc).__name__}: {message}", file=sys.stderr, flush=True)
        raise SystemExit(1) from None
