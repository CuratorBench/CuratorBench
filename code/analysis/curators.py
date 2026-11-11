# /// script
# requires-python = ">=3.11"
# dependencies = ["eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""E1, curator attribution."""
import argparse
import json
import random
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests
from eth_utils import keccak, to_checksum_address

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common import env, evmabi, io_utils, multicall, rpc

GENERATOR = "code/analysis/curators.py"
ROOT = HERE.parents[1]
DATA = ROOT / "data"
CHAINS = json.loads((HERE.parent / "config" / "chains.json").read_text())
SOURCES = "registry/curator_sources"

PAIRS = {"morpho": [1, 8453, 4663, 42161, 747474, 999],
         "euler_v2": [1, 9745, 59144],
         "moolah": [56],
         "silo_v2": [43114, 146, 1, 42161]}

ROLE_PRIORITY = {
    "morpho_v1": ["curator", "owner", "allocator"],
    "morpho_v2": ["curator", "owner", "allocator", "sentinel"],
    "moolah_vault": ["CURATOR", "MANAGER", "DEFAULT_ADMIN_ROLE", "ALLOCATOR"],
    "silo_vault": ["curator", "owner", "allocator"],
    "euler_earn": ["curator", "owner", "allocator"],
    "euler_lending": ["governorAdmin"],
}
SECONDARY_ROLES = {"allocator", "sentinel", "ALLOCATOR"}
BURN = {"0x" + "0" * 40, "0x" + "0" * 39 + "1",
        "0x000000000000000000000000000000000000dead"}

FACTS_MIN_USD = 1_000

CONFIDENCE_ORDER = {"official": 4, "hand": 3, "cluster": 2, "name_only": 1,
                    "unattributed": 0}
HAND = f"{SOURCES}/hand_entities.jsonl"

MORPHO_API = "https://api.morpho.org/graphql"
LISTA_API = "https://api.lista.org/api/moolah"
SILO_API = "https://app.silo.finance/api"
SILO_CHAIN_KEYS = {1: "ethereum", 42161: "arbitrum", 43114: "avalanche",
                   146: "sonic"}
EULER_REPO = "euler-xyz/euler-labels"
ETHERSCAN = "https://api.etherscan.io/v2/api"
ENS_REGISTRY = "0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e"

MORPHO_CURATORS_Q = """{ curators(first: 1000) { items { id name verified
 ownerOnly description socials { type url } addresses { chainId address } } } }"""
MORPHO_VAULTS_Q = """query($skip: Int!) { vaults(first: 500, skip: $skip) {
 items { address chain { id } name symbol listed creatorAddress
 metadata { description }
 state { curator owner guardian curators { id name } } } } }"""
MORPHO_VAULTV2S_Q = """query($skip: Int!) { vaultV2s(first: 100, skip: $skip) {
 items { address chain { id } name symbol listed curator { address }
 owner { address } metadata { description } curators { items { id name } } } } }"""

NAME_ALIASES = {
    "ninesummits": "9summits",
    "silodao": "silo-dao",
    "edgeultrayield": "ultrayield",
    "re7": "re7-labs",
}
ID_OVERRIDES = {"eco-vaults-shf": "waterline"}

SYMBOL_PREFIXES = {"gt": "gauntlet", "steak": "steakhouse-financial",
                   "re7": "re7-labs", "mev": "mev-capital"}


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def norm(name):
    """Normalized key for comparing entity names across sources."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def slug(name):
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", name.lower())).strip("-")


def http_json(method, url, retries=5, **kw):
    """JSON over HTTP with retries on transient errors."""
    last = None
    for attempt in range(retries):
        try:
            r = requests.request(method, url, timeout=120, **kw)
            if r.status_code in (429, 502, 503, 504):
                last = f"HTTP {r.status_code}"
                time.sleep(2 * (attempt + 1))
                continue
            return r
        except requests.RequestException as exc:
            last = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"{url}: {last}")


def write_work(work, name, obj):
    path = Path(work) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj))


def read_work(work, name):
    return json.loads((Path(work) / name).read_text())


def morpho_query(query, variables=None):
    for attempt in range(5):
        d = http_json("POST", MORPHO_API,
                      json={"query": query, "variables": variables or {}}).json()
        if "errors" not in d:
            return d["data"]
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Morpho API: {d['errors'][:1]}")


def morpho_paged(query, key, page):
    """All items of a paged Morpho API list query."""
    items, skip = [], 0
    while True:
        batch = morpho_query(query, {"skip": skip})[key]["items"]
        items += batch
        if len(batch) < page:
            return items
        skip += page


def fetch_morpho():
    """Curator registry and vault-level curator labels from the Morpho API."""
    retrieved = now_utc()
    curators = morpho_query(MORPHO_CURATORS_Q)["curators"]["items"]
    meta = {"what": "Morpho API curator registry: curator id, name, ownerOnly "
            "flag and the addresses Morpho attributes to each curator per chain",
            "url": MORPHO_API, "query": MORPHO_CURATORS_Q, "retrieved_at": retrieved}
    io_utils.write_json(f"{SOURCES}/morpho_api_curators.json",
                        {"retrieved_at": retrieved, "items": curators},
                        GENERATOR, meta)
    v1 = morpho_paged(MORPHO_VAULTS_Q, "vaults", 500)
    v2 = morpho_paged(MORPHO_VAULTV2S_Q, "vaultV2s", 100)
    meta = {"what": "Morpho API vault listings (V1 vaults and V2 vaultV2s) with "
            "the API's curator labels, current curator and owner, and "
            "listing descriptions; used to validate attribution",
            "url": MORPHO_API, "query": [MORPHO_VAULTS_Q, MORPHO_VAULTV2S_Q],
            "retrieved_at": now_utc()}
    io_utils.write_json(f"{SOURCES}/morpho_api_vaults.json",
                        {"retrieved_at": meta["retrieved_at"], "vaults": v1,
                         "vaultV2s": v2}, GENERATOR, meta)
    print(f"morpho: {len(curators)} curators, {len(v1)} V1, {len(v2)} V2 vaults")


def fetch_euler(ref):
    """Every chain's entities, products and earn-vaults files of euler-labels at the head of `ref`, pinned to its commit."""
    api = f"https://api.github.com/repos/{EULER_REPO}"
    repo = http_json("GET", api).json()
    sha = http_json("GET", f"{api}/commits/{ref}").json()["sha"]
    tree = http_json("GET", f"{api}/git/trees/{sha}?recursive=1").json()["tree"]
    files = [t["path"] for t in tree if t["type"] == "blob"
             and re.fullmatch(r"\d+/(entities|products|earn-vaults)\.json", t["path"])]
    retrieved = now_utc()
    for path in files:
        url = f"https://raw.githubusercontent.com/{EULER_REPO}/{sha}/{path}"
        r = http_json("GET", url)
        r.raise_for_status()
        io_utils.write_json(f"{SOURCES}/euler-labels/{path}", r.json(), GENERATOR,
                            {"what": "euler-labels file, verbatim", "url": url,
                             "commit": sha, "retrieved_at": retrieved})
    manifest = {"repository": f"https://github.com/{EULER_REPO}",
                "owner": repo["owner"]["login"], "owner_type": repo["owner"]["type"],
                "default_branch": repo["default_branch"], "ref": ref,
                "commit": sha, "files": sorted(files), "retrieved_at": retrieved}
    io_utils.write_json(f"{SOURCES}/euler-labels/manifest.json", manifest, GENERATOR,
                        {"what": "euler-labels snapshot manifest",
                         "retrieved_at": retrieved})
    print(f"euler-labels: {len(files)} files at {sha}")


def fetch_lista():
    """Lista's Moolah vault list and per-vault info (curator name) for every registry vault, from the API that Lista's own SDK uses."""
    retrieved = now_utc()
    lists = {}
    for zone in (0, 1):
        url = f"{LISTA_API}/vault/list"
        params = {"page": 1, "pageSize": 500, "chain": "bsc", "zone": zone}
        d = http_json("GET", url, params=params).json()
        lists[str(zone)] = d.get("data") or {"error": d.get("msg")}
    info = {}
    for row in io_utils.read_jsonl("registry/vaults/moolah/56.jsonl"):
        d = http_json("GET", f"{LISTA_API}/vault/info",
                      params={"address": row["vault"]}).json()
        info[row["vault"]] = (d["data"] if d.get("code") == "000000000"
                              else {"error": d.get("msg")})
        time.sleep(0.2)
    meta = {"what": "Lista (Moolah) vault list by zone and vault/info for every "
            "registry vault; the curator field names the vault's curator",
            "url": [f"{LISTA_API}/vault/list", f"{LISTA_API}/vault/info"],
            "sdk": "@lista-dao/moolah-sdk-core (LISTA_API_URLS.prod)",
            "retrieved_at": retrieved}
    io_utils.write_json(f"{SOURCES}/lista_api_vaults.json",
                        {"retrieved_at": retrieved, "list": lists, "info": info},
                        GENERATOR, meta)
    listed = sum(1 for v in info.values() if "error" not in v)
    print(f"lista: {listed} of {len(info)} registry vaults known to the API")


def fetch_silo():
    """Silo's earn-vault list and the lending-vault record (vault manager name, owner, curator) for every registry vault in sco."""
    retrieved = now_utc()
    body = {"vaultIds": [], "search": None, "riskProfiles": [], "chainKeys": [],
            "minTotalSupplyUsd": None, "maxTotalSupplyUsd": None, "sort": None,
            "limit": 500, "offset": 0}
    earn = http_json("POST", f"{SILO_API}/earn-vaults", json=body).json()
    info = {}
    for cid in PAIRS["silo_v2"]:
        for row in io_utils.read_jsonl(f"registry/vaults/silo_v2/{cid}.jsonl"):
            vid = f"{SILO_CHAIN_KEYS[cid]}-{to_checksum_address(row['vault'])}"
            r = http_json("POST", f"{SILO_API}/lending-vault",
                          json={"vaultId": vid, "account": None})
            try:
                d = r.json()
                info[f"{cid}:{row['vault']}"] = {
                    k: d.get(k) for k in ("vaultManagerInfo", "visibility", "owner",
                                          "curator", "guardian", "vaultName",
                                          "vaultSymbol", "vaultId")}
            except ValueError:
                info[f"{cid}:{row['vault']}"] = {"error": r.text[:200]}
            time.sleep(0.2)
    meta = {"what": "Silo earn-vaults list and lending-vault records; "
            "vaultManagerInfo names the vault's manager (curator)",
            "url": [f"{SILO_API}/earn-vaults", f"{SILO_API}/lending-vault"],
            "retrieved_at": retrieved}
    io_utils.write_json(f"{SOURCES}/silo_api_vaults.json",
                        {"retrieved_at": retrieved, "earn_vaults": earn,
                         "lending_vault": info}, GENERATOR, meta)
    listed = sum(1 for v in info.values() if v.get("vaultManagerInfo"))
    print(f"silo: {listed} of {len(info)} registry vaults have a vault manager")


def stage_fetch(args):
    """Snapshot every official source into data/registry/curator_sources/."""
    fetch_morpho()
    fetch_euler(args.euler_ref)
    fetch_lista()
    fetch_silo()


def load_aum():
    """Per vault: monthly net AUM in USD (the vault's own reading minus the part held by other vaults, so nested deposits count once)."""
    out = {}
    for r in io_utils.read_jsonl("registry/vault_aum.jsonl"):
        net = {}
        for t, usd in r["usd"].items():
            held = sum(s.get(t, 0) for s in r["held_by_vaults_usd"].values())
            net[int(t)] = max(0.0, usd - held)
        out[(r["protocol"], r["chain_id"], r["vault"])] = (r["kind"], r.get("name"), net)
    return out


def role_events(protocol, cid):
    """Role events per vault as [block, log_index, timestamp, role, address, added], from the vault layer's roles.jsonl."""
    out = defaultdict(list)
    for r in io_utils.read_jsonl(f"vaults/{protocol}/{cid}/roles.jsonl"):
        added = r["action"] == "added" if "action" in r else bool(r["added"])
        out[r["vault"]].append([r["block"], r["log_index"], r["timestamp"],
                                r["role"], r["address"].lower(), added])
    return out


def governor_events(cid):
    """Euler lending vault governor history."""
    out = defaultdict(list)
    for r in io_utils.read_jsonl(f"panel/euler_v2/{cid}/governance.jsonl"):
        if r["event"] == "EVaultCreated":
            who = r["creator"]
        elif r["event"] == "GovSetGovernorAdmin":
            who = r["newGovernorAdmin"]
        else:
            continue
        out[r["vault"]].append([r["block"], r["log_index"], r["timestamp"],
                                "governorAdmin", who.lower(), True])
    return out


def stage_objects(args):
    """Every curated object with its role events and AUM series."""
    aum = load_aum()
    objs = []
    for protocol, cids in PAIRS.items():
        for cid in cids:
            events = role_events(protocol, cid)
            for row in io_utils.read_jsonl(f"registry/vaults/{protocol}/{cid}.jsonl"):
                kind, _, net = aum[(protocol, cid, row["vault"])]
                objs.append({"protocol": protocol, "chain_id": cid,
                             "vault": row["vault"], "kind": row["kind"],
                             "name": row.get("name"), "symbol": row.get("symbol"),
                             "created_block": row["created_block"],
                             "created_ts": row["created_ts"],
                             "events": sorted(events.get(row["vault"], [])),
                             "aum": net})
            if protocol != "euler_v2":
                continue
            gov = governor_events(cid)
            markets = {r["vault"]: r for r in
                       io_utils.read_jsonl(f"registry/markets/euler_v2/{cid}.jsonl")
                       if r.get("type") == "vault"}
            for (p, c, vault), (kind, _, net) in aum.items():
                if p != protocol or c != cid or kind != "euler_lending":
                    continue
                objs.append({"protocol": protocol, "chain_id": cid, "vault": vault,
                             "kind": kind, "name": None, "symbol": None,
                             "created_block": markets[vault]["created_block"],
                             "created_ts": markets[vault]["created_ts"],
                             "events": sorted(gov.get(vault, [])), "aum": net})
    for o in objs:
        peak = max(o["aum"].items(), key=lambda kv: kv[1], default=(None, 0.0))
        o["peak_ts"], o["peak_usd"] = peak if peak[1] > 0 else (None, 0.0)
        o["aum"] = {str(k): v for k, v in o["aum"].items()}
    write_work(args.work, "objects.json", objs)
    print(f"objects: {len(objs)}", Counter(o["kind"] for o in objs))


def replay(events):
    """Role state after each block that changes it."""
    state = defaultdict(set)
    out = []
    for block, _, ts, role, who, added in events:
        if role == "governorAdmin":
            state[role] = set() if who in BURN else {who}
        elif added and who not in BURN:
            state[role].add(who)
        else:
            state[role].discard(who)
        snap = {k: sorted(v) for k, v in state.items() if v}
        if out and out[-1][0] == block:
            out[-1] = (block, ts, snap)
        else:
            out.append((block, ts, snap))
    return out


class Entities:
    """Canonical entity ids across sources."""

    def __init__(self):
        self.rows = {}
        self.by_key = {}

    def resolve(self, name, source_id=None):
        """Canonical id for a name (and the source's own id), creating the entity on first sight."""
        key = norm(name)
        eid = (NAME_ALIASES.get(key) or self.by_key.get(key)
               or (source_id and self.by_key.get(norm(source_id))))
        if not eid:
            eid = ID_OVERRIDES.get(source_id, source_id) if source_id else slug(name)
        row = self.rows.setdefault(eid, {"entity_id": eid, "display_name": name,
                                         "aliases": set(), "official_sources": [],
                                         "owner_only": False, "notes": []})
        row["aliases"].add(name)
        self.by_key[key] = eid
        self.by_key[norm(eid)] = eid
        if source_id:
            self.by_key[norm(source_id)] = eid
        return eid

    def source(self, eid, name, url, path, retrieved_at):
        ref = {"name": name, "url": url, "path": path, "retrieved_at": retrieved_at}
        if ref not in self.rows[eid]["official_sources"]:
            self.rows[eid]["official_sources"].append(ref)


def split_curators(text):
    """Lista writes co-curated vaults as 'A x B'."""
    return [p.strip() for p in re.split(r"\s+x\s+", text) if p.strip()]


def load_official():
    """Parse the fetched official sources into address- and vault-keyed maps over canonical entity ids."""
    ents = Entities()
    off = {"entities": ents, "morpho_addr": {}, "euler_addr": {},
           "lista": {}, "silo": {}, "morpho_vault": {}, "morpho_desc": {},
           "euler_products": {}, "euler_earn_listed": set(), "hand": {},
           "files": []}

    path = f"{SOURCES}/morpho_api_curators.json"
    cur = json.loads((DATA / path).read_text())
    off["files"].append(path)
    for c in cur["items"]:
        eid = ents.resolve(c["name"], c["id"])
        ents.rows[eid]["owner_only"] |= bool(c["ownerOnly"])
        ents.rows[eid]["morpho_id"] = c["id"]
        ents.source(eid, "Morpho API curators registry", MORPHO_API, f"data/{path}",
                    cur["retrieved_at"])
        for a in c["addresses"]:
            off["morpho_addr"][(a["chainId"], a["address"].lower())] = eid

    path = f"{SOURCES}/morpho_api_vaults.json"
    mv = json.loads((DATA / path).read_text())
    off["files"].append(path)
    for v in mv["vaults"] + mv["vaultV2s"]:
        key = (v["chain"]["id"], v["address"].lower())
        if "state" in v:
            labels = ((v["state"] or {}).get("curators")) or []
        else:
            labels = (v.get("curators") or {}).get("items") or []
        off["morpho_vault"][key] = {
            "entities": [ents.resolve(x["name"], x["id"]) for x in labels],
            "listed": v.get("listed"), "name": v.get("name")}
        desc = (v.get("metadata") or {}).get("description")
        if desc:
            off["morpho_desc"][key] = desc
    off["morpho_retrieved_at"] = mv["retrieved_at"]

    base = DATA / SOURCES / "euler-labels"
    manifest = json.loads((base / "manifest.json").read_text())
    off["euler_manifest"] = manifest
    for f in sorted(base.glob("*/entities.json")):
        rel = str(f.relative_to(DATA))
        off["files"].append(rel)
        for key, e in json.loads(f.read_text()).items():
            eid = ents.resolve(e["name"], key)
            ents.rows[eid]["euler_id"] = key
            ents.source(eid, f"euler-labels {f.parent.name}/entities.json @ "
                        f"{manifest['commit'][:12]}",
                        f"https://github.com/{EULER_REPO}/blob/{manifest['commit']}/"
                        f"{f.parent.name}/entities.json", f"data/{rel}",
                        manifest["retrieved_at"])
            for a, label in (e.get("addresses") or {}).items():
                prev = off["euler_addr"].get(a.lower())
                if prev and prev[0] != eid:
                    raise RuntimeError(f"euler-labels: {a} under {prev[0]} and {eid}")
                off["euler_addr"][a.lower()] = (eid, label, int(f.parent.name))
    euler_ids = {r.get("euler_id"): eid for eid, r in ents.rows.items() if r.get("euler_id")}
    for f in sorted(base.glob("*/products.json")):
        off["files"].append(str(f.relative_to(DATA)))
        cid = int(f.parent.name)
        for pid, p in json.loads(f.read_text()).items():
            owners = p.get("entity") or []
            owners = [owners] if isinstance(owners, str) else owners
            gov = euler_ids.get(owners[0]) if owners else None
            for dep, vaults in ((False, p.get("vaults") or []),
                                (True, p.get("deprecatedVaults") or [])):
                for v in vaults:
                    off["euler_products"][(cid, v.lower())] = {
                        "entity": gov, "entity_raw": owners, "product": pid,
                        "deprecated": dep}
    for f in sorted(base.glob("*/earn-vaults.json")):
        off["files"].append(str(f.relative_to(DATA)))
        for item in json.loads(f.read_text()):
            a = item if isinstance(item, str) else item["address"]
            off["euler_earn_listed"].add((int(f.parent.name), a.lower()))

    path = f"{SOURCES}/lista_api_vaults.json"
    lista = json.loads((DATA / path).read_text())
    off["files"].append(path)
    for vault, info in lista["info"].items():
        if "error" in info or not info.get("curator"):
            continue
        names = split_curators(info["curator"])
        eids = [ents.resolve(n) for n in names]
        for eid in eids:
            ents.source(eid, "Lista API vault/info", f"{LISTA_API}/vault/info",
                        f"data/{path}", lista["retrieved_at"])
        off["lista"][(56, vault)] = {"entities": eids, "raw": info["curator"],
                                     "name": info.get("name")}

    path = f"{SOURCES}/silo_api_vaults.json"
    silo = json.loads((DATA / path).read_text())
    off["files"].append(path)
    for key, info in silo["lending_vault"].items():
        mgr = info.get("vaultManagerInfo")
        if not mgr:
            continue
        cid, vault = key.split(":")
        eid = ents.resolve(mgr["name"], mgr["id"])
        ents.source(eid, "Silo API lending-vault", f"{SILO_API}/lending-vault",
                    f"data/{path}", silo["retrieved_at"])
        off["silo"][(int(cid), vault)] = {"entities": [eid], "raw": mgr["name"],
                                          "visibility": info.get("visibility"),
                                          "owner": (info.get("owner") or "").lower(),
                                          "curator": (info.get("curator") or "").lower()}

    if (DATA / HAND).exists():
        off["files"].append(HAND)
        for r in (json.loads(x) for x in (DATA / HAND).read_text().splitlines() if x.strip()):
            eid = ents.resolve(r["display_name"], r["entity_id"])
            ents.source(eid, "hand_entities.jsonl, hand-verified addresses", None,
                        f"data/{HAND}", r["checked_at"])
            key = (int(r["chain_id"]), r["address"].lower())
            if key in off["hand"] and off["hand"][key]["entity"] != eid:
                raise RuntimeError(f"hand_entities: {key} under two entities")
            off["hand"][key] = {"entity": eid, "row": r}
    return off


def namehash(name):
    """ENS namehash (EIP-137) of an ASCII lowercase name."""
    node = b"\0" * 32
    if name:
        for label in reversed(name.split(".")):
            node = keccak(node + keccak(text=label))
    return node


def code_kind(code):
    """eoa or contract from eth_getCode; Safes are told apart later."""
    if code in ("0x", "", None):
        return "eoa"
    if code.startswith("0xef0100") and len(code) == 48:
        return "eoa"
    return "contract"


def etherscan_name(cid, addr, key):
    """Verified contract name (and implementation name for proxies) from the local source cache or Etherscan v2."""
    cached = DATA / "contracts" / f"{cid}-{addr}" / "_explorer.json"
    if cached.exists():
        item = json.loads(cached.read_text())
        calls = 0
    else:
        for attempt in range(5):
            d = http_json("GET", ETHERSCAN, params={
                "chainid": cid, "module": "contract", "action": "getsourcecode",
                "address": addr, "apikey": key}).json()
            if d.get("status") == "1":
                break
            if "rate limit" in str(d.get("result", "")).lower():
                time.sleep(1.5 * (attempt + 1))
                continue
            return {"error": str(d.get("result"))[:120]}, attempt + 1
        else:
            return {"error": "rate limited"}, 5
        item = d["result"][0]
        calls = 1
        time.sleep(0.25)
    return ({"contract_name": item.get("ContractName") or None,
             "proxy_implementation": (item.get("Implementation") or "").lower() or None},
            calls)


def stage_onchain(args):
    """Address facts for clustering."""
    objs = read_work(args.work, "objects.json")
    off = load_official()
    pairs = set()
    for o in objs:
        for e in o["events"]:
            listed = (o["chain_id"], e[4]) in off["morpho_addr"] or e[4] in off["euler_addr"]
            if e[4] not in BURN and (o["peak_usd"] >= FACTS_MIN_USD or listed):
                pairs.add((o["chain_id"], e[4]))
    scope = {c for cids in PAIRS.values() for c in cids}
    for (cid, a) in off["morpho_addr"]:
        if cid in scope:
            pairs.add((cid, a))
    for a in off["euler_addr"]:
        for cid in PAIRS["euler_v2"]:
            pairs.add((cid, a))
    for (cid, a) in off["hand"]:
        if cid in scope:
            pairs.add((cid, a))
    listed_on = defaultdict(set)
    for (cid, a) in off["morpho_addr"]:
        listed_on[a].add(cid)
    have = defaultdict(set)
    for cid, a in pairs:
        have[a].add(cid)
    for a, cids in list(have.items()):
        if listed_on.get(a) and not (listed_on[a] & cids):
            reachable = [c for c in listed_on[a] if CHAINS.get(str(c), {}).get("drpc")]
            if reachable:
                pairs.add((min(reachable, key=lambda c: (c not in scope, c)), a))
    pairs = sorted(p for p in pairs if CHAINS.get(str(p[0]), {}).get("drpc"))
    path = Path(args.work) / "address_facts.json"
    prev = (json.loads(path.read_text()) if path.exists() else
            {"facts": {}, "ens": {}, "ens_checked": [], "rpc_calls": 0,
             "etherscan_calls": 0})
    facts = prev["facts"]
    todo = [(cid, a) for cid, a in pairs if f"{cid}:{a}" not in facts]
    print(f"onchain: {len(pairs)} chain-address pairs, {len(todo)} new")
    start_calls = rpc.calls_made()

    for i, (cid, a) in enumerate(todo):
        code = rpc.get_code(CHAINS[str(cid)]["drpc"], a)
        facts[f"{cid}:{a}"] = {"chain_id": cid, "address": a, "kind": code_kind(code)}
        if i % 500 == 499:
            print(f"  getCode {i + 1}/{len(todo)}", flush=True)
    new = {f"{cid}:{a}" for cid, a in todo}

    sel = {name: evmabi.selector(sig) for name, sig in [
        ("getOwners", "getOwners()"), ("getThreshold", "getThreshold()"),
        ("owner", "owner()")]}
    admin_call = evmabi.encode_call("getRoleMember(bytes32,uint256)",
                                    ["bytes32", "uint256"], [b"\0" * 32, 0])
    by_chain = defaultdict(list)
    for k in new:
        if facts[k]["kind"] == "contract":
            by_chain[facts[k]["chain_id"]].append(k)
    for cid, keys in by_chain.items():
        calls = []
        for k in keys:
            t = facts[k]["address"]
            calls += [(t, sel["getOwners"]), (t, sel["getThreshold"]),
                      (t, sel["owner"]), (t, admin_call)]
        res = multicall.aggregate3(CHAINS[str(cid)]["drpc"], calls, chunk=200)
        for j, k in enumerate(keys):
            ow, th, own, adm = res[4 * j: 4 * j + 4]
            f = facts[k]
            try:
                owners = evmabi.decode(["address[]"], ow[1])[0] if ow[0] else None
                thr = evmabi.decode(["uint256"], th[1])[0] if th[0] else None
            except Exception:
                owners, thr = None, None
            if owners and thr and 0 < thr <= len(owners):
                f["kind"] = "safe"
                f["owners"] = sorted(x.lower() for x in owners)
                f["threshold"] = thr
            for name, (ok, ret) in (("owner_fn", own), ("admin0", adm)):
                if ok and len(ret) == 66:
                    who = evmabi.addr(ret)
                    if who not in BURN:
                        f[name] = who

    checked = set(prev["ens_checked"])
    fresh = sorted({f["address"] for f in facts.values()} - checked)
    ens = dict(prev["ens"], **ens_reverse(fresh)) if fresh else prev["ens"]

    key = env.require("ETHERSCAN_API_KEY")
    escalls = 0
    for k in sorted(new):
        f = facts[k]
        if f["kind"] != "contract":
            continue
        info, n = etherscan_name(f["chain_id"], f["address"], key)
        escalls += n
        f.update(info)
        impl = info.get("proxy_implementation")
        if impl:
            iinfo, n = etherscan_name(f["chain_id"], impl, key)
            escalls += n
            f["implementation_name"] = iinfo.get("contract_name")
    calls = rpc.calls_made() - start_calls
    out = {"facts": facts, "ens": ens, "ens_checked": sorted(checked | set(fresh)),
           "rpc_calls": prev["rpc_calls"] + calls,
           "etherscan_calls": prev["etherscan_calls"] + escalls, "built_at": now_utc()}
    write_work(args.work, "address_facts.json", out)
    print(f"onchain: {calls} RPC calls this run ({out['rpc_calls']} in total), "
          f"{escalls} Etherscan calls; {sum(1 for f in facts.values() if f['kind'] == 'safe')} "
          f"Safes, {len(ens)} ENS names")


def ens_reverse(addresses):
    """Primary ENS names on Ethereum, kept only when the forward record of the name resolves back to the address."""
    chain = CHAINS["1"]["drpc"]
    nodes = {a: namehash(a[2:] + ".addr.reverse") for a in addresses}
    sel_res = "resolver(bytes32)"
    res = multicall.aggregate3(chain, [(ENS_REGISTRY, evmabi.encode_call(
        sel_res, ["bytes32"], [nodes[a]])) for a in addresses])
    resolvers = {}
    for a, (ok, ret) in zip(addresses, res):
        if ok and len(ret) == 66 and evmabi.addr(ret) not in BURN:
            resolvers[a] = evmabi.addr(ret)
    todo = sorted(resolvers)
    res = multicall.aggregate3(chain, [(resolvers[a], evmabi.encode_call(
        "name(bytes32)", ["bytes32"], [nodes[a]])) for a in todo])
    names = {}
    for a, (ok, ret) in zip(todo, res):
        try:
            name = evmabi.decode(["string"], ret)[0] if ok else ""
        except Exception:
            name = ""
        if name and name.isascii():
            names[a] = name.lower()
    todo = sorted(names)
    res = multicall.aggregate3(chain, [(ENS_REGISTRY, evmabi.encode_call(
        sel_res, ["bytes32"], [namehash(names[a])])) for a in todo])
    fwd_res = {a: evmabi.addr(ret) for a, (ok, ret) in zip(todo, res)
               if ok and len(ret) == 66}
    todo = sorted(fwd_res)
    res = multicall.aggregate3(chain, [(fwd_res[a], evmabi.encode_call(
        "addr(bytes32)", ["bytes32"], [namehash(names[a])])) for a in todo])
    out = {}
    for a, (ok, ret) in zip(todo, res):
        if ok and len(ret) == 66 and evmabi.addr(ret) == a:
            out[a] = names[a]
    return out


def link(entity, rule, confidence, evidence, source):
    return {"entity": entity, "rule": rule, "confidence": confidence,
            "evidence": evidence, "source": source}


def top_holders(kind, roles):
    """Holders of the highest-priority controlling role that is set."""
    for role in ROLE_PRIORITY[kind]:
        if role in SECONDARY_ROLES:
            break
        if roles.get(role):
            return role, roles[role]
    return None, []


class Book:
    """Address-to-entity links per (chain, address), with the rule and evidence that produced each."""

    def __init__(self, objs, facts, off):
        self.objs, self.off = objs, off
        self.facts = facts["facts"]
        self.ens = facts["ens"]
        self.links = {}
        self.conflicts = []
        self.rule_counts = Counter()
        self.holders = defaultdict(set)
        self.spread = defaultdict(set)
        for o in objs:
            for e in o["events"]:
                if e[4] not in BURN:
                    self.holders[(o["chain_id"], e[4])].add(f"{o['protocol']}:{e[3]}")
                    self.spread[(o["chain_id"], e[4], e[3])].add(o["vault"])
        self.official()
        self.hand()
        self.listings()
        self.cross_chain()
        self.safe_signers()
        self.contract_controllers()
        self.ens_names()

    def fact(self, cid, a):
        return self.facts.get(f"{cid}:{a}")

    def put(self, key, lk):
        if key in self.links:
            return
        self.links[key] = lk
        self.rule_counts[lk["rule"]] += 1

    def put_unique(self, cands, rule):
        """cands: {(chain, addr)."""
        for key, by_entity in cands.items():
            if key in self.links:
                continue
            if len(by_entity) == 1:
                self.put(key, next(iter(by_entity.values())))
            else:
                self.conflicts.append({"chain_id": key[0], "address": key[1],
                                       "rule": rule, "entities": sorted(by_entity)})

    def official(self):
        """Rule 1: addresses on the Morpho and euler-labels lists."""
        src_m = f"data/{SOURCES}/morpho_api_curators.json"
        for (cid, a), eid in self.off["morpho_addr"].items():
            self.put((cid, a), link(
                eid, "morpho_api_curator_address", "official",
                f"Morpho API curator registry lists {a} for {eid} on chain {cid}", src_m))
        commit = self.off["euler_manifest"]["commit"]
        chains = {c for c, _ in self.holders} | set(PAIRS["euler_v2"])
        for a, (eid, label, listed) in self.off["euler_addr"].items():
            for cid in chains:
                if (cid, a) in self.links and self.links[(cid, a)]["entity"] != eid:
                    other = self.links[(cid, a)]["entity"]
                    self.conflicts.append({"chain_id": cid, "address": a, "rule": "official",
                                           "entities": sorted({eid, other})})
                    continue
                where = "" if cid == listed else (
                    "; euler-labels addresses hold on every chain")
                self.put((cid, a), link(
                    eid, "euler_labels_entity_address", "official",
                    f"euler-labels {listed}/entities.json @ {commit[:12]} lists {a} "
                    f"as '{label}' under {eid}{where}",
                    f"data/{SOURCES}/euler-labels/{listed}/entities.json"))

    def hand(self):
        """Rule 2: hand-verified addresses (hand_entities.jsonl)."""
        for (cid, a), h in sorted(self.off["hand"].items()):
            eid, r = h["entity"], h["row"]
            prev = self.links.get((cid, a))
            if prev and prev["entity"] != eid:
                self.conflicts.append({"chain_id": cid, "address": a, "rule": "hand",
                                       "entities": sorted({eid, prev["entity"]})})
                continue
            ev = "; ".join(f"{k}: {v}" for k, v in sorted(r["evidence"].items()))
            self.put((cid, a), link(
                eid, "hand_entity_address", "hand",
                f"hand-verified {r['kind']} {a} on chain {cid} for {eid} "
                f"({', '.join(r['roles_seen'])}); {ev}; source {r['source_url']}",
                f"data/{HAND}"))

    def listings(self):
        """Role holders of vaults that Lista or Silo list by address with a named curator."""
        heads = {(o["chain_id"], o["vault"]): o for o in self.objs}
        seen = defaultdict(list)
        for source, rows, path in (("lista", self.off["lista"], "lista_api_vaults.json"),
                                   ("silo", self.off["silo"], "silo_api_vaults.json")):
            for key, lst in rows.items():
                o = heads.get(key)
                if not o:
                    continue
                states = replay(o["events"])
                role, holders = top_holders(o["kind"], states[-1][2] if states else {})
                for h in holders:
                    seen[(o["chain_id"], h)].append((set(lst["entities"]), role,
                                                     o["vault"], lst["raw"], source, path))
        cands = {}
        for key, items in seen.items():
            named = Counter(e for s, *_ in items for e in s)
            (eid, n), *rest = named.most_common()
            if 2 * n <= len(items) or (rest and rest[0][1] == n):
                self.conflicts.append({"chain_id": key[0], "address": key[1],
                                       "rule": "vault_listing_role_holder",
                                       "entities": sorted(named)})
                continue
            ex = "; ".join(f"{r} of {v} listed as '{raw}' by {src}"
                           for _, r, v, raw, src, _ in items[:3])
            cands[key] = {eid: link(eid, "vault_listing_role_holder", "cluster",
                                    f"holds the top controlling role on {len(items)} "
                                    f"listed vault(s), {n} of them naming {eid}: {ex}",
                                    f"data/{SOURCES}/{items[0][5]}")}
        self.put_unique(cands, "vault_listing_role_holder")

    def same_account(self, ca, cb, a):
        """Why address `a` is one controller on chains ca and cb, or None."""
        fa, fb = self.fact(ca, a), self.fact(cb, a)
        if not fa or not fb:
            return None
        if fa["kind"] == "eoa" and fb["kind"] == "eoa":
            return "EOA on both chains (one key)"
        if fa["kind"] == "safe" and fb["kind"] == "safe":
            sa, sb = set(fa["owners"]), set(fb["owners"])
            if sa == sb:
                return f"Safe on both chains with the same {len(sa)} signers"
        return None

    def cross_chain(self):
        """An address resolved on one chain is the same controller on another chain when it is an EOA on both or a Safe with identical signers."""
        resolved = defaultdict(list)
        for (cid, a), lk in self.links.items():
            resolved[a].append((cid, lk))
        cands = {}
        for (cid, a) in self.holders:
            if (cid, a) in self.links or a not in resolved:
                continue
            for other, lk in resolved[a]:
                why = self.same_account(other, cid, a)
                if why:
                    cands.setdefault((cid, a), {})[lk["entity"]] = link(
                        lk["entity"], "same_address_other_chain", "cluster",
                        f"{a} is resolved on chain {other} ({lk['rule']}); {why}",
                        lk["source"])
        self.put_unique(cands, "same_address_other_chain")

    def safe_signers(self):
        """Safes and EOAs tied to exactly one entity through the signers of that entity's own Safes (anchors."""
        signer_entities = defaultdict(set)
        signer_safes = defaultdict(list)
        for (cid, a), lk in self.links.items():
            f = self.fact(cid, a)
            if f and f["kind"] == "safe":
                for s in f["owners"]:
                    signer_entities[s].add(lk["entity"])
                    signer_safes[s].append(f"{cid}:{a}")
        distinct = {s: next(iter(es)) for s, es in signer_entities.items() if len(es) == 1}
        safe_c, eoa_c = {}, {}
        for (cid, a) in self.holders:
            if (cid, a) in self.links:
                continue
            f = self.fact(cid, a)
            if not f:
                continue
            if f["kind"] == "safe":
                by = defaultdict(list)
                for s in f["owners"]:
                    if s in distinct:
                        by[distinct[s]].append(s)
                need = max(2, -(-len(f["owners"]) // 2))
                for eid, shared in by.items():
                    if len(shared) >= need:
                        anchors = sorted({x for s in shared for x in signer_safes[s]})[:4]
                        safe_c.setdefault((cid, a), {})[eid] = link(
                            eid, "safe_shares_signers", "cluster",
                            f"Safe {a} ({f['threshold']}/{len(f['owners'])}) shares "
                            f"{len(shared)} signers {shared} with {eid} Safes {anchors}",
                            "eth_call getOwners()")
            elif f["kind"] == "eoa" and a in signer_entities:
                es = signer_entities[a]
                if len(es) == 1:
                    eid = next(iter(es))
                    eoa_c[(cid, a)] = {eid: link(
                        eid, "eoa_signs_entity_safe", "cluster",
                        f"EOA {a} signs {eid} Safes {sorted(set(signer_safes[a]))[:4]}",
                        "eth_call getOwners()")}
                else:
                    self.conflicts.append({"chain_id": cid, "address": a,
                                           "rule": "eoa_signs_entity_safe",
                                           "entities": sorted(es)})
        self.put_unique(safe_c, "safe_shares_signers")
        self.put_unique(eoa_c, "eoa_signs_entity_safe")

    def contract_controllers(self):
        """A non-Safe contract whose owner() or first DEFAULT_ADMIN_ROLE member is an entity address belongs to that entity."""
        cands = {}
        for (cid, a) in self.holders:
            f = self.fact(cid, a)
            if (cid, a) in self.links or not f or f["kind"] != "contract":
                continue
            for fn in ("owner_fn", "admin0"):
                who = f.get(fn)
                lk = self.links.get((cid, who)) if who else None
                if lk:
                    getter = ("owner()" if fn == "owner_fn"
                              else "getRoleMember(DEFAULT_ADMIN_ROLE, 0)")
                    name = f.get("contract_name") or "unverified"
                    cands.setdefault((cid, a), {})[lk["entity"]] = link(
                        lk["entity"], "contract_controlled_by_entity", "cluster",
                        f"{name} {a}: {getter} = {who}, which is {lk['entity']} "
                        f"({lk['rule']})", f"eth_call {getter}")
        self.put_unique(cands, "contract_controlled_by_entity")

    def ens_names(self):
        """A forward-verified primary ENS name whose second-level label is an entity alias."""
        keys = {}
        for eid, row in self.off["entities"].rows.items():
            for alias in row["aliases"] | {eid}:
                keys.setdefault(norm(alias), set()).add(eid)
        cands = {}
        for (cid, a) in self.holders:
            name = self.ens.get(a)
            f = self.fact(cid, a)
            if (cid, a) in self.links or not name or not f:
                continue
            if cid != 1 and f["kind"] != "eoa":
                continue
            labels = name.split(".")
            if len(labels) < 2 or len(keys.get(norm(labels[-2]), ())) != 1:
                continue
            eid = next(iter(keys[norm(labels[-2])]))
            cands[(cid, a)] = {eid: link(eid, "ens_primary_name", "cluster",
                                         f"{a} has forward-verified primary ENS name "
                                         f"{name}", "ENS registry on Ethereum")}
        self.put_unique(cands, "ens_primary_name")

    def get(self, cid, a):
        return self.links.get((cid, a))


GENERIC_WORDS = {"native", "edge", "galaxy", "unified", "sky"}


def name_patterns(ents):
    """Alias regexes per entity for proposing name_only candidates."""
    pats = []
    for eid, row in ents.rows.items():
        words = set()
        for alias in row["aliases"]:
            words.add(alias.lower())
            first = alias.split()[0].lower()
            if len(alias.split()) > 1 and len(first) >= 4 and first not in GENERIC_WORDS:
                words.add(first)
        for w in words:
            if w in GENERIC_WORDS:
                continue
            pats.append((eid, re.compile(r"(?<![a-z0-9])(?<!not )" + re.escape(w)
                                         + r"(?![a-z0-9])")))
    return pats


class Attributor:
    """Turns each object's role history into attribution intervals."""

    def __init__(self, book, off):
        self.book, self.off = book, off
        self.pats = name_patterns(off["entities"])
        self.infra = self.shared_secondaries()

    def shared_secondaries(self):
        """Allocator or sentinel addresses that serve objects whose primary roles resolve to two or more entities are shared infras."""
        served = defaultdict(set)
        for o in self.book.objs:
            states = replay(o["events"])
            if not states:
                continue
            roles = states[-1][2]
            _, holders = top_holders(o["kind"], roles)
            ents = {lk["entity"] for h in holders
                    if (lk := self.book.get(o["chain_id"], h))}
            for role in SECONDARY_ROLES & set(roles):
                for h in roles[role]:
                    served[(o["chain_id"], h)] |= ents
        return {k for k, v in served.items() if len(v) >= 2}

    def candidates(self, o):
        """Name candidates: vault name, symbol prefix, Morpho API description, and a vault listing outside its current interval."""
        found = []
        key = (o["chain_id"], o["vault"])
        for src in ("lista", "silo"):
            lst = self.off[src].get(key)
            if lst and len(lst["entities"]) == 1:
                found.append((lst["entities"][0], f"{src} listing names '{lst['raw']}' "
                              "for the vault today"))
        desc = self.off["morpho_desc"].get(key, "")
        for m in re.finditer(r"curated by ([^.;]+)", desc, re.I):
            for eid, pat in self.pats:
                if pat.match(m.group(1).lower()):
                    found.append((eid, f"Morpho API description: '{m.group(0)[:80]}'"))
        text = (o["name"] or "").lower()
        for eid, pat in self.pats:
            if pat.search(text):
                found.append((eid, f"vault name '{o['name']}'"))
        sym = o["symbol"] or ""
        for prefix, eid in SYMBOL_PREFIXES.items():
            if (sym.lower().startswith(prefix) and len(sym) > len(prefix)
                    and sym[len(prefix)].isupper()):
                found.append((eid, f"symbol '{sym}' uses the {prefix} prefix"))
        ents = sorted({e for e, _ in found})
        return ents, [w for _, w in found]

    def decide(self, o, roles, current, prev=None):
        """Attribution for one role state; `prev` is the previous state's result, kept when its controller still qualifies."""
        cid = o["chain_id"]
        notes = []
        for role in ROLE_PRIORITY[o["kind"]]:
            holders = roles.get(role, [])
            if role in SECONDARY_ROLES:
                holders = [h for h in holders if (cid, h) not in self.infra]
            hits = [(h, lk) for h in holders if (lk := self.book.get(cid, h))]
            if not hits:
                if holders and role not in SECONDARY_ROLES:
                    notes.append(f"{role} {','.join(holders)} unresolved")
                continue
            if role == "owner" and roles.get("curator"):
                ents = self.off["entities"].rows
                if all(ents[lk["entity"]]["owner_only"] for _, lk in hits):
                    notes.append(f"owner {hits[0][0]} is {hits[0][1]['entity']}, which "
                                 "Morpho marks as owner-only; curator unresolved")
                    continue
            listed = set()
            if current:
                for src in ("lista", "silo"):
                    listed |= set((self.off[src].get((cid, o["vault"])) or {}).get("entities", []))
            stay = prev["via_address"] if prev else None
            top = sorted(hits, key=lambda x: (
                x[1]["entity"] not in listed, -CONFIDENCE_ORDER[x[1]["confidence"]],
                x[0] != stay, len(self.book.spread[(cid, x[0], role)]), x[0]))
            h, lk = top[0]
            others = sorted({x["entity"] for _, x in top} - {lk["entity"]})
            ev = lk["evidence"]
            if others:
                ev += f"; same role also held by addresses of {others}"
            if notes:
                ev += "; " + "; ".join(notes)
            res = {"entity_id": lk["entity"], "via_role": role, "via_address": h,
                   "confidence": lk["confidence"], "rule": lk["rule"], "evidence": ev}
            break
        else:
            res = None
        if current:
            res = self.apply_listing(o, roles, res)
        if res:
            return res
        ents, why = self.candidates(o)
        base = "; ".join(notes) or "no controlling role holder"
        if len(ents) == 1:
            return {"entity_id": ents[0], "via_role": None, "via_address": None,
                    "confidence": "name_only", "rule": "name_candidate",
                    "evidence": f"{base}; candidate from " + "; ".join(why)}
        if ents:
            base += f"; conflicting name candidates {ents}"
        return {"entity_id": None, "via_role": None, "via_address": None,
                "confidence": "unattributed", "rule": None, "evidence": base}

    def apply_listing(self, o, roles, res):
        """A vault that Lista or Silo lists today with one named curator is official for its current interval; a listing that disag."""
        key = (o["chain_id"], o["vault"])
        for src in ("lista", "silo"):
            lst = self.off[src].get(key)
            if not lst:
                continue
            note = f"{src} lists the vault as curated by '{lst['raw']}'"
            if res and res["entity_id"] in lst["entities"]:
                return dict(res, confidence="official", listed=True,
                            evidence=res["evidence"] + "; " + note)
            if res:
                return dict(res, evidence=res["evidence"] + f"; {note} (disagrees)")
            if len(lst["entities"]) == 1:
                role, holders = top_holders(o["kind"], roles)
                return {"entity_id": lst["entities"][0], "via_role": role,
                        "via_address": holders[0] if holders else None,
                        "confidence": "official", "rule": "vault_listing",
                        "listed": True, "evidence": note}
        return res

    def intervals(self, o):
        """Attribution intervals of one object, oldest first."""
        states = replay(o["events"])
        if not states:
            r = self.decide(o, {}, True)
            return [dict(r, from_block=o["created_block"], from_ts=o["created_ts"],
                         to_block=None, to_ts=None)]
        out = []
        for i, (block, ts, roles) in enumerate(states):
            r = self.decide(o, roles, i == len(states) - 1, out[-1] if out else None)
            if not any(roles.get(role) for role in ROLE_PRIORITY[o["kind"]]):
                r["evidence"] = "all controlling roles renounced or unset; " + r["evidence"]
            same = out and all(out[-1][k] == r[k] for k in
                               ("entity_id", "via_role", "via_address", "confidence"))
            if same:
                if i == len(states) - 1:
                    out[-1]["evidence"] = r["evidence"]
                continue
            if out:
                out[-1]["to_block"], out[-1]["to_ts"] = block, ts
            out.append(dict(r, from_block=block, from_ts=ts, to_block=None, to_ts=None))
        while (len(out) > 1 and out[-1].get("listed") and all(
                out[-2][k] == out[-1][k] for k in ("entity_id", "via_role", "via_address"))):
            last = out.pop()
            out[-1] = dict(last, from_block=out[-1]["from_block"], from_ts=out[-1]["from_ts"])
        return out


def peak_interval(o, ivs):
    """The interval in force at the object's peak month (the last one when the object never held assets)."""
    if o["peak_ts"] is None:
        return ivs[-1]
    cur = ivs[0]
    for iv in ivs:
        if iv["from_ts"] <= o["peak_ts"]:
            cur = iv
    return cur


def first_seen(objs):
    """First block at which each (chain, address) held any role."""
    out = {}
    for o in objs:
        for e in o["events"]:
            k = (o["chain_id"], e[4])
            out[k] = min(out.get(k, e[0]), e[0])
    return out


def entity_rows(book, off, objs):
    """curators.jsonl: one row per entity with every address linked to it."""
    ents = off["entities"].rows
    seen = first_seen(objs)
    addrs = defaultdict(list)
    for (cid, a), lk in sorted(book.links.items()):
        listed_here = (lk["rule"] != "euler_labels_entity_address"
                       or off["euler_addr"][a][2] == cid)
        if (cid, a) not in book.holders and not listed_here:
            continue
        f = book.fact(cid, a)
        row = {"chain_id": cid, "address": a, "kind": f["kind"] if f else "unknown",
               "roles_seen": sorted(book.holders.get((cid, a), ())),
               "first_seen_block": seen.get((cid, a)), "source": lk["rule"],
               "source_path": lk["source"], "confidence": lk["confidence"],
               "evidence": lk["evidence"]}
        if f and f["kind"] == "safe":
            row["safe"] = {"threshold": f["threshold"], "owners": f["owners"]}
        if f and f.get("contract_name"):
            row["contract_name"] = f["contract_name"]
        if a in book.ens and (cid == 1 or (f and f["kind"] == "eoa")):
            row["ens"] = book.ens[a]
        addrs[lk["entity"]].append(row)
    rows = []
    for eid, r in sorted(ents.items()):
        notes = list(r["notes"])
        if r["owner_only"]:
            notes.append("Morpho marks this entity ownerOnly: it owns vaults whose "
                         "curation it delegates; it never wins over a set curator")
        srcs = {s["name"].split(" ")[0] for s in r["official_sources"]}
        if len(r["aliases"]) > 1 or len(srcs) > 1:
            notes.append(f"one entity in {sorted(srcs)} under the names "
                         f"{sorted(r['aliases'])}; see names_joined_across_sources in "
                         "curators_validation.json for address confirmation")
        rows.append({"entity_id": eid, "display_name": r["display_name"],
                     "aliases": sorted(r["aliases"]), "addresses": addrs.get(eid, []),
                     "official_sources": r["official_sources"], "notes": notes})
    return rows


def coverage(objs, ivs_by_obj, book, off):
    """Counts and peak-AUM shares per protocol, kind and confidence, judged on the interval in force at each object's peak month."""
    groups = defaultdict(lambda: {"objects": 0, "funded_objects": 0, "peak_usd": 0.0,
                                  "by_confidence": defaultdict(lambda: {
                                      "objects": 0, "peak_usd": 0.0})})
    rules = defaultdict(lambda: {"objects": 0, "peak_usd": 0.0})
    unattributed = []
    for o in objs:
        iv = peak_interval(o, ivs_by_obj[(o["chain_id"], o["vault"])])
        for g in ("all", o["protocol"], f"{o['protocol']}:{o['kind']}"):
            G = groups[g]
            G["objects"] += 1
            G["funded_objects"] += o["peak_usd"] > 0
            G["peak_usd"] += o["peak_usd"]
            c = G["by_confidence"][iv["confidence"]]
            c["objects"] += 1
            c["peak_usd"] += o["peak_usd"]
        rules[iv["rule"] or "none"]["objects"] += 1
        rules[iv["rule"] or "none"]["peak_usd"] += o["peak_usd"]
        if iv["confidence"] in ("unattributed", "name_only") and o["peak_usd"] > 0:
            unattributed.append((o, iv))
    out = {}
    for g, G in sorted(groups.items()):
        out[g] = {"objects": G["objects"], "funded_objects": G["funded_objects"],
                  "peak_usd": round(G["peak_usd"]), "by_confidence": {
                      c: {"objects": v["objects"], "peak_usd": round(v["peak_usd"]),
                          "share_of_peak_usd": round(v["peak_usd"] / G["peak_usd"], 4)
                          if G["peak_usd"] else None}
                      for c, v in sorted(G["by_confidence"].items())}}
    unattributed.sort(key=lambda x: -x[0]["peak_usd"])
    largest = []
    for o, iv in unattributed[:40]:
        states = replay(o["events"])
        roles = {}
        for _, ts, st in states:
            if o["peak_ts"] is None or ts <= o["peak_ts"]:
                roles = st
        controlling = {r: v for r, v in roles.items() if r in ROLE_PRIORITY[o["kind"]]}
        detail = {}
        for r, hs in controlling.items():
            for h in hs:
                f = book.fact(o["chain_id"], h) or {}
                detail[h] = {k: f[k] for k in ("kind", "threshold", "owners",
                                               "contract_name", "owner_fn") if k in f}
                if h in book.ens:
                    detail[h]["ens"] = book.ens[h]
        largest.append({"protocol": o["protocol"], "chain_id": o["chain_id"],
                        "kind": o["kind"], "vault": o["vault"], "name": o["name"],
                        "peak_usd": round(o["peak_usd"]), "confidence": iv["confidence"],
                        "candidate_entity": iv["entity_id"], "roles_at_peak": controlling,
                        "role_holders": detail, "evidence": iv["evidence"]})
    return {"rule": "confidence of the interval in force at the object's peak "
                    "month of net AUM (vault_aum.jsonl usd minus held_by_vaults_usd)",
            "groups": out,
            "objects_by_rule_at_peak": {k: {"objects": v["objects"],
                                            "peak_usd": round(v["peak_usd"])}
                                        for k, v in sorted(rules.items())},
            "address_links_by_rule": dict(sorted(book.rule_counts.items())),
            "address_conflicts": book.conflicts,
            "largest_unattributed_or_name_only": largest}


def stage_attribute(args):
    """Build the address book, the intervals and the three outputs."""
    objs = read_work(args.work, "objects.json")
    facts = read_work(args.work, "address_facts.json")
    off = load_official()
    book = Book(objs, facts, off)
    att = Attributor(book, off)
    ivs_by_obj, rows = {}, []
    for o in objs:
        ivs = att.intervals(o)
        ivs_by_obj[(o["chain_id"], o["vault"])] = ivs
        for iv in ivs:
            rows.append({"protocol": o["protocol"], "chain_id": o["chain_id"],
                         "kind": o["kind"], "vault": o["vault"], "name": o["name"],
                         "from_block": iv["from_block"], "to_block": iv["to_block"],
                         "from_ts": iv["from_ts"], "to_ts": iv["to_ts"],
                         "entity_id": iv["entity_id"], "via_role": iv["via_role"],
                         "via_address": iv["via_address"], "confidence": iv["confidence"],
                         "rule": iv["rule"], "evidence": iv["evidence"]})
    ent_rows = entity_rows(book, off, objs)
    cov = coverage(objs, ivs_by_obj, book, off)
    sources = sorted(off["files"])
    built = now_utc()
    common_meta = {"built_at": built, "official_sources": sources,
                   "euler_labels_commit": off["euler_manifest"]["commit"],
                   "rpc_calls_onchain_stage": facts["rpc_calls"],
                   "etherscan_calls_onchain_stage": facts["etherscan_calls"],
                   "address_facts_built_at": facts["built_at"]}
    io_utils.write_jsonl("registry/curators.jsonl", ent_rows, GENERATOR, dict(
        common_meta, what="Curator entities: canonical id, names, every address "
        "linked to the entity with the rule and evidence, and the official "
        "sources that name it"))
    io_utils.write_jsonl("registry/vault_curators.jsonl", rows, GENERATOR, dict(
        common_meta, what="Attribution intervals per curated object. from_block is "
        "the block of the role change that opens the interval; to_block is the "
        "block of the change that closes it (exclusive), null while current",
        objects=len(objs)))
    io_utils.write_json("registry/curators_coverage.json", cov, GENERATOR, dict(
        common_meta, what="Coverage of the attribution by protocol, kind and "
        "confidence, weighted by peak net AUM"))
    write_work(args.work, "attribution_index.json",
               {f"{c}:{v}": ivs for (c, v), ivs in ivs_by_obj.items()})
    g = cov["groups"]
    for k in ("all", "morpho", "euler_v2", "moolah", "silo_v2"):
        print(k, {c: (v["objects"], v["share_of_peak_usd"])
                  for c, v in g[k]["by_confidence"].items()})
    print("address links by rule", cov["address_links_by_rule"])
    print("conflicts", len(book.conflicts))


def api_roles(v):
    """Current curator and owner as the Morpho API reports them."""
    if "state" in v:
        st = v["state"] or {}
        return {"curator": (st.get("curator") or "").lower(),
                "owner": (st.get("owner") or "").lower()}
    return {"curator": ((v.get("curator") or {}).get("address") or "").lower(),
            "owner": ((v.get("owner") or {}).get("address") or "").lower()}


def compare(label, mine):
    """agree, disagree, name_only_agree, name_only_disagree or unattributed."""
    if mine["confidence"] == "unattributed":
        return "unattributed"
    hit = mine["entity_id"] in label
    if mine["confidence"] == "name_only":
        return "name_only_agree" if hit else "name_only_disagree"
    return "agree" if hit else "disagree"


def mine_detail(iv):
    return {k: iv[k] for k in ("entity_id", "confidence", "via_role", "via_address",
                               "rule", "from_block", "evidence")}


def validate_morpho(objs, idx, off):
    """Our current interval against the Morpho API vault-level labels."""
    raw = json.loads((DATA / SOURCES / "morpho_api_vaults.json").read_text())
    api = {(v["chain"]["id"], v["address"].lower()): v
           for v in raw["vaults"] + raw["vaultV2s"]}
    tally, disagree, unattributed, name_only = Counter(), [], [], []
    by_rule = defaultdict(Counter)
    labelled = 0
    for o in objs:
        if o["protocol"] != "morpho":
            continue
        key = (o["chain_id"], o["vault"])
        lab = off["morpho_vault"].get(key)
        if not lab or not lab["entities"]:
            continue
        labelled += 1
        cur = idx[f"{o['chain_id']}:{o['vault']}"][-1]
        status = compare(lab["entities"], cur)
        tally[status] += 1
        by_rule[cur["rule"] or "none"][status] += 1
        item = {"chain_id": o["chain_id"], "vault": o["vault"], "name": o["name"],
                "peak_usd": round(o["peak_usd"]), "api_entities": lab["entities"],
                "api_roles": api_roles(api[key]), "mine": mine_detail(cur)}
        if status == "disagree":
            disagree.append(item)
        elif status == "unattributed":
            unattributed.append(item)
        elif status.startswith("name_only"):
            name_only.append(dict(item, status=status))
    scope = set(PAIRS["morpho"])
    ours = {(o["chain_id"], o["vault"]) for o in objs if o["protocol"] == "morpho"}
    missing = [k for k, v in off["morpho_vault"].items()
               if k[0] in scope and v["entities"] and k not in ours]
    return {"source": f"data/{SOURCES}/morpho_api_vaults.json",
            "retrieved_at": raw["retrieved_at"],
            "compared": "API vault-level curator labels (V1 state.curators, V2 "
                        "curators) against our current interval",
            "vaults_with_api_label": labelled, "tally": dict(tally),
            "tally_by_our_rule": {k: dict(v) for k, v in sorted(by_rule.items())},
            "independence": "the API derives vault labels by matching the vault's "
                            "current curator and owner against its own curator "
                            "registry, the same registry our official rule uses; "
                            "agreement under other rules is the independent part",
            "agreement_rate_among_attributed": round(
                tally["agree"] / (tally["agree"] + tally["disagree"]), 4)
            if tally["agree"] + tally["disagree"] else None,
            "disagreements": disagree, "left_unattributed": unattributed,
            "name_only": name_only,
            "api_labelled_vaults_not_in_registry": [list(k) for k in missing]}


def validate_euler(objs, idx, off):
    """Our current interval against the governing entity of euler-labels products."""
    tally, disagree, unattributed, name_only = Counter(), [], [], []
    by_rule = defaultdict(Counter)
    ours = {(o["chain_id"], o["vault"]): o for o in objs if o["protocol"] == "euler_v2"}
    not_lending = []
    for key, prod in sorted(off["euler_products"].items()):
        if key[0] not in PAIRS["euler_v2"]:
            continue
        o = ours.get(key)
        if not o:
            not_lending.append(list(key))
            continue
        if not prod["entity"]:
            tally["product_entity_missing"] += 1
            continue
        cur = idx[f"{key[0]}:{key[1]}"][-1]
        status = compare([prod["entity"]], cur)
        tally[status] += 1
        by_rule[cur["rule"] or "none"][status] += 1
        item = {"chain_id": key[0], "vault": key[1], "product": prod["product"],
                "product_entities": prod["entity_raw"], "deprecated": prod["deprecated"],
                "peak_usd": round(o["peak_usd"]), "mine": mine_detail(cur)}
        if status == "disagree":
            disagree.append(item)
        elif status == "unattributed":
            unattributed.append(item)
        elif status.startswith("name_only"):
            name_only.append(dict(item, status=status))
    earn = Counter()
    for key in off["euler_earn_listed"]:
        if key in ours:
            earn[idx[f"{key[0]}:{key[1]}"][-1]["confidence"]] += 1
    return {"source": f"data/{SOURCES}/euler-labels/*/products.json",
            "commit": off["euler_manifest"]["commit"],
            "compared": "product governing entity (first entry of products.json "
                        "'entity') against our current interval for lending vaults",
            "tally": dict(tally),
            "tally_by_our_rule": {k: dict(v) for k, v in sorted(by_rule.items())},
            "agreement_rate_among_attributed": round(
                tally["agree"] / (tally["agree"] + tally["disagree"]), 4)
            if tally["agree"] + tally["disagree"] else None,
            "disagreements": disagree, "left_unattributed": unattributed,
            "name_only": name_only,
            "product_vaults_outside_lending_set": len(not_lending),
            "earn_vaults_listed_by_confidence": dict(earn)}


def sample(objs, idx, seed=20261008):
    """30 attributed objects across protocols with a checkable line each."""
    rng = random.Random(seed)
    quota = {"morpho:morpho_v1": 6, "morpho:morpho_v2": 6, "euler_v2:euler_lending": 5,
             "euler_v2:euler_earn": 4, "moolah:moolah_vault": 4, "silo_v2:silo_vault": 5}
    pools = defaultdict(list)
    for o in objs:
        cur = idx[f"{o['chain_id']}:{o['vault']}"][-1]
        if o["peak_usd"] > 0 and cur["confidence"] in ("official", "cluster"):
            pools[f"{o['protocol']}:{o['kind']}"].append((o, cur))
    picked = []
    for g, n in quota.items():
        pool = sorted(pools[g], key=lambda x: (x[0]["chain_id"], x[0]["vault"]))
        picked += rng.sample(pool, min(n, len(pool)))
    short = 30 - len(picked)
    if short > 0:
        rest = [x for g in sorted(pools) for x in sorted(pools[g], key=lambda x: (
            x[0]["chain_id"], x[0]["vault"])) if x not in picked]
        picked += rng.sample(rest, min(short, len(rest)))
    out = []
    for o, cur in picked:
        out.append({"protocol": o["protocol"], "kind": o["kind"], "chain_id": o["chain_id"],
                    "vault": o["vault"], "name": o["name"], "entity_id": cur["entity_id"],
                    "confidence": cur["confidence"],
                    "justification": f"{cur['via_role']} of {o['vault']} on chain "
                    f"{o['chain_id']} is {cur['via_address']} since block "
                    f"{cur['from_block']}; {cur['evidence']}"})
    return out


def holdout(book):
    """Precision of the clustering rules on addresses whose entity is known
    without them: addresses from an official address list, and addresses
    linked through a Lista or Silo vault listing. Each is treated as
    unknown and the rule's prediction, made only from official anchors and
    never from an anchor at the same address, is compared with the known
    entity. The same-address rule is tested only between Morpho's per-chain
    listings, because euler-labels lists an address once for every chain."""
    address_rules = ("morpho_api_curator_address", "euler_labels_entity_address")
    official = {k: lk for k, lk in book.links.items() if lk["rule"] in address_rules}
    subjects = {k: lk for k, lk in book.links.items()
                if lk["rule"] in address_rules + ("vault_listing_role_holder",)}
    anchors = defaultdict(lambda: defaultdict(set))
    for (cid, a), lk in official.items():
        f = book.fact(cid, a)
        if f and f["kind"] == "safe":
            for s in f["owners"]:
                anchors[s][lk["entity"]].add(a)
    results = defaultdict(lambda: defaultdict(set))
    wrong = []

    def score(group, key, truth, cands, why):
        if not cands:
            results[group]["no_prediction"].add(key)
        elif len(cands) > 1:
            results[group]["ambiguous"].add(key)
        else:
            pred = next(iter(cands))
            results[group]["correct" if pred == truth else "wrong"].add(key)
            if pred != truth:
                wrong.append({"test": group, "chain_id": key[0], "address": key[1],
                              "known": truth, "predicted": pred, "why": why})

    for (cid, a), lk in sorted(subjects.items()):
        f = book.fact(cid, a)
        if not f:
            continue
        src = "listing" if lk["rule"] == "vault_listing_role_holder" else "official"

        def ents_of(signer):
            return {e for e, addrs in anchors.get(signer, {}).items() if addrs - {a}}
        if f["kind"] == "safe":
            by = defaultdict(list)
            for s in f["owners"]:
                es = ents_of(s)
                if len(es) == 1:
                    by[next(iter(es))].append(s)
            need = max(2, -(-len(f["owners"]) // 2))
            cands = {e for e, shared in by.items() if len(shared) >= need}
            score(f"safe_shares_signers on {src} addresses", (cid, a), lk["entity"],
                  cands, {e: by[e] for e in cands})
        elif f["kind"] == "eoa" and a in anchors:
            es = ents_of(a)
            score(f"eoa_signs_entity_safe on {src} addresses", (cid, a), lk["entity"],
                  es, sorted(es))
        elif f["kind"] == "contract":
            cands = {official[(cid, w)]["entity"] for fn in ("owner_fn", "admin0")
                     if (w := f.get(fn)) and w != a and (cid, w) in official}
            if cands:
                score(f"contract_controlled_by_entity on {src} addresses", (cid, a),
                      lk["entity"], cands, sorted(cands))
        if lk["rule"] == "morpho_api_curator_address":
            for (other, b), lk2 in official.items():
                if (b == a and other != cid and lk2["rule"] == lk["rule"]
                        and book.same_account(other, cid, a)):
                    score("same_address_other_chain between Morpho chain listings",
                          (cid, a), lk["entity"], {lk2["entity"]}, f"listed on chain {other}")
                    break
    return {"method": " ".join(holdout.__doc__.split()),
            "by_test": {g: {k: {"chain_address_pairs": len(v),
                                "addresses": len({a for _, a in v})}
                            for k, v in sorted(c.items())}
                        for g, c in sorted(results.items())},
            "wrong": wrong}


def consistency(ent_rows, vc_rows, off):
    """Structural checks on the outputs and on the cited sources."""
    by_key = defaultdict(set)
    by_addr = defaultdict(set)
    for e in ent_rows:
        for a in e["addresses"]:
            by_key[(a["chain_id"], a["address"])].add(e["entity_id"])
            by_addr[a["address"]].add(e["entity_id"])
    multi = [{"chain_id": k[0], "address": k[1], "entities": sorted(v)}
             for k, v in by_key.items() if len(v) > 1]
    multi_chain = [{"address": a, "entities": sorted(v)}
                   for a, v in by_addr.items() if len(v) > 1]
    cited = set(off["files"])
    for e in ent_rows:
        for s in e["official_sources"]:
            cited.add(s["path"].removeprefix("data/"))
        for a in e["addresses"]:
            if a["evidence"] and a["source"] in (
                    "morpho_api_curator_address", "euler_labels_entity_address",
                    "vault_listing_role_holder", "hand_entity_address"):
                cited.add(a["source_path"].removeprefix("data/"))
    missing = sorted(p for p in cited if not (DATA / p).exists())
    ids = {e["entity_id"] for e in ent_rows}
    unknown_ids = sorted({r["entity_id"] for r in vc_rows if r["entity_id"]} - ids)
    gaps = 0
    by_obj = defaultdict(list)
    for r in vc_rows:
        by_obj[(r["chain_id"], r["vault"])].append(r)
    for rows in by_obj.values():
        for a, b in zip(rows, rows[1:]):
            gaps += a["to_block"] != b["from_block"]
        gaps += rows[-1]["to_block"] is not None
    current = {(r["chain_id"], r["vault"]): r for r in vc_rows if r["to_block"] is None}
    address_rules = ("morpho_api_curator_address", "euler_labels_entity_address")
    merges = []
    for e in ent_rows:
        kinds = {s["name"].split(" ")[0] for s in e["official_sources"]}
        if len(kinds) > 1:
            per = defaultdict(set)
            for a in e["addresses"]:
                if a["source"] in address_rules:
                    per[a["source"]].add(a["address"])
            shared = (per["morpho_api_curator_address"] & per["euler_labels_entity_address"])
            listed = [k for src in ("lista", "silo") for k, lst in off[src].items()
                      if e["entity_id"] in lst["entities"]]
            confirmed = [f"{k[0]}:{k[1]}" for k in listed if k in current
                         and current[k]["entity_id"] == e["entity_id"]
                         and current[k]["rule"] in address_rules]
            merges.append({"entity_id": e["entity_id"], "aliases": e["aliases"],
                           "sources": sorted(kinds),
                           "addresses_shared_by_morpho_and_euler": sorted(shared),
                           "listed_vaults": len(listed),
                           "listed_vaults_controlled_by_official_address": confirmed})
    disagreeing_listings = [
        {k: r[k] for k in ("protocol", "chain_id", "vault", "entity_id", "evidence")}
        for r in vc_rows if r["to_block"] is None and "(disagrees)" in r["evidence"]]
    return {"chain_address_to_multiple_entities": multi,
            "address_to_multiple_entities_across_chains": multi_chain,
            "official_source_files_cited": len(cited),
            "official_source_files_missing": missing,
            "entity_ids_missing_from_curators": unknown_ids,
            "interval_chain_breaks": gaps,
            "names_joined_across_sources": merges,
            "listings_disagreeing_with_address_evidence": disagreeing_listings}


def stage_validate(args):
    """Write data/registry/curators_validation.json."""
    objs = read_work(args.work, "objects.json")
    idx = read_work(args.work, "attribution_index.json")
    off = load_official()
    ent_rows = io_utils.read_jsonl("registry/curators.jsonl")
    vc_rows = io_utils.read_jsonl("registry/vault_curators.jsonl")
    book = Book(objs, read_work(args.work, "address_facts.json"), off)
    out = {"morpho_api": validate_morpho(objs, idx, off),
           "euler_labels": validate_euler(objs, idx, off),
           "cluster_rules_on_known_addresses": holdout(book),
           "random_sample": sample(objs, idx),
           "consistency": consistency(ent_rows, vc_rows, off)}
    io_utils.write_json("registry/curators_validation.json", out, GENERATOR, {
        "what": "Validation of the curator attribution against vault-level official "
                "labels (Morpho API, euler-labels products), a random sample with "
                "justifications, and consistency checks",
        "built_at": now_utc()})
    for k in ("morpho_api", "euler_labels"):
        print(k, out[k]["tally"], len(out[k]["disagreements"]))
    for g, c in out["cluster_rules_on_known_addresses"]["by_test"].items():
        print("holdout", g, {k: v["chain_address_pairs"] for k, v in c.items()})
    c = out["consistency"]
    print("multi-entity addresses", len(c["chain_address_to_multiple_entities"]),
          "missing files", c["official_source_files_missing"],
          "unknown ids", c["entity_ids_missing_from_curators"],
          "interval breaks", c["interval_chain_breaks"])


STAGES = {"fetch": stage_fetch, "objects": stage_objects, "onchain": stage_onchain,
          "attribute": stage_attribute, "validate": stage_validate}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--stage", choices=["all", *STAGES], default="all")
    ap.add_argument("--work", default="/tmp/curators_work")
    ap.add_argument("--euler-ref", default="master",
                    help="euler-labels branch or commit to snapshot (default master, "
                         "the branch the labels are merged into)")
    args = ap.parse_args()
    Path(args.work).mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    for name, fn in STAGES.items():
        if args.stage in ("all", name):
            fn(args)
    print(f"done in {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main()
