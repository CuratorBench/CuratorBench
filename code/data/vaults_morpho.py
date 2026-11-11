# /// script
# requires-python = ">=3.11"
# dependencies = ["hypersync==1.2.0", "eth-abi>=5.0", "eth-hash[pycryptodome]", "eth-utils>=4.0", "requests>=2.31"]
# ///
"""Reconstruct curator vault histories from verified ABIs and HyperSync logs."""
import argparse
import csv
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import random
import re
import sys
import tempfile
import threading
import time
import traceback
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'code'))
from common import env, evmabi, hypersync_client as hs, io_utils, logs, multicall, rpc

GENERATOR = 'code/data/vaults_morpho.py'
ZERO = '0x' + '0' * 40
PREFETCH_WORKERS = 1
FETCH_BLOCKS = {'vault': 20000, 'core': 25000, 'wrapper_transfers': 10000,
                'wrapper_from': 250000, 'wrapper_to': 250000}
SCRATCH = Path(tempfile.gettempdir()) / 'vaults_morpho'


def atomic(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    io_utils._atomic_text(path, json.dumps(obj, indent=2, sort_keys=True) + '\n')


def scratch_file(name):
    """Rate and writer-lock files shared by concurrent processes."""
    SCRATCH.mkdir(parents=True, exist_ok=True)
    return SCRATCH / name


def explorer_wait():
    """Share the explorer rate limit across concurrent chain processes."""
    with scratch_file('explorer_rate').open('a+') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.seek(0)
        previous = float(f.read() or 0)
        delay = previous + 0.36 - time.time()
        if delay > 0:
            time.sleep(delay)
        f.seek(0)
        f.truncate()
        f.write(str(time.time()))
        f.flush()


def hypersync_wait():
    """Pace concurrent page requests across chain processes to avoid retry storms."""
    with scratch_file('hypersync_rate').open('a+') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.seek(0)
        previous = float(f.read() or 0)
        delay = previous + 0.25 - time.time()
        if delay > 0:
            time.sleep(min(delay, 1))
        f.seek(0)
        f.truncate()
        f.write(str(time.time()))
        f.flush()


def source(cid, address):
    """Cache the explorer response and its exact source units without credentials."""
    address = address.lower()
    d = ROOT / 'data/contracts' / f'{cid}-{address}'
    cache = d / '_explorer.json'
    if cache.exists():
        item = json.loads(cache.read_text())
    else:
        item = None
        for attempt in range(6):
            try:
                explorer_wait()
                response = requests.get('https://api.etherscan.io/v2/api', params={
                    'chainid': cid, 'module': 'contract', 'action': 'getsourcecode',
                    'address': address, 'apikey': env.require('ETHERSCAN_API_KEY')}, timeout=60)
                body = response.json()
                if body.get('status') == '1':
                    item = body['result'][0]
                    break
            except (requests.RequestException, ValueError):
                pass
            time.sleep(1 + attempt)
        if item is None:
            raise RuntimeError(f'explorer source request failed: {cid}:{address}')
        d.mkdir(parents=True, exist_ok=True)
        atomic(cache, item)
        raw = item.get('SourceCode', '')
        if raw:
            if raw.startswith('{{'):
                parsed = json.loads(raw[1:-1])
                files = parsed['sources']
            elif raw.startswith('{'):
                parsed = json.loads(raw)
                files = parsed.get('sources', parsed)
            else:
                files = {item['ContractName'] + '.sol': raw}
            for name, unit in files.items():
                dest = d / name.replace('/', '__').replace('\\', '__')
                if not dest.exists():
                    io_utils._atomic_text(dest, unit['content'] if isinstance(unit, dict) else unit)
            if not (d / '_meta.json').exists():
                atomic(d / '_meta.json', {'contract_name': item['ContractName'],
                    'compiler': item['CompilerVersion'], 'n_files': len(files),
                    'chain_id': int(cid), 'address': address,
                    'source': 'Etherscan v2 getsourcecode'})
    if not (d / '_meta.json').exists():
        atomic(d / '_meta.json', {'contract_name': item.get('ContractName'), 'chain_id': int(cid),
               'address': address, 'verified': bool(item.get('SourceCode')), 'source': 'Etherscan v2 getsourcecode'})
    abi = json.loads(item['ABI']) if item.get('ABI', '').startswith('[') else []
    return {'address': address, 'name': item.get('ContractName'),
            'implementation': item.get('Implementation'), 'abi': abi,
            'path': str(d.relative_to(ROOT))}


def typ(p):
    return '(' + ','.join(typ(c) for c in p['components']) + ')' + p['type'][5:] if p['type'].startswith('tuple') else p['type']


def signature(e):
    return e['name'] + '(' + ','.join(typ(p) for p in e['inputs']) + ')'


def setup(protocol, cid):
    cfg = logs.load_chains()[cid]
    entry = json.loads((ROOT / 'code/config/deployments.json').read_text())['protocols'][protocol]['chains'][cid]
    vaults = io_utils.read_jsonl(f'registry/vaults/{protocol}/{cid}.jsonl')
    markets = io_utils.read_jsonl(f'registry/markets/{protocol}/{cid}.jsonl')
    meta = json.loads((ROOT / f'data/registry/vaults/{protocol}/{cid}.jsonl.meta.json').read_text())
    return cfg, entry[protocol].lower(), {v['vault']: v for v in vaults}, {m['market_id']: m for m in markets}, meta['scanned_to_block']


EXPLORER_SETTINGS = ('CompilerVersion', 'OptimizationUsed', 'Runs', 'EVMVersion')


def unit_hashes(path):
    return sorted(hashlib.sha256(p.read_bytes()).hexdigest() for p in path.glob('*.sol'))


def library_events(protocol, cid, entry):
    """Supply core event layouts that the explorer ABI omits."""
    if any(e['type'] == 'event' for e in entry['abi']):
        return entry
    local = ROOT / entry['path']
    item = json.loads((local / '_explorer.json').read_text())
    chains = json.loads((ROOT / 'code/config/deployments.json').read_text())['protocols'][protocol]['chains']
    for other in sorted(chains, key=int):
        ref = ROOT / 'data/contracts' / f'{other}-{chains[other][protocol].lower()}'
        if other == cid or not (ref / '_explorer.json').exists():
            continue
        ref_item = json.loads((ref / '_explorer.json').read_text())
        abi = json.loads(ref_item['ABI']) if ref_item.get('ABI', '').startswith('[') else []
        events = [e for e in abi if e['type'] == 'event']
        if (events and unit_hashes(local) and unit_hashes(ref) == unit_hashes(local)
                and all(ref_item.get(k) == item.get(k) for k in EXPLORER_SETTINGS)):
            return {**entry, 'abi': entry['abi'] + events, 'event_abi_source': str(ref.relative_to(ROOT)),
                    'event_abi_evidence': 'explorer ABI omits library events; verified source units (sha256) '
                                          'and compiler settings equal those of the reference deployment'}
    raise RuntimeError(f'{cid}:{entry["address"]} ABI has no events and no identical verified deployment lists them')


def bootstrap(protocol, cid, d):
    cfg, core, vaults, markets, head = setup(protocol, cid)
    reps = {}
    for v in vaults.values():
        key = (v['kind'], v['factory'])
        reps.setdefault(key, v['vault'])
    addresses = [core] + list(reps.values())
    manifest = json.loads((d / 'sources.json').read_text()) if (d / 'sources.json').exists() else {}
    for a in addresses:
        s = source(cid, a)
        manifest[a] = s
        if s['implementation']:
            imp = source(cid, s['implementation'])
            manifest[imp['address']] = imp
        print(f"source {cid}:{a} {s['name']} events={sum(x['type']=='event' for x in s['abi'])}", flush=True)
    manifest[core] = library_events(protocol, cid, manifest[core])
    atomic(d / 'sources.json', manifest)


def clean(value):
    if isinstance(value, bytes):
        return '0x' + value.hex()
    if isinstance(value, (list, tuple)):
        return [clean(x) for x in value]
    return value


_EVENT_PLANS = {}


def decode_word(tp, word):
    """Decode fixed ABI words; the types always come from a verified ABI."""
    if len(word) != 64:
        raise ValueError('ABI word length')
    if tp == 'address':
        if int(word[:24], 16):
            raise ValueError('address padding')
        return '0x' + word[24:].lower()
    if tp == 'bool':
        value = int(word, 16)
        if value not in (0, 1):
            raise ValueError('boolean value')
        return bool(value)
    if tp.startswith('uint'):
        value = int(word, 16)
        if value >= 1 << int(tp[4:] or '256'):
            raise ValueError('unsigned integer range')
        return value
    if tp.startswith('int'):
        value = int(word, 16)
        if value >= 1 << 255:
            value -= 1 << 256
        bits = int(tp[3:] or '256')
        if not -(1 << (bits - 1)) <= value < 1 << (bits - 1):
            raise ValueError('signed integer range')
        return value
    size = int(tp[5:])
    if int(word[2 * size:] or '0', 16):
        raise ValueError('bytes padding')
    return '0x' + word[:2 * size].lower()


def decode_event(e, lg):
    plan = _EVENT_PLANS.get(id(e))
    if plan is None:
        indexed = [(x['name'], typ(x)) for x in e['inputs'] if x['indexed']]
        plain = [(x['name'], typ(x)) for x in e['inputs'] if not x['indexed']]
        fixed = all(re.fullmatch(r'(address|bool|u?int[0-9]+|bytes[0-9]+)', tp) for _, tp in plain)
        plan = _EVENT_PLANS[id(e)] = (e, indexed, plain, fixed)
    _, indexed, plain, fixed = plan
    if len(indexed) != len(lg['topics']) - 1:
        raise ValueError('indexed field count')
    if fixed:
        data = lg['data'].removeprefix('0x')
        if len(data) != 64 * len(plain):
            raise ValueError('static ABI data length')
        result = {name: decode_word(tp, data[i * 64:(i + 1) * 64]) for i, (name, tp) in enumerate(plain)}
    else:
        vals = evmabi.decode([tp for _, tp in plain], lg['data'])
        result = {name: clean(v) for (name, _), v in zip(plain, vals)}
    for (name, tp), topic in zip(indexed, lg['topics'][1:]):
        result[name] = topic if ('[' in tp or tp in ('bytes', 'string') or tp.startswith('(')) else decode_word(tp, topic.removeprefix('0x'))
    return result


class Decoder:
    def __init__(self, cid, d, vaults):
        self.cid, self.d, self.vaults = cid, d, vaults
        self.sources = json.loads((d / 'sources.json').read_text())
        self.by_topic = defaultdict(list)
        self.preferred = {}
        for a, s in self.sources.items():
            self.add(s)
            if a in vaults:
                v = vaults[a]
                abi = self.sources.get((s['implementation'] or '').lower(), s)['abi']
                self.preferred[(v['kind'], v['factory'])] = {evmabi.topic0(signature(e)): e for e in abi if e['type'] == 'event'}
        self.fetched = set()

    def add(self, s):
        for e in s['abi']:
            if e['type'] == 'event':
                t = evmabi.topic0(signature(e))
                if e not in self.by_topic[t]:
                    self.by_topic[t].append(e)

    def decode(self, lg):
        a, topic = lg['address'].lower(), lg['topics'][0]
        v = self.vaults[a]
        preferred = self.preferred.get((v['kind'], v['factory']), {}).get(topic)
        candidates = ([preferred] if preferred else []) + self.by_topic.get(topic, [])
        for e in candidates:
            try:
                return e['name'], decode_event(e, lg)
            except Exception:
                continue
        if a not in self.fetched:
            self.fetched.add(a)
            s = source(self.cid, a)
            self.sources[a] = s
            self.add(s)
            if s['implementation']:
                imp = source(self.cid, s['implementation'])
                self.sources[imp['address']] = imp
                self.add(imp)
            atomic(self.d / 'sources.json', self.sources)
            return self.decode(lg)
        return None, None


class Stream:
    """Append outputs with durable page boundaries, retaining a partial last block."""
    def __init__(self, d, stage, head, initial=None):
        self.d, self.stage, self.head = d, stage, head
        d.mkdir(parents=True, exist_ok=True)
        key = hashlib.sha256(str(d.resolve()).encode()).hexdigest()[:16]
        self.writer_lock = scratch_file(f'{key}-{stage}.lock').open('a+')
        try:
            fcntl.flock(self.writer_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.writer_lock.close()
            raise RuntimeError(f'Another writer owns the {stage} checkpoint') from None
        self.path = d / f'_{stage}_checkpoint.json'
        self.ck = json.loads(self.path.read_text()) if self.path.exists() else {
            'next_block': 0, 'to_block': head, 'done': False, 'bytes': {},
            'counts': {}, 'state': initial or {}, 'pages': 0, 'logs': 0}
        if self.ck['to_block'] != head:
            raise RuntimeError('Registry head changed; use a distinct output directory')
        for name, size in self.ck['bytes'].items():
            path = d / name
            if not path.exists() or path.stat().st_size < size:
                raise RuntimeError(f'checkpoint output missing or short: {path}')
            with path.open('r+b') as f:
                f.truncate(size)
        self.buffer = defaultdict(list)
        self.pending = []
        self.last_print = 0

    def emit(self, name, row, fields=None):
        if name not in self.ck['bytes'] and name not in self.buffer and fields:
            self.buffer[name].append(','.join(fields) + '\n')
        if fields:
            out = io.StringIO()
            csv.writer(out, lineterminator='\n').writerow([row.get(k, '') for k in fields])
            self.buffer[name].append(out.getvalue())
        else:
            self.buffer[name].append(json.dumps(row, separators=(',', ':')) + '\n')
        self.ck['counts'][name] = self.ck['counts'].get(name, 0) + 1

    def flush(self, next_block, done=False):
        for name, lines in self.buffer.items():
            p = self.d / name
            p.parent.mkdir(parents=True, exist_ok=True)
            mode = 'ab' if name in self.ck['bytes'] else 'wb'
            with p.open(mode) as f:
                f.write(''.join(lines).encode())
                f.flush()
            self.ck['bytes'][name] = p.stat().st_size
        self.buffer.clear()
        self.ck['next_block'], self.ck['done'] = next_block, done
        atomic(self.path, self.ck)

    def run(self, slug, addresses, topics, handler, selections=None):
        try:
            self._run(slug, addresses, topics, handler, selections)
        finally:
            self.writer_lock.close()

    def _run(self, slug, addresses, topics, handler, selections=None):
        if self.ck['done']:
            print(f'{self.stage}: already complete', flush=True)
            return
        def page(logs_page):
            ordered = sorted(self.pending + logs_page, key=lambda l: (l['block_number'], l['log_index']))
            boundary = ordered[-1]['block_number']
            self.pending = []
            for lg in ordered:
                if lg['block_number'] == boundary:
                    self.pending.append(lg)
                else:
                    handler(lg, self)
                    self.ck['logs'] += 1
            self.ck['pages'] += 1
            self.flush(boundary)
            if time.monotonic() - self.last_print > 15:
                print(f'{self.stage}: block={boundary:,}/{self.head:,} logs={self.ck["logs"]:,} pages={self.ck["pages"]}', flush=True)
                self.last_print = time.monotonic()
        if selections is None and (PREFETCH_WORKERS == 1 or self.stage not in FETCH_BLOCKS):
            hs.scan_logs_stream(slug, addresses=addresses, topics=topics,
                                from_block=self.ck['next_block'], to_block=self.head, on_page=page)
        else:
            width = FETCH_BLOCKS[self.stage]
            ranges = iter((lo, min(lo + width, self.head)) for lo in range(self.ck['next_block'], self.head, width))
            def fetch(bounds):
                pages = []
                def collect(batch):
                    pages.append(batch)
                    hypersync_wait()
                for addrs, filter_topics in selections or [(addresses, topics)]:
                    hypersync_wait()
                    hs.scan_logs_stream(slug, addresses=addrs, topics=filter_topics,
                                        from_block=bounds[0], to_block=bounds[1], on_page=collect)
                if selections is not None:
                    ordered = sorted((lg for batch in pages for lg in batch), key=lambda lg: (lg['block_number'], lg['log_index']))
                    return [ordered[i:i + 5000] for i in range(0, len(ordered), 5000)]
                return pages
            with ThreadPoolExecutor(max_workers=PREFETCH_WORKERS) as pool:
                pending = deque()
                for bounds in ranges:
                    pending.append(pool.submit(fetch, bounds))
                    if len(pending) == PREFETCH_WORKERS:
                        break
                while pending:
                    for fetched_page in pending.popleft().result():
                        page(fetched_page)
                    bounds = next(ranges, None)
                    if bounds is not None:
                        pending.append(pool.submit(fetch, bounds))
        for lg in self.pending:
            handler(lg, self)
            self.ck['logs'] += 1
        self.pending = []
        self.flush(self.head, done=True)
        print(f'{self.stage}: complete logs={self.ck["logs"]:,}', flush=True)


FLOW_FIELDS = ['block', 'log_index', 'timestamp', 'tx_hash', 'kind', 'sender', 'owner', 'receiver', 'assets', 'shares']
FEE_FIELDS = ['vault', 'block', 'log_index', 'timestamp', 'tx_hash', 'event', 'newTotalAssets', 'feeShares', 'previousTotalAssets', 'performanceFeeShares', 'managementFeeShares', 'fields']
POSITION_FIELDS = ['block', 'log_index', 'timestamp', 'tx_hash', 'market_id', 'kind', 'assets', 'shares_delta', 'cum_shares', 'adapter', 'vault_cum_shares']
RAW_FIELDS = POSITION_FIELDS[:8] + ['adapter']
IGNORED = {'Transfer', 'Approval', 'AllowanceUpdatedByTransferFrom', 'Permit'}


def event_base(lg):
    return {'vault': lg['address'].lower(), 'block': lg['block_number'],
            'log_index': lg['log_index'], 'timestamp': lg['timestamp'], 'tx_hash': lg['tx_hash']}


def adapter_registration(intervals, vault, adapter, added, block, log_index):
    """Replay membership, including setters that emit on idempotent calls."""
    active = [r for r in intervals if r['vault'] == vault and r['adapter'] == adapter
              and r['removed_block'] is None]
    if added and not active:
        intervals.append({'vault': vault, 'adapter': adapter, 'added_block': block,
                          'added_log_index': log_index, 'removed_block': None})
        return True
    if not added and active:
        for row in active:
            row['removed_block'] = block
            row['removed_log_index'] = log_index
        return True
    return False


def role_change(stream, base, role, address, added, event, single=False):
    roles = stream.ck['state']['roles'].setdefault(base['vault'], {})
    members = roles.setdefault(role, [])
    def emit(a, add):
        stream.emit('roles.jsonl', {**base, 'role': role, 'address': a,
                    'added': add, 'change': 'added' if add else 'removed', 'event': event})
    if single:
        for old in members[:]:
            if old != address:
                emit(old, False)
                members.remove(old)
    if address == ZERO and single:
        return
    emit(address, added)
    if added and address not in members:
        members.append(address)
        members.sort()
    elif not added and address in members:
        members.remove(address)


def vault_selections(cid, d, vaults, decoder):
    """Omit ERC20 bookkeeping only for immutable, source-verified factory families."""
    filtered, proofs, selected_topics = set(), [], set()
    families = sorted({v['factory'] for v in vaults.values() if v.get('factory')})
    for factory in families:
        src = source(cid, factory)
        if src['implementation'] or not src['abi']:
            continue
        files = [p for p in (ROOT / src['path']).glob('*.sol') if p.name == src['name'] + '.sol' or p.name.endswith('__' + src['name'] + '.sol')]
        bodies = '\n'.join(p.read_text() for p in files)
        kinds = set(re.findall(r'new\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:\{[^}]*\})?\s*\(', bodies))
        if len(kinds) != 1 or not kinds <= {'MetaMorpho', 'MetaMorphoV1_1', 'VaultV2'}:
            continue
        references = [r for a, r in decoder.sources.items() if a in vaults and vaults[a]['factory'] == factory and r['name'] in kinds and r['abi']]
        if not references:
            continue
        addresses = {a for a, v in vaults.items() if v['factory'] == factory}
        filtered.update(addresses)
        for reference in references:
            selected_topics.update(
                evmabi.topic0(signature(event)) for event in reference['abi']
                if event['type'] == 'event' and event['name'] not in IGNORED)
        proofs.append({'factory': factory, 'source': src['path'], 'factory_source_files': [str(p.relative_to(ROOT)) for p in files], 'child_type': next(iter(kinds)), 'child_abi_sources': [r['path'] for r in references], 'vaults': len(addresses)})
    topics = sorted(selected_topics)
    other = sorted(set(vaults) - filtered)
    selections = []
    if filtered:
        selections.append((sorted(filtered), [topics]))
    if other:
        selections.append((other, None))
    atomic(d / 'vault_selections.json', {'factory_proofs': proofs, 'omitted_events': sorted(IGNORED),
        'filtered_vaults': len(filtered), 'all_topic_vaults': len(other), 'all_topic_addresses': other,
        'selected_topics': sorted(topics), 'note': 'All ABI events other than ERC20 bookkeeping for immutable verified factory families; all topics for remaining vaults.'})
    return selections


def vault_scan(protocol, cid, d):
    cfg, core, vaults, markets, head = setup(protocol, cid)
    decoder = Decoder(cid, d, vaults)
    ignored_topics = {t for t, es in decoder.by_topic.items() if all(e['name'] in IGNORED for e in es)}
    stream = Stream(d, 'vault', head, {'roles': {}, 'adapters': [], 'event_counts': {}, 'unknown': {}, 'first': {}, 'last': {}})
    role_ids = {evmabi.topic0(x): x for x in ['MANAGER', 'CURATOR', 'ALLOCATOR', 'BOT']}
    role_ids['0x' + '00' * 32] = 'DEFAULT_ADMIN_ROLE'
    def handle(lg, s):
        if lg['topics'][0] in ignored_topics:
            return
        name, f = decoder.decode(lg)
        base = event_base(lg)
        v = base['vault']
        st = s.ck['state']
        st['first'].setdefault(v, base['block'])
        st['last'][v] = base['block']
        counts = st['event_counts'].setdefault(v, {})
        counts[name or lg['topics'][0]] = counts.get(name or lg['topics'][0], 0) + 1
        if not name:
            st['unknown'][lg['topics'][0]] = st['unknown'].get(lg['topics'][0], 0) + 1
            s.emit('unknown_events.jsonl', lg)
            return
        if name in ('Deposit', 'Withdraw'):
            s.emit(f'flows/{v}.csv', {**base, **f, 'owner': f.get('owner', f.get('onBehalf', '')), 'kind': name.lower()}, FLOW_FIELDS)
            return
        if name == 'AccrueInterest':
            s.emit('fees.csv', {**base, **f, 'event': name, 'fields': json.dumps(f, separators=(',', ':'))}, FEE_FIELDS)
            return
        s.emit('actions.jsonl', {**base, 'event': name, 'fields': f})
        if name in ('OwnershipTransferred', 'SetOwner', 'Constructor'):
            a = f.get('newOwner', f.get('owner'))
            if a is not None:
                role_change(s, base, 'owner', a, True, name, True)
            if name == 'OwnershipTransferred':
                role_change(s, base, 'pending_owner', ZERO, False, name, True)
        elif name == 'OwnershipTransferStarted':
            role_change(s, base, 'pending_owner', f['newOwner'], True, name, True)
        elif name == 'SetCurator':
            role_change(s, base, 'curator', f['newCurator'], True, name, True)
        elif name == 'SetGuardian':
            role_change(s, base, 'guardian', f.get('guardian', f.get('newGuardian')), True, name, True)
            role_change(s, base, 'pending_guardian', ZERO, False, name, True)
        elif name == 'SubmitGuardian':
            role_change(s, base, 'pending_guardian', f['newGuardian'], True, name, True)
        elif name == 'RevokePendingGuardian':
            role_change(s, base, 'pending_guardian', ZERO, False, name, True)
        elif name in ('SetIsAllocator', 'SetIsSentinel'):
            role = 'allocator' if name == 'SetIsAllocator' else 'sentinel'
            account = f.get('account', f.get('allocator'))
            enabled = next(val for val in f.values() if isinstance(val, bool))
            role_change(s, base, role, account, enabled, name)
        elif name in ('RoleGranted', 'RoleRevoked'):
            role_change(s, base, role_ids.get(f['role'], f['role']), f['account'], name == 'RoleGranted', name)
        if name in ('AddAdapter', 'RemoveAdapter', 'SetIsAdapter'):
            a = f.get('account', f.get('adapter'))
            add = name == 'AddAdapter' or (name == 'SetIsAdapter' and f['newIsAdapter'])
            adapter_registration(st['adapters'], v, a, add, base['block'], base['log_index'])
        if name == 'Upgraded':
            imp = source(cid, f['implementation'])
            decoder.sources[imp['address']] = imp
            decoder.add(imp)
            atomic(d / 'sources.json', decoder.sources)
    selections = vault_selections(cid, d, vaults, decoder) if protocol == 'morpho' else None
    if stream.ck['next_block'] == 0:
        first = hs.first_log(cfg['hypersync'], addresses=list(vaults), to_block=head)
        stream.flush(first['block_number'] if first else head)
    stream.run(cfg['hypersync'], list(vaults), None, handle, selections=selections)


def estimate(protocol, cid, d):
    path = d / 'estimate.json'
    if path.exists():
        print('estimate: ' + json.dumps(json.loads(path.read_text())), flush=True)
        return
    cfg, core, vaults, markets, head = setup(protocol, cid)
    dec = Decoder(cid, d, vaults)
    core_abi = dec.sources[core]
    core_abi = dec.sources.get((core_abi['implementation'] or '').lower(), core_abi)
    topics = [evmabi.topic0(signature(e)) for e in core_abi['abi'] if e['type'] == 'event' and e['name'] in ['Supply', 'Withdraw']]
    lo = min(v['created_block'] for v in vaults.values())
    span = head - lo
    width = max(10, span // 1000)
    hits = Counter()
    for i in range(5):
        start = lo + span * (2 * i + 1) // 10
        cr, _ = hs.scan_logs(cfg['hypersync'], addresses=[core], topics=[topics], from_block=start, to_block=start + width)
        vr, _ = hs.scan_logs(cfg['hypersync'], addresses=list(vaults), from_block=start, to_block=start + width)
        hits['core_supply_withdraw'] += len(cr)
        hits['vault_all_logs'] += len(vr)
        hits['direct_position_logs'] += sum(evmabi.addr(lg['topics'][3 if lg['topics'][0] == topics[0] else 2]) in vaults for lg in cr)
        print(f'estimate window {i+1}/5: core={len(cr)} vault={len(vr)}', flush=True)
    factor = span / (5 * width)
    result = {'method': 'five evenly spaced windows, 0.5% of block span; activity is nonstationary',
              'vaults': len(vaults), 'from_block': lo, 'to_block_exclusive': head,
              'estimated_events': {k: round(v * factor) for k, v in hits.items()},
              'estimated_output_bytes': round((hits['vault_all_logs'] * 250 + hits['core_supply_withdraw'] * 350) * factor),
              'size_note': 'conservative bound using all core events before vault and adapter filtering'}
    atomic(path, result)
    print('estimate: ' + json.dumps(result), flush=True)


def discover_adapters(protocol, cid, d):
    """Scan the small adapter event stream independently of the large fee stream."""
    cfg, core, vaults, markets, head = setup(protocol, cid)
    decoder = Decoder(cid, d, vaults)
    addresses = [v for v, r in vaults.items() if r['kind'] == 'morpho_v2']
    stream = Stream(d, 'adapter_discovery', head, {'adapters': []})
    if not addresses:
        stream.flush(head, True)
        return
    topics = [t for t, es in decoder.by_topic.items() if any(e['name'] in ('AddAdapter', 'RemoveAdapter', 'SetIsAdapter') for e in es)]
    def handle(lg, st):
        name, f = decoder.decode(lg)
        if name is None:
            raise RuntimeError('Adapter event could not be decoded')
        v, a = lg['address'].lower(), f.get('account', f.get('adapter'))
        add = name == 'AddAdapter' or (name == 'SetIsAdapter' and f['newIsAdapter'])
        adapter_registration(st.ck['state']['adapters'], v, a, add, lg['block_number'], lg['log_index'])
    stream.run(cfg['hypersync'], addresses, [topics], handle)


def normalize_adapters(protocol, cid, d):
    """Repair inherited interval checkpoints without fetching or discarding logs."""
    vault_path = d / '_vault_checkpoint.json'
    discovery_path = d / '_adapter_discovery_checkpoint.json'
    vault_ck, discovery = json.loads(vault_path.read_text()), json.loads(discovery_path.read_text())
    if not vault_ck['done'] or not discovery['done']:
        raise RuntimeError('Both vault and discovery scans must finish before adapter normalization')
    original = d / 'adapter_registration_records.jsonl'
    if not original.exists():
        io_utils._atomic_text(original, (d / 'adapters.jsonl').read_text())
    intervals, seen, noops, names = [], set(), 0, Counter()
    pattern = re.compile(rb'"event":"(?:AddAdapter|RemoveAdapter|SetIsAdapter)"')
    ledger = d / 'adapter_registration_events.jsonl'
    with (d / 'actions.jsonl').open('rb') as src, ledger.with_suffix('.jsonl.tmp').open('w') as output:
        for line in src:
            if not pattern.search(line):
                continue
            row = json.loads(line)
            name, f = row['event'], row['fields']
            a = f.get('account', f.get('adapter'))
            add = name == 'AddAdapter' or name == 'SetIsAdapter' and f['newIsAdapter']
            changed = adapter_registration(intervals, row['vault'], a, add, row['block'], row['log_index'])
            seen.add((row['vault'], a))
            names[name] += 1
            noops += not changed
            output.write(json.dumps({**row, 'state_changed': changed}, separators=(',', ':')) + '\n')
    if sum(names.values()) != discovery['logs']:
        raise RuntimeError('Adapter event count differs between full vault and discovery scans')
    details = {(r['vault'], r['adapter']): r for r in
               (json.loads(line) for line in original.read_text().splitlines())}
    if seen != set(details):
        raise RuntimeError('Adapter account set changed during normalization')
    os.replace(ledger.with_suffix('.jsonl.tmp'), ledger)
    boundary_fields = {'vault', 'adapter', 'added_block', 'added_log_index', 'removed_block', 'removed_log_index'}
    rows = [{**{k: val for k, val in details[r['vault'], r['adapter']].items() if k not in boundary_fields}, **r} for r in intervals]
    io_utils.write_jsonl(str((d / 'adapters.jsonl').relative_to(ROOT / 'data')), rows, GENERATOR,
                         {'head': vault_ck['to_block'] - 1, 'registration_events': sum(names.values()), 'no_op_events': noops})
    for path, ck in [(vault_path, vault_ck), (discovery_path, discovery)]:
        ck['state'].setdefault('adapter_registration_records_before_normalization', ck['state']['adapters'])
        ck['state']['adapters'] = intervals
        ck['adapter_intervals_normalized'] = True
        atomic(path, ck)
    result = {'events': sum(names.values()), 'events_by_name': dict(names), 'no_op_events': noops,
              'intervals': len(intervals), 'distinct_adapters': len({r['adapter'] for r in rows}),
              'source': 'actions.jsonl; independent _adapter_discovery_checkpoint.json event count',
              'original_records': original.name, 'event_ledger': ledger.name,
              'note': 'AddAdapter/RemoveAdapter and SetIsAdapter may emit even when membership is unchanged.'}
    atomic(d / 'adapter_normalization.json', result)
    print('adapter normalization: ' + json.dumps(result), flush=True)


def adapter_factories(protocol, cid, d):
    """Prove adapter types from verified factory code and its creation events."""
    cfg, core, vaults, markets, head = setup(protocol, cid)
    discovery = json.loads((d / '_adapter_discovery_checkpoint.json').read_text())
    if not discovery['done']:
        raise RuntimeError('Adapter discovery incomplete')
    addresses = sorted({r['adapter'] for r in discovery['state']['adapters']})
    cache = d / '_adapter_factories.json'
    if cache.exists():
        factories = json.loads(cache.read_text())
    else:
        got = multicall.try_view(logs.rpc_target(cfg), addresses, 'factory()', ['address'], block=head - 1)
        factories = {a: r[0] if r else None for a, r in zip(addresses, got)}
        atomic(cache, factories)
    evidence = {}
    eligible = {}
    for factory in sorted({f for f in factories.values() if f and f != ZERO}):
        src = source(cid, factory)
        if src['implementation'] or not src['abi']:
            continue
        files = [p for p in (ROOT / src['path']).glob('*.sol') if p.name == src['name'] + '.sol' or p.name.endswith('__' + src['name'] + '.sol')]
        constructed = set()
        for p in files:
            constructed.update(re.findall(r'new\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:\{[^}]*\})?\s*\(', p.read_text()))
        if len(constructed) != 1:
            continue
        target_type = next(iter(constructed))
        if target_type not in ('MorphoVaultV1Adapter', 'MorphoMarketV1Adapter', 'MorphoMarketV1AdapterV2'):
            continue
        events = {evmabi.topic0(signature(e)): e for e in src['abi'] if e['type'] == 'event' and e['name'].startswith('Create')}
        if events:
            eligible[factory] = {'type': target_type, 'source': src['path'], 'events': events, 'factory_source_files': [str(p.relative_to(ROOT)) for p in files]}
        print(f'factory {factory}: {src["name"]} constructs {target_type}', flush=True)
    atomic(d / 'factory_sources.json', eligible)
    if not eligible:
        atomic(d / '_factory_proofs.json', {})
        return
    stream = Stream(d, 'factory_events', head, {'proofs': {}})
    topics = sorted({t for x in eligible.values() for t in x['events']})
    def handle(lg, st):
        f = lg['address'].lower()
        spec = eligible[f]
        if lg['topics'][0] not in spec['events']:
            return
        e = spec['events'][lg['topics'][0]]
        values = decode_event(e, lg)
        for a in values.values():
            if isinstance(a, str) and a in factories and factories[a] == f:
                st.ck['state']['proofs'][a] = {'type': spec['type'], 'source': spec['source'],
                    'factory': f, 'creation_event': e['name'], 'creation_fields': values,
                    'creation_block': lg['block_number'], 'creation_tx': lg['tx_hash'],
                    'factory_source_files': spec['factory_source_files'],
                    'type_evidence': 'verified factory has one concrete new-contract type; creation event and adapter factory() agree'}
    stream.run(cfg['hypersync'], list(eligible), [topics], handle)
    evidence = stream.ck['state']['proofs']
    wrappers = sorted(a for a, r in evidence.items() if r['type'] == 'MorphoVaultV1Adapter')
    got = multicall.try_view(logs.rpc_target(cfg), wrappers, 'morphoVaultV1()', ['address'], block=head - 1)
    for a, r in zip(wrappers, got):
        evidence[a]['underlying_vault'] = r[0] if r else None
        evidence[a]['underlying_getter'] = 'morphoVaultV1'
    atomic(d / '_factory_proofs.json', evidence)
    print(f'factory proofs: {len(evidence)}/{len(addresses)} adapters', flush=True)


def adapter_stage(protocol, cid, d):
    cfg, core, vaults, markets, head = setup(protocol, cid)
    discover_adapters(protocol, cid, d)
    if not (d / '_factory_proofs.json').exists():
        adapter_factories(protocol, cid, d)
    state = json.loads((d / '_adapter_discovery_checkpoint.json').read_text())['state']
    intervals = state['adapters']
    if not intervals:
        io_utils.write_jsonl(str((d / 'adapters.jsonl').relative_to(ROOT / 'data')), [], GENERATOR, {'head': head - 1})
        return
    cachepath = d / '_adapter_details.json'
    details = json.loads(cachepath.read_text()) if cachepath.exists() else {}
    proofs = json.loads((d / '_factory_proofs.json').read_text()) if (d / '_factory_proofs.json').exists() else {}
    addresses = sorted({x['adapter'] for x in intervals})
    missing = [a for a in addresses if a not in details or 'underlying_getter' not in details[a]
               or (details[a]['type'] == 'unverified' and a in proofs)]
    parents = multicall.try_view(logs.rpc_target(cfg), missing, 'parentVault()', ['address'], block=head - 1)
    def classify(pair):
        a, parent = pair
        if a in proofs:
            proof = proofs[a]
            return a, {**proof, 'parentVault': parent[0] if parent else None,
                'parentVault_call_success': parent is not None,
                'underlying_vault': proof.get('underlying_vault'), 'underlying_getter': proof.get('underlying_getter')}
        src = source(cid, a)
        impl = source(cid, src['implementation']) if src['implementation'] else src
        functions = {e['name']: e for e in impl['abi'] if e['type'] == 'function'}
        underlying = None
        getter = None
        for fn in ('morphoVaultV1', 'vaultV1', 'erc4626Vault', 'vault'):
            if fn in functions and not functions[fn]['inputs']:
                outputs = functions[fn].get('outputs', [])
                if not outputs or outputs[0]['type'] != 'address':
                    continue
                if fn == 'vault' and 'IERC4626' not in outputs[0].get('internalType', ''):
                    continue
                ret = multicall.try_view(logs.rpc_target(cfg), [a], fn + '()', ['address'], block=head - 1)[0]
                underlying = ret[0] if ret else None
                getter = fn
                break
        return a, {'type': impl['name'] or 'unverified', 'source': impl['path'],
                   'parentVault': parent[0] if parent else None,
                   'parentVault_call_success': parent is not None,
                   'underlying_vault': underlying, 'underlying_getter': getter}
    with ThreadPoolExecutor(max_workers=3) as pool:
        for i, (a, detail) in enumerate(pool.map(classify, zip(missing, parents))):
            details[a] = detail
            atomic(cachepath, details)
            if i % 20 == 0:
                print(f'adapters: classified {len(details)}/{len(addresses)}', flush=True)
    rows = [{**r, **details[r['adapter']], 'parentVault_confirmed': details[r['adapter']]['parentVault'] == r['vault']} for r in intervals]
    io_utils.write_jsonl(str((d / 'adapters.jsonl').relative_to(ROOT / 'data')), rows, GENERATOR, {'head': head - 1})
    print(f'adapters: {len(addresses)} distinct, {len(rows)} registration intervals', flush=True)


def core_scan(protocol, cid, d):
    cfg, core, vaults, markets, head = setup(protocol, cid)
    dec = Decoder(cid, d, vaults)
    src = dec.sources[core]
    src = dec.sources.get((src['implementation'] or '').lower(), src)
    abi = {e['name']: e for e in src['abi'] if e['type'] == 'event'}
    owners = {v: v for v in vaults}
    discovery = json.loads((d / '_adapter_discovery_checkpoint.json').read_text())
    if not discovery['done']:
        raise RuntimeError('Adapter discovery must finish before core scanning')
    intervals = discovery['state']['adapters']
    parent_path = d / '_adapter_parents.json'
    if parent_path.exists():
        parents = json.loads(parent_path.read_text())
    else:
        addresses = sorted({r['adapter'] for r in intervals})
        got = multicall.try_view(logs.rpc_target(cfg), addresses, 'parentVault()', ['address'], block=head - 1)
        parents = {a: p[0] if p else None for a, p in zip(addresses, got)}
        atomic(parent_path, parents)
    for r in intervals:
        if parents[r['adapter']] == r['vault']:
            if r['adapter'] in owners and owners[r['adapter']] != r['vault']:
                raise RuntimeError('adapter maps to multiple parents')
            owners[r['adapter']] = r['vault']
    events = {evmabi.topic0(signature(abi[name])): abi[name] for name in ('Supply', 'Withdraw')}
    st = Stream(d, 'core', head)
    def handle(lg, stream):
        e = events[lg['topics'][0]]
        indexed = [x['name'] for x in e['inputs'] if x['indexed']]
        owner = evmabi.addr(lg['topics'][1 + indexed.index('onBehalf')])
        if owner not in owners:
            return
        f = decode_event(e, lg)
        v = owners[owner]
        stream.emit(f'_raw/core/{v}.csv', {**event_base(lg),
            'market_id': f['id'], 'kind': e['name'].lower(), 'assets': f['assets'],
            'shares_delta': f['shares'] if e['name'] == 'Supply' else -f['shares'],
            'adapter': owner if owner != v else ''}, RAW_FIELDS)
    st.run(cfg['hypersync'], [core], [list(events)], handle)


def csv_rows(path):
    if path.exists():
        with path.open() as f:
            yield from csv.DictReader(f)


def compile_positions(protocol, cid, d):
    cfg, core, vaults, markets, head = setup(protocol, cid)
    result = {}
    for v in vaults:
        rows = csv_rows(d / f'_raw/core/{v}.csv')
        state, by_adapter, count = Counter(), Counter(), 0
        dest = d / f'positions/{v}.csv'
        dest.parent.mkdir(parents=True, exist_ok=True)
        first = last = None
        with dest.with_suffix('.csv.tmp').open('w') as f:
            writer = csv.DictWriter(f, POSITION_FIELDS, lineterminator='\n')
            writer.writeheader()
            for r in rows:
                m, a, delta = r['market_id'], r['adapter'] or v, int(r['shares_delta'])
                state[m] += delta
                by_adapter[(m, a)] += delta
                r['cum_shares'], r['vault_cum_shares'] = by_adapter[(m, a)], state[m]
                writer.writerow(r)
                count += 1
                first = int(r['block']) if first is None else first
                last = int(r['block'])
        os.replace(dest.with_suffix('.csv.tmp'), dest)
        result[v] = {'position_events': count, 'markets_touched': len(state),
            'first_position_block': first, 'last_position_block': last,
            'final_shares': dict(state),
            'positions': [{'market_id': m, 'account': a, 'shares': sh} for (m, a), sh in sorted(by_adapter.items())]}
    atomic(d / '_positions.json', result)
    print(f'positions: {sum(r["position_events"] for r in result.values()):,} events, {sum(bool(r["position_events"]) for r in result.values())} vaults', flush=True)


def wrapper_stage(protocol, cid, d):
    """Track exact ERC4626 share balances using indexed incoming/outgoing streams."""
    import heapq
    cfg, core, vaults, markets, head = setup(protocol, cid)
    done_path = d / '_wrapper_transfers_checkpoint.json'
    if done_path.exists() and json.loads(done_path.read_text())['done'] and (d / '_wrapper_positions.json').exists():
        print('wrapper share layer: already complete', flush=True)
        return
    adapters = [json.loads(line) for line in (d / 'adapters.jsonl').read_text().splitlines()]
    holders = {r['adapter']: r for r in adapters if r['parentVault_confirmed'] and r.get('underlying_vault')}
    underlying = sorted({r['underlying_vault'] for r in holders.values()})
    fields = ['vault', 'adapter', 'underlying_vault', 'block', 'log_index', 'timestamp', 'tx_hash', 'from', 'to', 'shares_delta', 'cum_shares']
    if not holders:
        io_utils._atomic_text(d / 'adapter_shares.csv', ','.join(fields) + '\n')
        atomic(d / '_wrapper_positions.json', {})
        atomic(done_path, {'done': True, 'next_block': head, 'to_block': head, 'logs': 0, 'bytes': {'adapter_shares.csv': (d / 'adapter_shares.csv').stat().st_size}})
        return
    dec = Decoder(cid, d, vaults)
    e = next(e for es in dec.by_topic.values() for e in es if e['name'] == 'Transfer')
    indexed = [p['name'] for p in e['inputs'] if p['indexed']]
    cancelled = threading.Event()
    def scan_direction(direction):
        index = indexed.index(direction) + 1
        topics = [[evmabi.topic0(signature(e))]] + [[] for _ in range(index)]
        topics[index] = ['0x' + a[2:].rjust(64, '0') for a in holders]
        st = Stream(d, 'wrapper_' + direction, head)
        def handle(lg, stream):
            if cancelled.is_set():
                raise InterruptedError('Wrapper scan cancelled at its last checkpoint')
            f = decode_event(e, lg)
            a = f[direction]
            h = holders[a]
            if lg['address'].lower() != h['underlying_vault'] or f['from'] == f['to']:
                return
            amount = next(v for k, v in f.items() if k not in ('from', 'to'))
            row = {**event_base(lg), 'vault': h['vault'], 'adapter': a,
                   'underlying_vault': h['underlying_vault'], 'from': f['from'], 'to': f['to'],
                   'shares_delta': amount if direction == 'to' else -amount}
            stream.emit('_raw/wrapper_' + direction + '.csv', row, fields[:-1])
        st.run(cfg['hypersync'], underlying, topics, handle)
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            list(pool.map(scan_direction, ('from', 'to')))
        except BaseException:
            cancelled.set()
            raise
    streams = [csv_rows(d / ('_raw/wrapper_' + direction + '.csv')) for direction in ('from', 'to')]
    balances, count = Counter(), 0
    tmp = d / 'adapter_shares.csv.tmp'
    with tmp.open('w') as f:
        writer = csv.DictWriter(f, fields, lineterminator='\n')
        writer.writeheader()
        for r in heapq.merge(*streams, key=lambda r: (int(r['block']), int(r['log_index']))):
            a = r['adapter']
            balances[a] += int(r['shares_delta'])
            writer.writerow({**r, 'cum_shares': balances[a]})
            count += 1
    os.replace(tmp, d / 'adapter_shares.csv')
    atomic(d / '_wrapper_positions.json', {a: {'vault': h['vault'], 'underlying_vault': h['underlying_vault'], 'shares': balances[a]} for a, h in holders.items()})
    atomic(done_path, {'done': True, 'next_block': head, 'to_block': head,
        'source_stages': ['wrapper_from', 'wrapper_to'], 'logs': count,
        'bytes': {'adapter_shares.csv': (d / 'adapter_shares.csv').stat().st_size},
        'state': {'balances': dict(balances)}})
    print(f'wrapper share layer: {count:,} rows, {len(holders)} balances', flush=True)


def check_wrappers(protocol, cid, d):
    """Compare replayed wrapper share balances with balanceOf at the head."""
    cfg, core, vaults, markets, head = setup(protocol, cid)
    target = logs.rpc_target(cfg)
    positions = json.loads((d / '_wrapper_positions.json').read_text())
    accounts = sorted(positions)
    got = multicall.aggregate3(target, [(positions[a]['underlying_vault'], evmabi.encode_call('balanceOf(address)', ['address'], [a])) for a in accounts], block=head - 1)
    results = []
    for a, (ok, raw) in zip(accounts, got):
        p = positions[a]
        actual = int(raw, 16) if ok and raw not in ('', '0x') else None
        kind = vaults.get(p['underlying_vault'], {}).get('kind', 'other')
        results.append({'adapter': a, **p, 'underlying_kind': kind, 'onchain_shares': actual, 'match': actual == p['shares']})
    failed = [r for r in results if r['onchain_shares'] is None]
    unverifiable = [r for r in failed if r['underlying_kind'] == 'other']
    for r in unverifiable:
        code = rpc.get_code(target, r['underlying_vault'], block=head - 1)
        r['underlying_code_bytes'] = len(code.removeprefix('0x')) // 2
    by_kind = defaultdict(Counter)
    for r in results:
        by_kind[r['underlying_kind']]['checked'] += 1
        by_kind[r['underlying_kind']]['matches'] += r['match']
    return {'positions_checked': len(results), 'matches': sum(r['match'] for r in results),
            'by_underlying_kind': {k: dict(v) for k, v in sorted(by_kind.items())},
            'mismatches': [r for r in results if r['onchain_shares'] is not None and not r['match']],
            'call_failed': [r for r in failed if r['underlying_kind'] != 'other'],
            'unverifiable': unverifiable, 'results': results}


def position_call(mid, account):
    return evmabi.encode_call('position(bytes32,address)', ['bytes32', 'address'], [bytes.fromhex(mid[2:]), account])


def check_old(cid, d):
    keys = ['block', 'log_index', 'market_id', 'kind', 'assets', 'shares_delta']
    result = {'vaults_compared': 0, 'rows_compared': 0, 'vaults_match': 0, 'differences': [], 'per_vault': []}
    for path in sorted((ROOT / 'data/trajectories').glob(f'{cid}-*.events.csv')):
        v = path.name.split('-')[1].split('.')[0]
        old = [tuple(r[k] for k in keys) for r in csv_rows(path)]
        last = max((int(r[0]) for r in old), default=-1)
        new = []
        for r in csv_rows(d / f'positions/{v}.csv'):
            if int(r['block']) > last:
                break
            new.append(tuple(r[k] for k in keys))
        diffs = []
        if old != new:
            om = {(r[0], r[1]): r for r in old}
            nm = {(r[0], r[1]): r for r in new}
            for key in sorted(om.keys() | nm.keys(), key=lambda k: (int(k[0]), int(k[1]))):
                if om.get(key) != nm.get(key):
                    diffs.append({'vault': v, 'block': int(key[0]), 'log_index': int(key[1]), 'old': om.get(key), 'new': nm.get(key)})
            if not diffs:
                diffs.append({'vault': v, 'error': 'ordering or duplicate multiplicity differs'})
        result['vaults_compared'] += 1
        result['rows_compared'] += len(old)
        result['vaults_match'] += old == new
        result['differences'].extend(diffs)
        result['per_vault'].append({'vault': v, 'last_old_block': last, 'old_rows': len(old), 'new_rows': len(new), 'equal': old == new, 'differences': len(diffs)})
    result['difference_count'] = len(result['differences'])
    return result


def check_head(protocol, cid, d):
    cfg, core, vaults, markets, head = setup(protocol, cid)
    positions = json.loads((d / '_positions.json').read_text())
    points = [{**p, 'vault': v} for v, r in positions.items() for p in r['positions']]
    got = multicall.aggregate3(logs.rpc_target(cfg), [(core, position_call(p['market_id'], p['account'])) for p in points], block=head - 1, chunk=300)
    result = {'block': head - 1, 'vaults': len(vaults), 'vaults_with_positions': sum(bool(r['positions']) for r in positions.values()),
              'positions_checked': len(points), 'matches': 0, 'mismatches': [], 'call_failed': [], 'results': []}
    for p, (ok, raw) in zip(points, got):
        onchain = int(raw[2:66], 16) if ok and len(raw) >= 66 else None
        r = {**p, 'onchain_shares': onchain, 'match': onchain == p['shares']}
        result['results'].append(r)
        if onchain is None:
            result['call_failed'].append(r)
        elif r['match']:
            result['matches'] += 1
        else:
            result['mismatches'].append(r)
    result['vaults_without_observed_core_positions'] = [v for v, r in positions.items() if not r['positions']]
    return result


def check_sample(protocol, cid, d):
    cfg, core, vaults, markets, head = setup(protocol, cid)
    positions = json.loads((d / '_positions.json').read_text())
    populated = sorted(v for v, r in positions.items() if r['positions'])
    rng = random.Random(917_113 + int(cid))
    desired = defaultdict(list)
    seen = set()
    while len(seen) < 200:
        v = rng.choice(populated)
        p = rng.choice(positions[v]['positions'])
        b = rng.randrange(max(vaults[v]['created_block'], markets.get(p['market_id'], {}).get('created_block', 0)), head)
        key = (v, p['market_id'], p['account'], b)
        if key in seen:
            continue
        seen.add(key)
        desired[v].append({'vault': v, 'market_id': p['market_id'], 'account': p['account'], 'block': b})
    points = []
    for v, targets in desired.items():
        targets.sort(key=lambda r: r['block'])
        state, index = Counter(), 0
        for r in csv_rows(d / f'positions/{v}.csv'):
            while index < len(targets) and int(r['block']) > targets[index]['block']:
                p = targets[index]
                points.append({**p, 'replay_shares': state[(p['market_id'], p['account'])]})
                index += 1
            if index == len(targets):
                break
            state[(r['market_id'], r['adapter'] or v)] += int(r['shares_delta'])
        for p in targets[index:]:
            points.append({**p, 'replay_shares': state[(p['market_id'], p['account'])]})
    def read(p):
        try:
            raw = rpc.eth_call(logs.rpc_target(cfg), core, position_call(p['market_id'], p['account']), block=p['block'])
            return {**p, 'onchain_shares': int(raw[2:66], 16)}
        except Exception as exc:
            return {**p, 'error': type(exc).__name__}
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(read, points))
    return {'seed': 917_113 + int(cid), 'method': 'uniform funded vault, then observed market-account, then uniform block from vault and market creation through scan head; end-of-block state',
            'points': len(results), 'matches': sum(p.get('onchain_shares') == p['replay_shares'] for p in results),
            'mismatches': [p for p in results if 'error' not in p and p['onchain_shares'] != p['replay_shares']],
            'call_failed': [p for p in results if 'error' in p], 'results': results}


def check_roles(protocol, cid, d):
    cfg, core, vaults, markets, head = setup(protocol, cid)
    state = json.loads((d / '_vault_checkpoint.json').read_text())['state']['roles']
    history = defaultdict(lambda: defaultdict(set))
    for line in (d / 'roles.jsonl').read_text().splitlines():
        r = json.loads(line)
        history[r['vault']][r['role']].add(r['address'])
    rng = random.Random(7919 + int(cid))
    chosen = set(rng.sample(sorted(vaults), min(50, len(vaults))))
    if protocol == 'morpho':
        v1 = sorted(v for v in vaults if vaults[v]['kind'] == 'morpho_v1' and v not in chosen)
        need = max(0, 50 - sum(vaults[v]['kind'] == 'morpho_v1' for v in chosen))
        chosen |= set(rng.sample(v1, min(need, len(v1))))
    chosen = sorted(chosen)
    queries, calls = [], []
    def add(v, role, sig, types, values, returns, expected, account=None):
        queries.append({'vault': v, 'role': role, 'getter': sig, 'account': account, 'expected': expected, 'returns': returns})
        calls.append((v, evmabi.encode_call(sig, types, values)))
    for v in chosen:
        roles = state.get(v, {})
        if protocol == 'moolah':
            ids = {evmabi.topic0(x): x for x in ['MANAGER', 'CURATOR', 'ALLOCATOR', 'BOT']}
            ids['0x' + '00' * 32] = 'DEFAULT_ADMIN_ROLE'
            for role in roles:
                if role.startswith('0x'):
                    ids[role] = role
            for role_id, role in ids.items():
                add(v, role, 'getRoleMembers(bytes32)', ['bytes32'], [bytes.fromhex(role_id[2:])], ['address[]'], sorted(roles.get(role, [])))
                for a in sorted(history[v].get(role, set()) | {ZERO}):
                    add(v, role, 'hasRole(bytes32,address)', ['bytes32', 'address'], [bytes.fromhex(role_id[2:]), a], ['bool'], a in roles.get(role, []), a)
        else:
            singles = [('owner', 'owner()'), ('curator', 'curator()')]
            if vaults[v]['kind'] == 'morpho_v1':
                singles += [('guardian', 'guardian()'), ('pending_owner', 'pendingOwner()')]
            for role, getter in singles:
                add(v, role, getter, [], [], ['address'], next(iter(roles.get(role, [])), ZERO))
            sets = [('allocator', 'isAllocator(address)')]
            if vaults[v]['kind'] == 'morpho_v2':
                sets += [('sentinel', 'isSentinel(address)')]
            for role, getter in sets:
                for a in sorted(history[v].get(role, set()) | {ZERO}):
                    add(v, role, getter, ['address'], [a], ['bool'], a in roles.get(role, []), a)
    got = multicall.aggregate3(logs.rpc_target(cfg), calls, block=head - 1, chunk=200)
    results = []
    for q, (ok, raw) in zip(queries, got):
        try:
            actual = clean(evmabi.decode(q['returns'], raw)[0]) if ok else None
            if q['returns'] == ['address[]']:
                actual = sorted(actual)
        except Exception:
            actual = None
        results.append({**{k: v for k, v in q.items() if k != 'returns'}, 'actual': actual, 'match': actual == q['expected']})
    bad = {r['vault'] for r in results if not r['match']}
    return {'seed': 7919 + int(cid), 'block': head - 1, 'vaults_checked': len(chosen), 'vaults_match': len(chosen) - len(bad),
            'v1_vaults_checked': sum(vaults[v]['kind'] == 'morpho_v1' for v in chosen),
            'getters_checked': len(results), 'matches': sum(r['match'] for r in results),
            'mismatches': [r for r in results if r['actual'] is not None and not r['match']],
            'call_failed': [r for r in results if r['actual'] is None], 'results': results,
            'limitation': ('Checks enumerable getRoleMembers(bytes32) plus hasRole for every event-observed member and zero.'
                           if protocol == 'moolah' else 'Morpho boolean membership getters check every event-observed member plus zero; they cannot enumerate silent members.')}


def validate(protocol, cid, d):
    cfg, core, vaults, markets, head = setup(protocol, cid)
    if not json.loads((d / '_core_checkpoint.json').read_text())['done']:
        raise RuntimeError('Core scan must finish before validation')
    outpath = d / 'validation.json'
    out = json.loads(outpath.read_text()) if outpath.exists() else {'head_block': head - 1}
    for name, fn in [('old_trajectories', lambda: check_old(cid, d)),
                     ('head', lambda: check_head(protocol, cid, d)),
                     ('archive_sample', lambda: check_sample(protocol, cid, d)),
                     ('roles', lambda: check_roles(protocol, cid, d)),
                     ('wrapper_balances', lambda: check_wrappers(protocol, cid, d))]:
        if name in out:
            continue
        if name == 'roles' and not json.loads((d / '_vault_checkpoint.json').read_text())['done']:
            print('validation roles: waiting for vault scan', flush=True)
            continue
        if name == 'wrapper_balances' and not (d / '_wrapper_positions.json').exists():
            print('validation wrapper_balances: waiting for wrapper scan', flush=True)
            continue
        start, calls0 = time.time(), rpc.calls_made()
        out[name] = fn()
        out[name]['seconds'] = round(time.time() - start, 3)
        out[name]['rpc_calls'] = rpc.calls_made() - calls0
        check = out[name]
        not_applicable = (name == 'old_trajectories' and check['vaults_compared'] == 0
                          or name == 'wrapper_balances' and check['positions_checked'] == 0)
        failed = any(check.get(key) for key in ['differences', 'mismatches', 'call_failed'])
        check['status'] = 'not_applicable' if not_applicable else 'fail' if failed else 'pass'
        if not_applicable:
            check['not_applicable_reason'] = ('No matching legacy trajectory CSV files exist in data/trajectories.'
                                              if name == 'old_trajectories' else 'No V2 wrapper adapters were discovered on this chain.')
        atomic(outpath, out)
        print(f'validation {name}: ' + json.dumps({k: v for k, v in out[name].items() if isinstance(v, (int, float, bool))}), flush=True)


def action_calls(cid, d, vaults, head):
    """Decode V2 timelock calldata in a joined supplement without rewriting logs."""
    meta_path = d / 'action_calls.jsonl.meta.json'
    size = (d / 'actions.jsonl').stat().st_size
    source_bytes = (d / 'sources.json').read_bytes()
    source_hash = hashlib.sha256(source_bytes).hexdigest()
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        if (meta.get('actions_bytes') == size and meta.get('head_block') == head - 1
                and meta.get('sources_sha256') == source_hash):
            return meta
    sources = json.loads(source_bytes)
    families, fallback = {}, {}
    for address, src in sources.items():
        if address not in vaults or vaults[address]['kind'] != 'morpho_v2':
            continue
        v = vaults[address]
        source_abi = sources.get((src['implementation'] or '').lower(), src)
        functions = {evmabi.selector(signature(e)): (e, source_abi['path']) for e in source_abi['abi'] if e['type'] == 'function'}
        if functions:
            families[v['factory']] = functions
            fallback.update(functions)
    pattern = re.compile(rb'"event":"(?:Submit|Accept|Revoke)"')
    result = {'head_block': head - 1, 'actions_bytes': size, 'sources_sha256': source_hash,
              'rows': 0, 'decoded': 0, 'unrecognized': 0,
              'generator': GENERATOR, 'join_keys': ['vault', 'block', 'log_index']}
    tmp = d / 'action_calls.jsonl.tmp'
    with (d / 'actions.jsonl').open('rb') as source_file, tmp.open('w') as output:
        for line in source_file:
            if not pattern.search(line):
                continue
            r = json.loads(line)
            data = r['fields'].get('data', '')
            selector = data[:10]
            known = families.get(vaults[r['vault']]['factory'], {})
            spec = known.get(selector) or fallback.get(selector)
            row = {k: r[k] for k in ['vault', 'block', 'log_index', 'timestamp', 'tx_hash', 'event']}
            row['selector'] = selector
            if spec:
                e, path = spec
                try:
                    args = evmabi.decode([typ(p) for p in e['inputs']], '0x' + data[10:])
                    row.update({'function': e['name'], 'signature': signature(e),
                        'arguments': {p['name']: clean(value) for p, value in zip(e['inputs'], args)},
                        'abi_source': path, 'status': 'decoded'})
                    result['decoded'] += 1
                except Exception:
                    row['status'] = 'calldata_does_not_match_abi'
            else:
                row['status'] = 'selector_not_in_verified_v2_abis'
            if row['status'] != 'decoded':
                result['unrecognized'] += 1
                row['data'] = data
            result['rows'] += 1
            output.write(json.dumps(row, separators=(',', ':')) + '\n')
    os.replace(tmp, d / 'action_calls.jsonl')
    atomic(meta_path, result)
    return result


def summary(protocol, cid, d):
    cfg, core, vaults, markets, head = setup(protocol, cid)
    ck = json.loads((d / '_vault_checkpoint.json').read_text())
    if not ck['done'] or not json.loads((d / '_core_checkpoint.json').read_text())['done']:
        raise RuntimeError('Both event scans must finish before summary')
    positions = json.loads((d / '_positions.json').read_text())
    call_summary = action_calls(cid, d, vaults, head)
    wrapper_counts, wrapper_tokens = Counter(), defaultdict(set)
    for r in csv_rows(d / 'adapter_shares.csv'):
        wrapper_counts[r['vault']] += 1
        if int(r['cum_shares']) > 0:
            wrapper_tokens[r['vault']].add(r['underlying_vault'])
    rows = []
    for v, r in vaults.items():
        p = positions[v]
        flow = d / f'flows/{v}.csv'
        if not flow.exists():
            flow.parent.mkdir(parents=True, exist_ok=True)
            io_utils._atomic_text(flow, ','.join(FLOW_FIELDS) + '\n')
        rows.append({**r, **{k: val for k, val in p.items() if k not in ['positions', 'final_shares']},
            'events': p['position_events'] + sum(ck['state']['event_counts'].get(v, {}).values()),
            'wrapper_share_events': wrapper_counts[v],
            'wrapped_vaults_ever_held': sorted(wrapper_tokens.get(v, set())),
            'events_by_name': ck['state']['event_counts'].get(v, {}), 'roles_at_head': ck['state']['roles'].get(v, {}),
            'first_block': min((x for x in [ck['state']['first'].get(v), p['first_position_block']] if x is not None), default=None),
            'last_block': max((x for x in [ck['state']['last'].get(v), p['last_position_block']] if x is not None), default=None)})
    io_utils.write_jsonl(str((d / 'vaults.jsonl').relative_to(ROOT / 'data')), rows, GENERATOR,
        {'protocol': protocol, 'chain_id': int(cid), 'head_block': head - 1, 'to_block_exclusive': head,
         'source': 'HyperSync logs; verified Etherscan ABI; archive dRPC validation',
         'cum_shares': 'supply shares per core account and market; account is adapter when present, otherwise vault',
         'vault_cum_shares': 'sum across the vault and its confirmed adapters in the market',
         'scope': 'every registry vault through its vault-registry scan head'})
    adapters = [json.loads(x) for x in (d / 'adapters.jsonl').read_text().splitlines()]
    paths = [p for p in d.rglob('*') if p.is_file() and not p.name.endswith('.tmp')]
    sizes = Counter()
    for p in paths:
        rel = p.relative_to(d)
        sizes[rel.parts[0] if len(rel.parts) > 1 else rel.name] += p.stat().st_size
    report = {'vaults': len(vaults), 'vault_kinds': dict(Counter(v['kind'] for v in vaults.values())),
        'vaults_with_positions': sum(p['position_events'] > 0 for p in positions.values()),
        'position_events': sum(p['position_events'] for p in positions.values()),
        'markets_touched': len({m for p in positions.values() for m in p['final_shares']}),
        'vault_market_pairs': sum(p['markets_touched'] for p in positions.values()),
        'adapters_found': len({r['adapter'] for r in adapters}),
        'adapter_intervals': len(adapters), 'adapter_types': dict(Counter({a['adapter']: a['type'] for a in adapters}.values())),
        'adapter_parent_failures': [a for a in adapters if not a['parentVault_confirmed']],
        'unknown_events': ck['state']['unknown'], 'head_block': head - 1,
        'flow_events': sum(n for f, n in ck['counts'].items() if f.startswith('flows/')),
        'fee_events': ck['counts'].get('fees.csv', 0), 'role_changes': ck['counts'].get('roles.jsonl', 0),
        'action_events': ck['counts'].get('actions.jsonl', 0), 'file_bytes': dict(sizes),
        'data_file_bytes': sum(p.stat().st_size for p in paths if not any(part.startswith('_') for part in p.relative_to(d).parts)),
        'unresolved': []}
    report['markets_not_in_registry'] = sorted(
        {m for p in positions.values() for m in p['final_shares']} - set(markets))
    report['registry_vaults_created_after_head'] = sorted(
        v for v, r in vaults.items() if r['created_block'] >= head)
    if report['markets_not_in_registry']:
        report['unresolved'].append(
            'Some observed market IDs postdate the market-registry snapshot; '
            'their positions are retained and their IDs are listed separately.')
    unverified = sorted({a['adapter'] for a in adapters if a['type'] == 'unverified'})
    report['unverified_adapters'] = unverified
    if unverified:
        report['unresolved'].append(f'{len(unverified)} adapter types have neither verified individual source nor verified factory creation proof.')
    if report['adapter_parent_failures']:
        report['unresolved'].append('Adapters failing parentVault() confirmation are excluded from core attribution and listed individually.')
    if report['unknown_events']:
        report['unresolved'].append('Unrecognized events are retained verbatim in unknown_events.jsonl.')
    wrappers = [a for a in adapters if a.get('underlying_vault')]
    if wrappers:
        report['unresolved'].append('ERC4626 adapters hold underlying vault shares; core position events of that underlying vault are not attributed in full to the V2 parent. See adapter_shares.csv for the distinct share layer when present.')
    manifest = json.loads((d / 'sources.json').read_text())
    catalog = {}
    for address, src in manifest.items():
        for event in src['abi']:
            if event['type'] != 'event':
                continue
            key = json.dumps(event, sort_keys=True)
            if key not in catalog:
                catalog[key] = {'event': event['name'], 'signature': signature(event),
                    'topic0': evmabi.topic0(signature(event)), 'inputs': event['inputs'], 'sources': []}
            catalog[key]['sources'].append({'chain_id': int(cid), 'address': address, 'contract': src['name'], 'path': src['path']})
    atomic(d / 'event_sources.json', list(catalog.values()))
    report['adapter_discovery_consistent'] = ck['state']['adapters'] == json.loads((d / '_adapter_discovery_checkpoint.json').read_text())['state']['adapters']
    report['timelock_calls'] = call_summary
    validation_path = d / 'validation.json'
    if validation_path.exists():
        wrapper_check = json.loads(validation_path.read_text()).get('wrapper_balances', {})
        report['wrapper_balance_check'] = {
            'positions_checked': wrapper_check.get('positions_checked', 0),
            'matches': wrapper_check.get('matches', 0),
            'mismatches': len(wrapper_check.get('mismatches', [])),
            'call_failed': len(wrapper_check.get('call_failed', [])),
            'unverifiable': len(wrapper_check.get('unverifiable', []))}
        if wrapper_check.get('call_failed') or wrapper_check.get('mismatches'):
            report['unresolved'].append(
                'Wrapper balance mismatches or failed balanceOf calls are listed in validation.json.')
        if wrapper_check.get('unverifiable'):
            report['unresolved'].append(
                f"{len(wrapper_check['unverifiable'])} wrapper adapters point at a contract outside the registry "
                'whose balanceOf reverts; their replayed balances cannot be checked (validation.json, unverifiable).')
    report['wrapper_share_events'] = sum(wrapper_counts.values())
    report['vaults_with_wrapper_positions'] = len(wrapper_tokens)
    report['vaults_with_recorded_positions'] = len(
        {v for v, p in positions.items() if p['position_events']} | set(wrapper_tokens))
    if call_summary['unrecognized']:
        report['unresolved'].append(
            f"{call_summary['unrecognized']} generic timelock payloads could not "
            "be decoded with the verified V2 ABIs; raw bytes and selectors "
            "are retained in action_calls.jsonl.")
    primary_names = ['actions.jsonl', 'roles.jsonl', 'fees.csv', 'adapters.jsonl', 'vaults.jsonl',
                     'vaults.jsonl.meta.json', 'validation.json', 'adapter_shares.csv', 'action_calls.jsonl']
    primary_sizes = {name: (d / name).stat().st_size for name in primary_names if (d / name).exists()}
    primary_sizes.update({name: sum(p.stat().st_size for p in (d / name).glob('*.csv')) for name in ['positions', 'flows']})
    report['primary_file_bytes'] = primary_sizes
    report['primary_output_bytes'] = sum(primary_sizes.values())
    atomic(d / 'report.json', report)
    print('summary: ' + json.dumps({k: v for k, v in report.items() if isinstance(v, int)}), flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--protocol', required=True, choices=['morpho', 'moolah'])
    ap.add_argument('--chain', required=True)
    ap.add_argument('--prefetch', type=int, default=1, choices=range(1, 9),
                    help='Concurrent bounded HyperSync ranges; replay remains ordered')
    ap.add_argument('--range-blocks', type=int,
                    help='Block width of every bounded HyperSync range; long chains with few logs '
                         'per block need far fewer requests than the defaults in FETCH_BLOCKS')
    ap.add_argument('--stage', default='all', choices=['all', 'sources', 'estimate', 'vault', 'adapters', 'normalize-adapters', 'factories', 'positions', 'wrappers', 'validate', 'summary'])
    args = ap.parse_args()
    global PREFETCH_WORKERS
    PREFETCH_WORKERS = args.prefetch
    if args.range_blocks:
        FETCH_BLOCKS.update(dict.fromkeys(FETCH_BLOCKS, args.range_blocks))
    d = ROOT / f'data/vaults/{args.protocol}/{args.chain}'
    started = time.time()
    stages = ['sources', 'estimate', 'vault', 'adapters', 'normalize-adapters', 'positions', 'wrappers', 'validate', 'summary'] if args.stage == 'all' else [args.stage]
    try:
        for stage in stages:
            if stage == 'sources':
                bootstrap(args.protocol, args.chain, d)
            elif stage == 'estimate':
                estimate(args.protocol, args.chain, d)
            elif stage == 'vault':
                vault_scan(args.protocol, args.chain, d)
            elif stage == 'adapters':
                adapter_stage(args.protocol, args.chain, d)
            elif stage == 'normalize-adapters':
                normalize_adapters(args.protocol, args.chain, d)
            elif stage == 'factories':
                adapter_factories(args.protocol, args.chain, d)
            elif stage == 'positions':
                core_scan(args.protocol, args.chain, d)
                compile_positions(args.protocol, args.chain, d)
            elif stage == 'wrappers':
                wrapper_stage(args.protocol, args.chain, d)
            elif stage == 'validate':
                validate(args.protocol, args.chain, d)
            elif stage == 'summary':
                summary(args.protocol, args.chain, d)
    finally:
        record = {'stage': args.stage, 'started_unix': started, 'seconds': round(time.time()-started, 3), 'rpc_calls': rpc.calls_made(),
                  'prefetch': args.prefetch, 'range_blocks': args.range_blocks}
        path = d / 'runs.jsonl'
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a') as f:
            f.write(json.dumps(record) + '\n')
        print(json.dumps(record), flush=True)


if __name__ == '__main__':
    try:
        main()
    except (Exception, KeyboardInterrupt) as exc:
        message = traceback.format_exc()
        for key in ('ETHERSCAN_API_KEY', 'DRPC_API_KEY', 'HYPERSYNC_API_TOKEN'):
            value = env.get(key)
            if value:
                message = message.replace(value, '<redacted>')
        print(message, file=sys.stderr, flush=True)
        sys.exit(130 if isinstance(exc, KeyboardInterrupt) else 1)
