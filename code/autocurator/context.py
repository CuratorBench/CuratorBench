"""Shared data access for the AutoCurator pipeline (E7 to E9)."""
import bisect
import csv
import datetime as dt
import json
import os
import pathlib
import pickle
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, field

ROOT = pathlib.Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
INPUTS = pathlib.Path(os.environ.get("AC_INPUTS") or os.environ.get("CB_OUT") or DATA).resolve()
if os.environ.get("AC_INPUTS"):
    os.environ["CB_OUT"] = str(INPUTS)
WORK = pathlib.Path(os.environ.get("AUTOCURATOR_WORK", "/tmp/autocurator_work"))
OUT = DATA / "autocurator"
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "code" / "analysis"))
sys.path.insert(0, str(ROOT / "code"))
import e2_events as e2

DAY = 86400
YEAR = 365 * DAY
ZERO = "0x" + "0" * 40
END_UTC = "2026-10-02"
MATERIAL_USD = 10_000.0
TOP_GROUPS = 30
EXPOSURE_LOOKBACK_D = 180
STUCK_DAYS = 30
PINNED_U = 0.99
PRICE_LAG_D = 2
EULER_INIT_FEE = 0.10


def jsonl(path):
    with open(path) as fh:
        return [json.loads(x) for x in fh if x.strip()]


def day_of_ts(ts):
    return int(ts) // DAY


def day_iso(d):
    return dt.datetime.fromtimestamp(int(d) * DAY, dt.timezone.utc).date().isoformat()


def ts_iso(ts):
    return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(text):
    """'2025-11-02 20:24' or ISO text in UTC to unix seconds."""
    t = text.strip().replace("Z", "").replace("T", " ")
    fmt = "%Y-%m-%d %H:%M:%S" if t.count(":") == 2 else "%Y-%m-%d %H:%M"
    return int(dt.datetime.strptime(t, fmt).replace(tzinfo=dt.timezone.utc).timestamp())


def now_iso():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def save_pickle(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with open(tmp, "wb") as fh:
        pickle.dump(obj, fh, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)


def load_pickle(path):
    with open(path, "rb") as fh:
        return pickle.load(fh)


def write_json(path, obj, indent=1):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(obj, indent=indent, sort_keys=True, default=str))
    os.replace(tmp, path)


def write_sidecar(path, meta):
    """`<file>.meta.json` next to an output, as every E1 to E6 output has."""
    meta = dict(meta)
    meta.setdefault("written_utc", now_iso())
    write_json(path.with_name(path.name + ".meta.json"), meta)


class Tokens:
    """Symbol, decimals and peg family per (chain, token), from the E1 price panel's token table, E2's supplement, Silo configs and Euler asset metadata."""

    def __init__(self):
        self.rows = {}
        for r in jsonl(DATA / "prices/tokens.jsonl"):
            dec = r["decimals"]
            if r.get("defillama_decimals") is not None and r["defillama_decimals"] != dec:
                dec = r["defillama_decimals"]
            self.rows[(r["chain_id"], r["token"])] = {
                "symbol": r["symbol"], "decimals": dec, "peg_family": r["peg_family"]}
        sup = INPUTS / "events/tokens_supplement.jsonl"
        if sup.exists():
            for r in jsonl(sup):
                self.rows.setdefault((r["chain_id"], r["token"]), {
                    "symbol": r["symbol"], "decimals": r["decimals"],
                    "peg_family": r["peg_family"]})
        for p in (DATA / "panel/silo_v2").glob("*/configs.jsonl"):
            for r in jsonl(p):
                k = (int(r["chain_id"]), r["token"])
                row = self.rows.setdefault(k, {"symbol": None, "decimals": None,
                                               "peg_family": "unknown"})
                row["symbol"] = row["symbol"] or r.get("symbol")
                row["decimals"] = row["decimals"] if row["decimals"] is not None else r.get("decimals")
        for p in (DATA / "vaults/euler_v2").glob("*/asset_metadata.json"):
            cid = int(p.parent.name)
            for v in json.loads(p.read_text()).values():
                row = self.rows.setdefault((cid, v["asset"]), {"symbol": None, "decimals": None,
                                                               "peg_family": "unknown"})
                if row["decimals"] is None:
                    row["decimals"] = v["decimals"]
        for p in (DATA / "registry/oracles").glob("*/*.jsonl"):
            cid = int(p.stem)
            for r in jsonl(p):
                for k in ("loan", "collateral"):
                    tok, sym = r.get(f"{k}_token"), r.get(f"{k}_symbol")
                    if tok and sym:
                        row = self.rows.setdefault((cid, tok), {"symbol": None, "decimals": None,
                                                                "peg_family": "unknown"})
                        row["symbol"] = row["symbol"] or sym

    def get(self, cid, token):
        return self.rows.get((int(cid), token)) or {}

    def symbol(self, cid, token):
        return self.get(cid, token).get("symbol") or (token[:10] if token else "?")

    def decimals(self, cid, token):
        return self.get(cid, token).get("decimals")

    def peg(self, cid, token):
        return {"usd_stable": "USD", "eth": "ETH", "btc": "BTC"}.get(
            self.get(cid, token).get("peg_family"))


class Prices:
    """USD per whole token on a UTC day."""

    WETH = (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
    WBTC = (1, "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599")
    CARRY_D = 3

    def __init__(self, tokens, wanted):
        self.tokens = tokens
        wanted = {(int(c), t) for c, t in wanted} | {self.WETH, self.WBTC}
        on_eth = defaultdict(list)
        for (c, t), row in tokens.rows.items():
            if c == 1 and row.get("symbol"):
                on_eth[row["symbol"].upper()].append(t)
        self.twin = {}
        for c, t in list(wanted):
            sym = (tokens.get(c, t).get("symbol") or "").upper()
            if c != 1 and len(on_eth.get(sym, [])) == 1:
                self.twin[(c, t)] = (1, on_eth[sym][0])
        wanted |= set(self.twin.values())
        self.series = defaultdict(dict)
        by_chain = defaultdict(set)
        for c, t in wanted:
            by_chain[c].add(t)
        for c, toks in by_chain.items():
            p = DATA / f"prices/{c}.csv"
            if not p.exists():
                continue
            with open(p) as fh:
                for r in csv.DictReader(fh):
                    if r["token"] in toks and r["price_usd"]:
                        self.series[(c, r["token"])][e2.day_of(r["date"])] = float(r["price_usd"])
        sup = INPUTS / "events/prices_supplement.csv"
        if sup.exists():
            with open(sup) as fh:
                for r in csv.DictReader(fh):
                    k = (int(r["chain_id"]), r["token"])
                    if k in wanted and r["price_usd"]:
                        self.series[k].setdefault(e2.day_of(r["date"]), float(r["price_usd"]))
        self.days = {k: sorted(v) for k, v in self.series.items()}

    def raw(self, cid, token, day):
        k = (int(cid), token)
        s = self.series.get(k)
        if not s:
            return None
        if day in s:
            return s[day]
        i = bisect.bisect_right(self.days[k], day) - 1
        if i >= 0 and day - self.days[k][i] <= self.CARRY_D:
            return s[self.days[k][i]]
        return None

    def usd(self, cid, token, day):
        px = self.raw(cid, token, day)
        if px is not None:
            return px
        twin = self.twin.get((int(cid), token))
        if twin is not None:
            px = self.raw(*twin, day)
            if px is not None:
                return px
        peg = self.tokens.peg(cid, token)
        if peg == "USD":
            return 1.0
        if peg == "ETH":
            return self.raw(*self.WETH, day)
        if peg == "BTC":
            return self.raw(*self.WBTC, day)
        return None

    def to_usd(self, cid, token, raw_amount, day):
        dec = self.tokens.decimals(cid, token)
        px = self.usd(cid, token, day)
        if dec is None or px is None:
            return None
        return raw_amount / 10 ** dec * px


_EVENTS = None
_E5 = None


def load_events():
    """{event_id: E2 event row}."""
    global _EVENTS
    if _EVENTS is None:
        _EVENTS = {e["event_id"]: e for e in jsonl(INPUTS / "events/events.jsonl")}
    return _EVENTS


def all_event_ids():
    """Every event of Table 3 (the excluded self-dealing loop left out)."""
    return [k for k, e in load_events().items() if not e.get("excluded")]


def e5_rows():
    global _E5
    if _E5 is None:
        with open(INPUTS / "metrics/e5/event_vault.csv") as fh:
            _E5 = list(csv.DictReader(fh))
    return _E5


def e5_t0(event_id):
    """E5's t0 (loss onset for USDX and RLP), else E2's onset."""
    for r in e5_rows():
        if r["event_id"] == event_id and r["t0_utc"]:
            return parse_utc(r["t0_utc"])
    return int(load_events()[event_id]["onset_ts"])


END_DAY = e2.day_of(END_UTC)


COMMON_START = None


def span_start_day(model, rule="stated"):
    """First day whose 00:00 UTC decision `model` may make."""
    import models as M
    if M.MODELS.get(model, {}).get("leakage_control"):
        return COMMON_START
    _ok, first, _why = M.eligible(model, 0, rule)
    if first is None:
        return None
    return first // DAY + 1


_ASSET = {}


def vault_asset(proto, cid, vault):
    """Loan token of a curated vault."""
    key = (proto, int(cid))
    if key not in _ASSET:
        m = {v.lower(): r["asset"].lower() for v, r in e2.vault_registry(proto, cid).items() if r.get("asset")}
        if proto == "euler_v2":
            for r in jsonl(DATA / f"registry/markets/euler_v2/{cid}.jsonl"):
                if r.get("type") == "vault" and r.get("asset"):
                    m.setdefault(r["vault"].lower(), r["asset"].lower())
        _ASSET[key] = m
    return _ASSET[key].get(vault.lower())


@dataclass
class Group:
    """One hypothetical vault."""
    protocol: str
    chain_id: int
    loan_token: str
    loan_symbol: str
    loan_decimals: int
    loan_unit: str
    first_day: int
    last_day: int
    humans: list = field(default_factory=list)

    @property
    def key(self):
        sym = "".join(ch for ch in (self.loan_symbol or "x") if ch.isalnum()) or "x"
        return f"{self.protocol}_{self.chain_id}_{sym}_{self.loan_token[2:8]}"

    def window(self, start_day=None):
        """(first day, last day) of the group's span from `start_day` on, or None when the group has no human vault left by then."""
        d0 = self.first_day if start_day is None else max(self.first_day, int(start_day))
        return (d0, self.last_day) if d0 <= self.last_day else None


_GROUPS = None


def load_groups():
    """Every group, from E4's vault scorecard (status in), in a fixed order."""
    global _GROUPS
    if _GROUPS is not None:
        return _GROUPS
    tokens = Tokens()
    by = defaultdict(list)
    with open(INPUTS / "metrics/e4/vault_scorecard.csv") as fh:
        for r in csv.DictReader(fh):
            if r["status"] != "in":
                continue
            asset = vault_asset(r["protocol"], int(r["chain_id"]), r["vault"])
            if asset is None:
                continue
            by[(r["protocol"], int(r["chain_id"]), asset)].append(r)
    out = []
    for (proto, cid, token), hs in sorted(by.items()):
        first = min(e2.day_of(h["first_active"]) for h in hs)
        last = min(max(e2.day_of(h["last_active"]) for h in hs), END_DAY)
        if last < first:
            continue
        out.append(Group(protocol=proto, chain_id=cid, loan_token=token,
                         loan_symbol=tokens.symbol(cid, token) or hs[0]["asset_symbol"],
                         loan_decimals=tokens.decimals(cid, token), loan_unit=hs[0]["loan_unit"],
                         first_day=first, last_day=last, humans=hs))
    _GROUPS = out
    return out


def group_aum(group):
    """E4's aum_mean_usd summed over the group's human vaults."""
    return sum(float(h.get("aum_mean_usd") or 0.0) for h in group.humans)


def bench_groups(n=TOP_GROUPS):
    """The n groups with the largest human AUM over the whole sample (ties by key), largest first."""
    return sorted(load_groups(), key=lambda g: (-group_aum(g), g.key))[:n]


def group_by_key(key):
    return next(g for g in load_groups() if g.key == key)


def group_events(group):
    """[(E2 event, its affected markets on the group's protocol, chain and loan token)] for every event of Table 3 that touches the group."""
    out = []
    for e in load_events().values():
        if e.get("excluded"):
            continue
        aff = [m for m in e["affected_markets"] if m["protocol"] == group.protocol
               and int(m["chain_id"]) == group.chain_id and m["loan_token"].lower() == group.loan_token]
        if aff:
            out.append((e, aff))
    return out


def e5_humans(event_id, group):
    """E5's third-party rows of the event on the group's protocol, chain and loan token (the humans as E5 measured them)."""
    out = []
    for r in e5_rows():
        if (r["event_id"] == event_id and r["third_party"] == "True" and r["protocol"] == group.protocol
                and int(r["chain_id"]) == group.chain_id
                and vault_asset(r["protocol"], int(r["chain_id"]), r["vault"]) == group.loan_token):
            out.append(r)
    return out


class Markets:
    """Static facts of every market of one protocol and chain."""

    def __init__(self, proto, cid):
        self.proto, self.cid = proto, int(cid)
        self.info = e2.market_info(proto, cid)
        self.irm = {}
        self.lltv = {}
        self.fee = {}
        self.ltv_hist = defaultdict(list)
        self.coll_asset = {}
        if proto in ("morpho", "moolah"):
            for r in jsonl(DATA / f"registry/markets/{proto}/{cid}.jsonl"):
                self.irm[r["market_id"]] = r.get("irm")
                self.lltv[r["market_id"]] = int(r.get("lltv") or 0) / 1e18
        elif proto == "euler_v2":
            rows = jsonl(DATA / f"registry/markets/euler_v2/{cid}.jsonl")
            for r in rows:
                if r.get("type") == "vault":
                    self.coll_asset[r["vault"]] = r["asset"]
            for r in rows:
                if r.get("type") == "ltv":
                    self.ltv_hist[r["vault"]].append(
                        (int(r["ts"]), r["collateral_vault"], int(r["liquidation_ltv"]) / 1e4))
            for v in self.ltv_hist:
                self.ltv_hist[v].sort()
            gov = DATA / f"panel/euler_v2/{cid}/governance.jsonl"
            if gov.exists():
                fees = defaultdict(list)
                for r in jsonl(gov):
                    if r.get("event") == "GovSetInterestFee":
                        fees[r["vault"]].append((int(r["timestamp"]), int(r["newFee"]) / 1e4))
                self.fee_hist = {v: sorted(x) for v, x in fees.items()}
            else:
                self.fee_hist = {}
        else:
            cfg = {r["silo"]: r for r in jsonl(DATA / f"panel/silo_v2/{cid}/configs.jsonl")}
            for s, r in cfg.items():
                self.fee[s] = (int(r.get("dao_fee") or 0) + int(r.get("deployer_fee") or 0)) / 1e18
                p = cfg.get(r.get("paired_silo"), {})
                self.lltv[s] = int(p.get("lt") or 0) / 1e18 if p else None

    def loan(self, m):
        return (self.info.get(m) or {}).get("loan")

    def markets_of_loan(self, token):
        return [m for m, r in self.info.items() if r.get("loan") == token]

    def collaterals(self, m, ts=None):
        """[(collateral token, collateral vault or None, lltv)] in force at ts."""
        row = self.info.get(m) or {}
        if self.proto == "euler_v2":
            cur = {}
            for t, cv, lt in self.ltv_hist.get(m, []):
                if ts is None or t <= ts:
                    cur[cv] = lt
            return [(self.coll_asset.get(cv), cv, lt) for cv, lt in cur.items() if lt > 0]
        out = []
        for tok, cv in row.get("collaterals") or []:
            out.append((tok, cv, self.lltv.get(m)))
        return out

    def oracle(self, m, collateral_vault=None):
        row = self.info.get(m) or {}
        if self.proto == "euler_v2":
            return (row.get("oracle_by_collateral") or {}).get(collateral_vault) or {}
        return row.get("oracle") or {}

    def interest_fee(self, m, ts):
        """Share of borrower interest that does not reach lenders."""
        if self.proto == "euler_v2":
            f = EULER_INIT_FEE
            for t, v in self.fee_hist.get(m, []):
                if t <= ts:
                    f = v
            return f
        if self.proto == "silo_v2":
            return self.fee.get(m, 0.0)
        return None


class DailySeries:
    """Day-end state of one market."""

    def __init__(self, proto, rows):
        self.proto = proto
        self.rows = rows
        self.days = [r[0] for r in rows]

    def at(self, day):
        i = bisect.bisect_right(self.days, day) - 1
        if i < 0:
            return None
        return self.rows[i]

    def aps(self, day):
        r = self.at(day)
        if r is None:
            return e2.aps_of(self.proto, 0, 0)
        return e2.aps_of(self.proto, r[1], r[3])

    def util(self, day):
        r = self.at(day)
        if r is None or r[1] <= 0:
            return 0.0
        return min(r[2] / r[1], 10.0)

    def pinned_runs(self, min_days=30, level=0.99):
        """[(first day, last day)] of runs of day-end utilization at or above `level` lasting at least `min_days` days (a run open."""
        if not self.rows:
            return []
        out, start = [], None
        for d in range(self.rows[0][0], self.rows[-1][0] + 1):
            if self.util(d) >= level:
                if start is None:
                    start = d
            elif start is not None:
                if d - start >= min_days:
                    out.append((start, d - 1))
                start = None
        if start is not None and self.rows[-1][0] + 1 - start >= min_days:
            out.append((start, self.rows[-1][0]))
        return out

    def util_min(self, day, days=30):
        """Lowest day-end utilization over the `days` day ends up to `day`."""
        return min(self.util(x) for x in range(day - days + 1, day + 1))

    def pinned_days(self, day, level=PINNED_U, cap=400):
        """Consecutive day ends up to `day` with utilization at or above `level` (0 when `day` itself is below it)."""
        n = 0
        while n < cap and self.at(day - n) is not None and self.util(day - n) >= level:
            n += 1
        return n

    def stuck(self, day, days=STUCK_DAYS, level=PINNED_U):
        """True when utilization was at or above `level` at every one of the `days` day ends up to `day`."""
        return all(self.util(x) >= level for x in range(day - days + 1, day + 1))


class PanelDaily:
    """Cached day-end series per market (pickles under the work directory, rebuilt when the panel CSV's size or mtime changes)."""

    def __init__(self):
        self.mem = {}

    def get(self, proto, cid, market):
        key = (proto, int(cid), market)
        if key in self.mem:
            return self.mem[key]
        src = e2.panel_dir(proto, cid) / f"{market}.csv"
        if not src.exists():
            self.mem[key] = None
            return None
        st = src.stat()
        cache = WORK / "panel_daily" / proto / str(cid) / f"{market}.pkl"
        if cache.exists():
            obj = load_pickle(cache)
            if obj["size"] == st.st_size and obj["mtime"] == st.st_mtime:
                self.mem[key] = DailySeries(proto, obj["rows"])
                return self.mem[key]
        rows = []
        apr = None
        cur_day = None
        last = None
        for block, li, ts, supply, borrow, shares, rate, _ev in e2.panel_rows(proto, src):
            d = ts // DAY
            if rate is not None:
                apr = rate
            if cur_day is not None and d != cur_day:
                rows.append(last)
            cur_day = d
            last = (d, supply, borrow, shares, apr)
        if last is not None:
            rows.append(last)
        save_pickle(cache, {"size": st.st_size, "mtime": st.st_mtime, "rows": rows})
        self.mem[key] = DailySeries(proto, rows)
        return self.mem[key]


class CorpusAssets:
    """Corpus asset table: collateral token -> corpus asset key (the unit a quality report is written for), with label, issuer name, members and peg."""

    def __init__(self):
        self.asset = {}
        self.by_token = {}
        p = DATA / "corpus/assets.jsonl"
        if not p.exists():
            return
        for r in jsonl(p):
            if r.get("row") == "asset":
                self.asset[r["asset"]] = r
            elif r.get("row") == "token":
                self.by_token[(int(r["chain_id"]), r["token"])] = r["asset"]
        for a, r in self.asset.items():
            for m in r.get("members") or []:
                self.by_token.setdefault((int(m["chain_id"]), m["token"]), a)

    def key_of(self, cid, token):
        return self.by_token.get((int(cid), token))

    def label(self, key):
        return (self.asset.get(key) or {}).get("label") or key

    def record(self, key):
        return self.asset.get(key) or {}


def family_key(assets, tokens, cid, token):
    """Corpus asset key of a collateral token; tokens outside the corpus table get a key of their own ('sym:<SYMBOL>') so they still have a report section of E1 facts."""
    k = assets.key_of(cid, token)
    if k:
        return k
    return "sym:" + (tokens.symbol(cid, token) or token[:10]).upper().replace("/", "-")


def euler_debt_shares(cid):
    """{lending vault: {day."""
    return e2.euler_distribution(cid)


def market_families(mk, assets, tokens, m, ts, dist=None):
    """[(family key, weight, collateral token, collateral vault, lltv)] of a market at ts."""
    cols = mk.collaterals(m, ts - 1)
    if mk.proto != "euler_v2":
        return [(family_key(assets, tokens, mk.cid, t), 1.0, t, cv, lt)
                for t, cv, lt in cols if t and t != ZERO]
    att = {}
    if dist is not None and m in dist:
        days = [d for d in dist[m] if d < day_of_ts(ts) and dist[m][d]]
        if days:
            att = dist[m][max(days)]
    total = sum(att.values())
    out = []
    for t, cv, lt in cols:
        if not t:
            continue
        w = att.get(cv, 0) / total if total else 1.0 / max(len(cols), 1)
        if total and w < 0.02:
            continue
        out.append((family_key(assets, tokens, mk.cid, t), w, t, cv, lt))
    return out


COMMON_START = e2.day_of("2025-10-02")
