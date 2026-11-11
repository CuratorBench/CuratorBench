"""Information-cutoff protocol."""
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
import corpus

MARGINS = {
    "archive_capture": 0,
    "platform_api": 0,
    "page_metadata": 0,
    "http_last_modified": 0,
    "http_date": 0,
    "body_date": 6 * 3600,
    "inferred": None,
}
DAY_PRECISION_MARGIN = 36 * 3600

FUTURE_MARKERS = [
    (1736380800, re.compile(r"USD0\+\+.{0,80}(depeg|unpeg|floor price)",
                            re.I | re.S)),
    (1762128000, re.compile(r"\bxUSD\b.{0,80}(collaps|depeg|insolven)",
                            re.I | re.S)),
    (1762128000, re.compile(r"\bdeUSD\b.{0,80}(collaps|depeg|wind[- ]?down|sunset)",
                            re.I | re.S)),
    (1762214400, re.compile(r"Stream Finance.{0,120}(\$93 ?m|93 million)",
                            re.I | re.S)),
]


def margin(d):
    m = MARGINS[d["timestamp_evidence"]]
    if m is None:
        return None
    if d.get("published_precision") == "day":
        m += DAY_PRECISION_MARGIN
    return m


class CutoffView:
    """Read-only corpus view at decision time t (unix seconds)."""

    def __init__(self, t, docs=None, allow_inferred=False):
        self.t = int(t)
        self._docs = []
        for d in (docs if docs is not None else corpus.load_all()):
            corpus.validate_doc(d)
            m = margin(d)
            if m is None:
                if not allow_inferred:
                    continue
                m = DAY_PRECISION_MARGIN
            if corpus.visible_from(d) + m < self.t:
                self._docs.append(d)

    def __iter__(self):
        return iter(self._docs)

    def __len__(self):
        return len(self._docs)


def future_reference_flags(docs):
    """Docs whose claimed timestamp predates an event they reference."""
    flags = []
    for d in docs:
        for event_ts, pat in FUTURE_MARKERS:
            if corpus.visible_from(d) < event_ts and pat.search(d["text"]):
                flags.append({"id": d["id"], "source": d["source"],
                              "url": d["url"],
                              "claimed_ts": corpus.visible_from(d),
                              "marker_event_ts": event_ts})
    return flags


def _mk(url, text, ts, evidence, precision="second", retrieved=None,
        modified=None):
    return corpus.make_doc(
        source="selfcheck", source_type="other", url=url, final_url=url,
        assets=["selfcheck"], title=url, text=text, published_ts=ts,
        precision=precision, evidence=evidence, evidence_source="synthetic",
        retrieved_ts=retrieved if retrieved is not None else ts,
        method="synthetic", raw_sha256="0" * 64, raw_path="-",
        modified_ts=modified)


def self_check():
    """Inject synthetic future/mislabeled docs; the protocol must catch all."""
    t = 1736380800
    honest_past = _mk("u://past", "morpho vault market commentary",
                      t - 86400, "platform_api", retrieved=t)
    plain_future = _mk("u://future", "anything", t + 1, "platform_api",
                       retrieved=t + 2)
    boundary = _mk("u://boundary", "anything", t, "platform_api", retrieved=t)
    weak_ts_near = _mk("u://weak", "anything", t - 3600, "body_date",
                       retrieved=t)
    day_only = _mk("u://day", "anything", t - 86400, "page_metadata",
                   precision="day", retrieved=t)
    edited_after = _mk("u://edited", "anything", t - 10 * 86400,
                       "platform_api", retrieved=t + 10, modified=t + 5)
    inferred = _mk("u://inferred", "anything", t - 10 * 86400, "inferred",
                   retrieved=t)
    mislabeled = _mk("u://mislabeled",
                     "USD0++ has depegged and the floor price mechanism...",
                     t - 5 * 86400, "body_date", retrieved=t)
    docs = [honest_past, plain_future, boundary, weak_ts_near, day_only,
            edited_after, inferred, mislabeled]

    view = CutoffView(t, docs=docs)
    ids = {d["id"] for d in view}
    assert honest_past["id"] in ids, "honest past doc must be visible"
    assert plain_future["id"] not in ids, "future doc leaked"
    assert boundary["id"] not in ids, "boundary must be strict (<, not <=)"
    assert weak_ts_near["id"] not in ids, "weak-evidence margin not applied"
    assert day_only["id"] not in ids, "day-precision margin not applied"
    assert edited_after["id"] not in ids, "post-cutoff edit leaked"
    assert inferred["id"] not in ids, "inferred-timestamp doc not quarantined"

    flags = future_reference_flags(docs)
    assert any(f["id"] == mislabeled["id"] for f in flags), \
        "backdated event reference not flagged"

    tampered = dict(honest_past)
    tampered["text"] = tampered["text"] + " [edited]"
    try:
        CutoffView(t, docs=[tampered])
        raise AssertionError("tampered text passed hash check")
    except ValueError:
        pass

    print("leakage self-check: 9/9 assertions PASS")

    real = corpus.load_all()
    if real:
        rf = future_reference_flags(real)
        print(f"real corpus: {len(real)} docs, "
              f"{len(rf)} future-reference flags for manual review")
        for f in rf[:10]:
            print(f"  FLAG {f['source']} {f['url']}")
    else:
        print("real corpus: empty (crawlers not yet run)")


if __name__ == "__main__":
    self_check()
