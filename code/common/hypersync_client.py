"""Sync wrapper over the HyperSync SDK."""
import asyncio

from hypersync import (BlockField, ClientConfig, FieldSelection,
                       HypersyncClient, LogField, LogSelection, Query)

from . import env

LOG_FIELDS = [
    LogField.ADDRESS, LogField.DATA,
    LogField.TOPIC0, LogField.TOPIC1, LogField.TOPIC2, LogField.TOPIC3,
    LogField.BLOCK_NUMBER, LogField.LOG_INDEX, LogField.TRANSACTION_HASH,
]
BLOCK_FIELDS = [BlockField.NUMBER, BlockField.TIMESTAMP]


def _client(chain: str) -> HypersyncClient:
    return HypersyncClient(ClientConfig(
        url=f"https://{chain}.hypersync.xyz",
        bearer_token=env.require("HYPERSYNC_API_TOKEN"),
        max_num_retries=8,
        http_req_timeout_millis=180_000,
    ))


def get_height(chain: str) -> int:
    return asyncio.run(_client(chain).get_height())


def get_chain_id(chain: str) -> int:
    return asyncio.run(_client(chain).get_chain_id())


def _log_to_dict(lg) -> dict:
    topics = [t for t in (lg.topics or []) if t is not None]
    return {
        "address": lg.address,
        "topics": topics,
        "data": lg.data,
        "block_number": lg.block_number,
        "log_index": lg.log_index,
        "tx_hash": lg.transaction_hash,
    }


async def _scan(chain, addresses, topics, from_block, to_block):
    client = _client(chain)
    height = to_block if to_block is not None else await client.get_height()
    sel = LogSelection(
        address=[a for a in (addresses or [])] or None,
        topics=topics,
    )
    out, frm = [], from_block
    while frm < height:
        q = Query(from_block=frm, to_block=height,
                  logs=[sel], field_selection=FieldSelection(
                      log=LOG_FIELDS, block=BLOCK_FIELDS))
        r = await client.get(q)
        ts = {}
        for b in r.data.blocks or []:
            t = b.timestamp
            ts[b.number] = int(t, 16) if isinstance(t, str) else t
        for lg in r.data.logs:
            d = _log_to_dict(lg)
            d["timestamp"] = ts.get(lg.block_number)
            out.append(d)
        if r.next_block <= frm:
            break
        frm = r.next_block
    return out, height


def scan_logs(chain: str, addresses=None, topics=None, from_block=0,
              to_block=None):
    """Return (logs, scanned_to_block)."""
    return asyncio.run(_scan(chain, addresses, topics, from_block, to_block))


async def _scan_stream(chain, addresses, topics, from_block, to_block,
                       on_page):
    client = _client(chain)
    height = to_block if to_block is not None else await client.get_height()
    sel = LogSelection(
        address=[a for a in (addresses or [])] or None,
        topics=topics,
    )
    frm = from_block
    while frm < height:
        q = Query(from_block=frm, to_block=height,
                  logs=[sel], field_selection=FieldSelection(
                      log=LOG_FIELDS, block=BLOCK_FIELDS))
        r = await client.get(q)
        ts = {}
        for b in r.data.blocks or []:
            t = b.timestamp
            ts[b.number] = int(t, 16) if isinstance(t, str) else t
        page = []
        for lg in r.data.logs:
            d = _log_to_dict(lg)
            d["timestamp"] = ts.get(lg.block_number)
            page.append(d)
        if page:
            on_page(page)
        if r.next_block <= frm:
            break
        frm = r.next_block
    return height


def scan_logs_stream(chain: str, addresses=None, topics=None, from_block=0,
                     to_block=None, on_page=None):
    """Like scan_logs but calls on_page(list_of_log_dicts) per response page instead of accumulating everything in memory."""
    return asyncio.run(_scan_stream(chain, addresses, topics, from_block,
                                    to_block, on_page))


async def _first(chain, addresses, topics, from_block, to_block):
    client = _client(chain)
    sel = LogSelection(
        address=[a for a in (addresses or [])] or None,
        topics=topics,
    )
    frm = from_block
    while frm < to_block:
        q = Query(from_block=frm, to_block=to_block, logs=[sel],
                  max_num_logs=1,
                  field_selection=FieldSelection(log=LOG_FIELDS))
        r = await client.get(q)
        if r.data.logs:
            return _log_to_dict(r.data.logs[0])
        if r.next_block <= frm:
            break
        frm = r.next_block
    return None


def first_log(chain: str, addresses=None, topics=None, from_block=0,
              to_block=None):
    """The first matching log in [from_block, to_block), or None."""
    if to_block is None:
        to_block = get_height(chain)
    return asyncio.run(_first(chain, addresses, topics, from_block,
                              to_block))


async def _scan_multi(chain, selections, from_block, to_block):
    client = _client(chain)
    sels = [LogSelection(address=a or None, topics=t) for a, t in selections]
    out, frm = [], from_block
    fields = [LogField.ADDRESS, LogField.TOPIC0, LogField.TOPIC1,
              LogField.BLOCK_NUMBER]
    while frm < to_block:
        q = Query(from_block=frm, to_block=to_block, logs=sels,
                  field_selection=FieldSelection(log=fields))
        r = await client.get(q)
        for lg in r.data.logs:
            tp = [t for t in (lg.topics or []) if t is not None]
            out.append((lg.address.lower() if lg.address else None,
                        tp[0] if tp else None,
                        tp[1] if len(tp) > 1 else None,
                        lg.block_number))
        if r.next_block <= frm:
            break
        frm = r.next_block
    return out


def scan_logs_multi(chain: str, selections, from_block, to_block):
    """[(address, topic0, topic1, block)] of every log matching any of `selections` = [(addresses or None, topics or None)] in [from_block, to_block)."""
    return asyncio.run(_scan_multi(chain, selections, from_block, to_block))
