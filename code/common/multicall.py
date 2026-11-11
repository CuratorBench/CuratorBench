"""Multicall3 batching for eth_call."""
import eth_abi

from . import evmabi, rpc

MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
_SEL = evmabi.selector("aggregate3((address,bool,bytes)[])")


def aggregate3(chain: str, calls, block="latest", chunk=400):
    """calls: list of (target, calldata_hex)."""
    results = []
    for i in range(0, len(calls), chunk):
        batch_calls = calls[i:i + chunk]
        batch = [(t, True, bytes.fromhex(cd.removeprefix("0x")))
                 for t, cd in batch_calls]
        data = _SEL + eth_abi.encode(["(address,bool,bytes)[]"], [batch]).hex()
        raw = rpc.eth_call(chain, MULTICALL3, data, block=block)
        if raw in ("0x", ""):
            for t, cd in batch_calls:
                try:
                    results.append((True, rpc.eth_call(chain, t, cd, block=block)))
                except Exception:
                    results.append((False, "0x"))
            continue
        decoded = eth_abi.decode(["(bool,bytes)[]"],
                                 bytes.fromhex(raw.removeprefix("0x")))[0]
        results.extend((ok, "0x" + ret.hex()) for ok, ret in decoded)
    return results


def try_view(chain: str, targets, func_sig: str, ret_types, block="latest"):
    """Call the same no-arg view on many targets."""
    calldata = evmabi.selector(func_sig)
    out = []
    for ok, ret in aggregate3(chain, [(t, calldata) for t in targets], block):
        if not ok or ret in ("0x", ""):
            out.append(None)
            continue
        try:
            out.append(evmabi.decode(ret_types, ret))
        except Exception:
            out.append(None)
    return out
