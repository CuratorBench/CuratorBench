"""Small ABI helpers: selectors, topic0, calldata and log codecs."""
import eth_abi
from eth_utils import keccak


def topic0(event_sig: str) -> str:
    """topic0('CreateMarket(bytes32,(address,address,address,address,uint256))')."""
    return "0x" + keccak(text=event_sig).hex()


def selector(func_sig: str) -> str:
    return "0x" + keccak(text=func_sig).hex()[:8]


def encode_call(func_sig: str, arg_types: list, args: list) -> str:
    data = selector(func_sig)
    if arg_types:
        data += eth_abi.encode(arg_types, args).hex()
    return data


def decode(types: list, hexdata: str):
    return eth_abi.decode(types, bytes.fromhex(hexdata.removeprefix("0x")))


def addr(word: str) -> str:
    """Extract an address from a 32-byte topic/word (lowercase, 0x-prefixed)."""
    return "0x" + word.removeprefix("0x")[-40:].lower()
