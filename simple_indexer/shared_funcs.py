def hex_to_bytes20(addr: str) -> bytes:  # hex to bytes for addresses
    # expects '0x' + 40 hex
    a = addr.lower()
    if a.startswith("0x"):
        a = a[2:]
    if len(a) != 40:
        raise ValueError(f"Bad address length: {addr}")
    return bytes.fromhex(a)


def hex_to_bytes32(h: str) -> bytes:  # hex to bytes for tx hashes
    # expects '0x' + 64 hex, or sometimes without 0x
    x = h.lower()
    if x.startswith("0x"):
        x = x[2:]
    if len(x) != 64:
        raise ValueError(f"Bad hash length: {h}")
    return bytes.fromhex(x)


def parse_block_number(x):  # block number from hex to int if needed
    if isinstance(x, int):
        return x
    return int(x, 16)