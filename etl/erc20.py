# etl/erc20.py
from typing import Optional

# keccak256("Transfer(address,address,uint256)")
TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

def _strip_0x(s: str) -> str:
    return s[2:] if isinstance(s, str) and s.startswith("0x") else s

def _hex_to_addr(topic_32bytes: str) -> str:
    # topic is 32-byte hex; last 20 bytes are the address
    t = _strip_0x(topic_32bytes) or ""
    if len(t) < 40:
        # fallback; not enough data
        return "0x" + t[-40:].rjust(40, "0")
    return "0x" + t[-40:]

def _hex_to_int(hex_or_int) -> int:
    if hex_or_int is None:
        return 0
    if isinstance(hex_or_int, int):
        return hex_or_int
    s = str(hex_or_int).lower()
    return int(s, 16) if s.startswith("0x") else int(s)

def is_erc20_transfer(log: dict) -> bool:
    topics = log.get("topics") or []
    if not topics or not isinstance(topics, list):
        return False
    t0 = str(topics[0]).lower()
    return t0 == TRANSFER_TOPIC0

def decode_erc20_transfer(log: dict) -> Optional[dict]:
    """
    Decode an ERC-20 Transfer log into a normalized record:
    {
      "tx_hash": str,
      "contract": str,
      "from": str,
      "to": str,
      "value": int,
      "blockNumber": int (if present)
    }
    Returns None if the log doesn't match or missing fields.
    """
    if not is_erc20_transfer(log):
        return None

    topics = log.get("topics") or []
    if len(topics) < 3:
        return None  # need from/to indexed topics

    from_addr = _hex_to_addr(topics[1])
    to_addr = _hex_to_addr(topics[2])

    # value is in data (32-byte)
    value_hex = log.get("data", "0x0")
    value = _hex_to_int(value_hex)

    tx_hash = log.get("transactionHash")
    contract = log.get("address")
    block_num = log.get("blockNumber")
    block_int = _hex_to_int(block_num) if isinstance(block_num, str) else block_num

    return {
        "tx_hash": tx_hash,
        "contract": contract,
        "from": from_addr,
        "to": to_addr,
        "value": value,
        "blockNumber": block_int,
    }
