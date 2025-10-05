import requests
from common.settings import load_settings

settings = load_settings()

def fetch_block(block_number: int) -> dict:
    """
    Fetch a single block by number over JSON-RPC (RPC mode).
    Returns the JSON block structure.
    Raises error for invalid responses or transport issues.
    """
    if block_number < 0:
        raise ValueError("block_number must be non-negative")

    rpc_cfg = settings.rpc
    url = rpc_cfg.url
    timeout = rpc_cfg.timeout

    hex_block = hex(block_number)
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex_block, True],
        "id": 1
    }

    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"RPC request failed for block {block_number}") from e

    resp_json = resp.json()
    if "error" in resp_json:
        raise RuntimeError(f"RPC returned error: {resp_json['error']}")
    result = resp_json.get("result")
    if result is None:
        raise RuntimeError("RPC response missing result field")

    return result
