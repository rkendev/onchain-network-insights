import requests
from typing import Optional, Callable
from common.settings import load_settings, Settings
from ingestion.checkpoint import Checkpoint
from storage.manager import get_storage
from storage.sqlite_backend import SQLiteStorage

settings = load_settings()

def fetch_block(block_number: int) -> dict:
    """
    Fetch a single block by number over JSON-RPC (RPC mode).
    Returns the JSON block structure.
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


def fetch_transaction(tx_hash: str) -> dict:
    """
    Fetch a single transaction by hash via JSON-RPC.
    Returns the transaction JSON (or None if not found, depending on node behavior).
    """
    if not isinstance(tx_hash, str) or not tx_hash.startswith("0x"):
        raise ValueError("tx_hash must be a 0x-prefixed hex string")

    rpc_cfg = settings.rpc
    url = rpc_cfg.url
    timeout = rpc_cfg.timeout

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getTransactionByHash",
        "params": [tx_hash],
        "id": 1
    }

    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"RPC request failed for tx {tx_hash}") from e

    j = resp.json()
    if "error" in j:
        raise RuntimeError(f"RPC returned error: {j['error']}")
    return j.get("result")


def fetch_logs(address: str, from_block: int, to_block: int) -> list[dict]:
    """
    Fetch logs for a contract (or account) address between blocks, inclusive.
    Returns a list of log objects.
    """
    if not isinstance(address, str) or not address.startswith("0x"):
        raise ValueError("address must be a 0x-prefixed hex string")
    if not isinstance(from_block, int) or not isinstance(to_block, int):
        raise ValueError("from_block and to_block must be integers")
    if from_block < 0 or to_block < from_block:
        raise ValueError("invalid block range: from_block <= to_block and both >= 0 required")

    rpc_cfg = settings.rpc
    url = rpc_cfg.url
    timeout = rpc_cfg.timeout

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [
            {
                "address": address,
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block)
            }
        ],
        "id": 1
    }

    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(
            f"RPC request failed for logs address={address} range=({from_block},{to_block})"
        ) from e

    j = resp.json()
    if "error" in j:
        raise RuntimeError(f"RPC returned error: {j['error']}")
    return j.get("result", [])


def ingest_incremental(
    batch_size: int = 1000,
    checkpoint_path: Optional[str] = None,
    settings: Optional[Settings] = None,
    fetch_block_fn: Optional[Callable[[int], dict]] = None,
) -> int:
    st = settings or load_settings()
    cp_file = checkpoint_path or st.checkpoint.file
    cp = Checkpoint(cp_file)

    fetch = fetch_block_fn or fetch_block

    last = cp.get_last()
    start = (last + 1) if last is not None else 0
    end = start + batch_size - 1

    for blk in range(start, end + 1):
        _ = fetch(blk)  # TODO: parse/store

    cp.update(end)
    return end
    

def ingestion_pipeline(use_postgres: bool = False):
    if use_postgres:
        mgr: StorageManager = PostgresStorage(dsn="...")
    else:
        mgr = SQLiteStorage(path="data/storage.db")

    mgr.setup()
    # After fetch & parse steps:
    mgr.write_block(parsed_block)
    mgr.write_transaction(parsed_txn)
    mgr.write_log(parsed_log)
