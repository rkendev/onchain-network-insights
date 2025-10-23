import requests
from typing import Optional, Callable, List, Dict

from common.settings import load_settings, Settings
from ingestion.checkpoint import Checkpoint
from storage.sqlite_backend import SQLiteStorage
from storage.manager import get_storage

_settings: Optional[Settings] = None

def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings

def _rpc_post(method: str, params: list) -> Dict:
    st = get_settings()
    url = st.rpc.url
    timeout = getattr(st.rpc, "timeout", 30)
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"RPC transport failed for {method}") from e
    data = resp.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(f"RPC error for {method}: {data['error']}")
    return data

def fetch_block(block_number: int) -> dict:
    if not isinstance(block_number, int) or block_number < 0:
        raise ValueError("block_number must be a non negative integer")
    hex_block = hex(block_number)
    data = _rpc_post("eth_getBlockByNumber", [hex_block, True])
    result = data.get("result")
    if result is None:
        raise RuntimeError("RPC response missing result for eth_getBlockByNumber")
    return result

def fetch_transaction(tx_hash: str) -> Optional[dict]:
    if not isinstance(tx_hash, str) or not tx_hash.startswith("0x"):
        raise ValueError("tx_hash must be a 0x prefixed hex string")
    data = _rpc_post("eth_getTransactionByHash", [tx_hash])
    return data.get("result")

def fetch_logs(address: str, from_block: int, to_block: int) -> List[dict]:
    if not isinstance(address, str) or not address.startswith("0x"):
        raise ValueError("address must be a 0x prefixed hex string")
    if not isinstance(from_block, int) or not isinstance(to_block, int):
        raise ValueError("from_block and to_block must be integers")
    if from_block < 0 or to_block < from_block:
        raise ValueError("invalid block range")
    params = [{"address": address, "fromBlock": hex(from_block), "toBlock": hex(to_block)}]
    data = _rpc_post("eth_getLogs", params)
    result = data.get("result", [])
    if not isinstance(result, list):
        raise RuntimeError("RPC response for eth_getLogs did not return a list")
    return result

def _get_storage():
    st = get_settings()
    if getattr(st.db, "driver", "sqlite") == "sqlite":
        return SQLiteStorage(path=st.db.sqlite_path)
    return get_storage(st.db)

def ingest_incremental(
    batch_size: int = 1000,
    checkpoint_path: Optional[str] = None,
    settings_override: Optional[Settings] = None,
    fetch_block_fn: Optional[Callable[[int], dict]] = None,
) -> int:
    st = settings_override or get_settings()
    cp_file = checkpoint_path or st.checkpoint.file
    cp = Checkpoint(cp_file)

    fetch = fetch_block_fn or fetch_block
    storage = _get_storage()
    storage.setup()

    last = cp.get_last()
    if last is None:
        start = 0
    else:
        start = last + 1

    end = start + batch_size - 1

    for blk_num in range(start, end + 1):
        block = fetch(blk_num)
        # storage.write_block(block)  left commented until schema is finalized
        pass

    cp.update(end)
    return end

__all__ = ["fetch_block", "fetch_transaction", "fetch_logs", "ingest_incremental", "get_settings"]
