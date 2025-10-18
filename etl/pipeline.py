# etl/pipeline.py
import etl.extract as extract
import etl.transform as transform
import etl.load as load
from etl.erc20 import is_erc20_transfer  # NEW


def _pick(raw: dict, key: str):
    """
    Safely get 'key' from either a flat dict or a nested {"result": {...}} response.
    """
    if not isinstance(raw, dict):
        return None
    if key in raw:
        return raw.get(key)
    res = raw.get("result")
    if isinstance(res, dict):
        return res.get(key)
    return None


def run_etl(block_number: int, backend: str = "sqlite", **backend_opts) -> int:
    """
    End-to-end ETL for a single block:
      - extract raw block
      - transform transactions, logs, and ERC-20 transfers
      - load transactions, non-ERC20 logs, and transfers
      - return count of DISTINCT stored records
    """
    raw = extract.extract_block(block_number)

    raw_txs = _pick(raw, "transactions") or []
    raw_logs = _pick(raw, "logs") or []
    if not isinstance(raw_txs, list):
        raw_txs = []
    if not isinstance(raw_logs, list):
        raw_logs = []

    # Decode ERC-20 transfers first
    transfers = transform.decode_erc20_transfers(raw_logs)

    # Exclude ERC-20 transfer logs from the generic logs path to avoid double-counting
    non_transfer_logs = [lg for lg in raw_logs if not is_erc20_transfer(lg)]

    # Transform remaining entities
    txs = transform.transform_transactions(raw_txs)
    logs = transform.transform_logs(non_transfer_logs)

    # Load into storage
    load.load_transactions(backend, txs, **backend_opts)
    load.load_logs(backend, logs, **backend_opts)
    load.load_transfers(backend, transfers, **backend_opts)

    # Return distinct records persisted: txs + non-transfer logs + transfers
    return len(txs) + len(logs) + len(transfers)
