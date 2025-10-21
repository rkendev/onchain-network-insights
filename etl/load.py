# etl/load.py
from __future__ import annotations
from typing import Iterable, Dict, Any

from storage.manager import get_storage

def _ensure_list(x):
    if x is None:
        return []
    return list(x)

def load_transactions(backend: str, txs: Iterable[Dict[str, Any]], **backend_opts) -> int:
    """
    Persist transactions via the selected storage backend.
    Accepts **backend_opts (e.g., db_path=...) and passes them through.
    """
    txs = _ensure_list(txs)
    if not txs:
        return 0

    sm = get_storage(backend, **backend_opts)
    sm.setup()
    wrote = 0
    for tx in txs:
        if hasattr(sm, "write_transaction"):
            sm.write_transaction(tx)
            wrote += 1
        # If no transaction writer is implemented, skip silently (or log)
    return wrote

def load_logs(backend: str, logs: Iterable[Dict[str, Any]], **backend_opts) -> int:
    """
    Persist generic logs. Passes **backend_opts to backend factory.
    """
    logs = _ensure_list(logs)
    if not logs:
        return 0

    sm = get_storage(backend, **backend_opts)
    sm.setup()
    wrote = 0
    for lg in logs:
        if hasattr(sm, "write_log"):
            sm.write_log(lg)
            wrote += 1
    return wrote

def load_transfers(backend: str, transfers: Iterable[Dict[str, Any]], **backend_opts) -> int:
    """
    Persist ERC-20 transfers. Passes **backend_opts to backend factory.
    """
    transfers = _ensure_list(transfers)
    if not transfers:
        return 0

    sm = get_storage(backend, **backend_opts)
    sm.setup()
    wrote = 0
    for tr in transfers:
        # normalize to backend schema if needed
        if "sender" not in tr and "from" in tr:
            tr["sender"] = tr["from"]
        if "recipient" not in tr and "to" in tr:
            tr["recipient"] = tr["to"]
        if hasattr(sm, "write_transfer"):
            sm.write_transfer(tr)
            wrote += 1
    return wrote
