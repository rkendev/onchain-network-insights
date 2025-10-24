# etl/pipeline.py
from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple

# Keep the existing imports in case other code paths use them
from etl import extract, load
from storage.sqlite_backend import SQLiteStorage


def _normalize_range(start_block: int, end_block: Optional[int]) -> Tuple[int, int]:
    """
    Ensure an inclusive window, never empty.
    """
    if end_block is None:
        end_block = start_block
    if end_block < start_block:
        start_block, end_block = end_block, start_block
    return int(start_block), int(end_block)


def _hex_to_int(x: Any) -> int:
    if isinstance(x, str) and x.startswith("0x"):
        return int(x, 16)
    return int(x or 0)


def _topic_to_address(topic_hex: str) -> str:
    """
    topics[1] and topics[2] are 32 byte values.
    The low 20 bytes are the address.
    """
    if not isinstance(topic_hex, str):
        return ""
    t = topic_hex.lower()
    if t.startswith("0x"):
        t = t[2:]
    # take the lower 40 hex chars
    addr_hex = t[-40:]
    return "0x" + addr_hex


def _is_erc20_transfer(log: Dict[str, Any]) -> bool:
    sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    topics = [str(t).lower() for t in (log.get("topics") or [])]
    return bool(topics) and topics[0] == sig


def _sqlite_persist_block(
    store: SQLiteStorage,
    bn: int,
    txs: List[Dict[str, Any]],
    logs: List[Dict[str, Any]],
) -> None:
    """
    Minimal persistence path for tests.
    Writes transactions and logs into the sqlite schema used by tests.
    Also derives transfers from ERC20 logs.
    """
    # transactions
    for tx in txs:
        store.write_transaction(tx)

    # logs plus derived transfers
    for lg in logs:
        store.write_log(lg)
        if _is_erc20_transfer(lg):
            tr = {
                "tx_hash": lg.get("transactionHash"),
                "contract": lg.get("address"),
                "sender": _topic_to_address(lg["topics"][1]) if len(lg.get("topics", [])) > 1 else "",
                "recipient": _topic_to_address(lg["topics"][2]) if len(lg.get("topics", [])) > 2 else "",
                "value": _hex_to_int(lg.get("data")),
                "block_number": _hex_to_int(lg.get("blockNumber", bn)),
            }
            store.write_transfer(tr)


def _safe_call_loader(fn, backend: str, payload, **kwargs) -> None:
    """
    Backward compatible call for external loaders when not using sqlite fast path.
    """
    try:
        fn(backend, payload, **kwargs)
    except TypeError:
        fn(backend, payload)


def run_etl(
    start_block: int,
    end_block: Optional[int] = None,
    backend: str = "sqlite",
    sqlite_path: Optional[str] = None,
    **opts,
) -> int:
    """
    Process an inclusive block window.
    Return transactions count plus raw logs count.
    """
    s, e = _normalize_range(start_block, end_block)

    total_tx = 0
    total_logs = 0

    # Fast path for sqlite used by the tests
    store: Optional[SQLiteStorage] = None
    if backend == "sqlite" and sqlite_path:
        store = SQLiteStorage(sqlite_path)
        store.setup()

    load_opts = dict(sqlite_path=sqlite_path)
    load_opts.update(opts)

    for bn in range(s, e + 1):
        raw = extract.extract_block(bn) or {}
        txs: List[Dict[str, Any]] = list(raw.get("transactions") or [])
        logs: List[Dict[str, Any]] = list(raw.get("logs") or [])

        if store is not None:
            _sqlite_persist_block(store, bn, txs, logs)
        else:
            _safe_call_loader(load.load_transactions, backend, txs, block_number=bn, **load_opts)
            _safe_call_loader(load.load_logs,         backend, logs, block_number=bn, **load_opts)

        total_tx += len(txs)
        total_logs += len(logs)

    return total_tx + total_logs
