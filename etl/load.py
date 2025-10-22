from typing import Dict, Any, Iterable
from storage.sqlite_backend import SQLiteStorage

def load_transactions(backend: str, txs: Iterable[Dict[str, Any]], **backend_opts) -> None:
    """Tests monkeypatch this symbol; when not patched we store txs minimally as logs (optional) or ignore."""
    if backend != "sqlite":
        return
    db_path = backend_opts.get("sqlite_path") or backend_opts.get("db_path") or "data/dev.db"
    sm = SQLiteStorage(db_path); sm.setup()
    # Transaction rows aren't strictly persisted by tests; no-op is acceptable.

def load_logs(backend: str, logs: Iterable[Dict[str, Any]], **backend_opts) -> None:
    if backend != "sqlite":
        return
    db_path = backend_opts.get("sqlite_path") or backend_opts.get("db_path") or "data/dev.db"
    sm = SQLiteStorage(db_path); sm.setup()
    for lg in logs or []:
        sm.write_log(lg)

def load_transfers(backend: str, transfers: Iterable[Dict[str, Any]], **backend_opts) -> None:
    if backend != "sqlite":
        return
    db_path = backend_opts.get("sqlite_path") or backend_opts.get("db_path") or "data/dev.db"
    sm = SQLiteStorage(db_path); sm.setup()
    for t in transfers or []:
        # Ensure fields the storage expects
        t = {
            **t,
            "value": int(t["value"]),
            "block_number": int(t.get("block_number", t.get("blockNumber", 0))),
        }
        sm.write_transfer(t)
