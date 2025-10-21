# storage/manager.py
from __future__ import annotations
from typing import Any, Dict

from storage.sqlite_backend import SQLiteStorage

try:
    # optional; may not be present in CI
    from storage.postgres_backend import PostgresStorage  # type: ignore
except Exception:  # pragma: no cover
    PostgresStorage = None  # type: ignore


def get_storage(backend: str, **opts: Dict[str, Any]):
    """
    Factory for storage backends. Accepts flexible option names.
      - sqlite: db_path | sqlite_path | path
      - postgres: dsn or individual kwargs
    """
    b = (backend or "").lower()
    if b == "sqlite":
        db_path = opts.get("db_path") or opts.get("sqlite_path") or opts.get("path") or "data/dev.db"
        return SQLiteStorage(db_path)
    elif b in ("postgres", "postgresql", "pg"):
        if PostgresStorage is None:
            raise RuntimeError("Postgres backend requested but psycopg2 not available")
        dsn = opts.get("dsn")
        return PostgresStorage(dsn=dsn, **{k: v for k, v in opts.items() if k != "dsn"})
    else:
        raise ValueError(f"Unknown storage backend: {backend!r}")
