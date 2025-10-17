import os
from storage.sqlite_backend import SQLiteStorage
from storage.postgres_backend import PostgresStorage

def _get_storage(backend: str, *, sqlite_path: str | None = None, pg_dsn: str | None = None):
    if backend == "sqlite":
        db_path = sqlite_path or os.environ.get("ETL_SQLITE_PATH") or "data/storage.db"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        return SQLiteStorage(path=db_path)
    if backend == "postgres":
        dsn = pg_dsn or os.environ.get("ETL_PG_DSN") or "postgresql://user:pass@localhost:5432/db"
        return PostgresStorage(dsn=dsn)
    raise ValueError(f"Unknown backend: {backend}")

def load_transactions(backend: str, txs: list[dict], *, sqlite_path: str | None = None, pg_dsn: str | None = None):
    sm = _get_storage(backend, sqlite_path=sqlite_path, pg_dsn=pg_dsn)
    sm.setup()
    for tx in txs:
        sm.write_transaction(tx)

def load_logs(backend: str, logs: list[dict], *, sqlite_path: str | None = None, pg_dsn: str | None = None):
    sm = _get_storage(backend, sqlite_path=sqlite_path, pg_dsn=pg_dsn)
    sm.setup()
    for lg in logs:
        sm.write_log(lg)
