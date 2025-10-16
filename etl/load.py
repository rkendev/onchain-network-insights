import os
from storage.sqlite_backend import SQLiteStorage
from storage.postgres_backend import PostgresStorage


def _get_storage(backend: str):
    if backend == "sqlite":
        db_path = "data/storage.db"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        return SQLiteStorage(path=db_path)
    if backend == "postgres":
        return PostgresStorage(dsn="postgresql://user:pass@localhost:5432/db")
    raise ValueError("Unknown backend: " + backend)


def load_transactions(backend: str, txs: list[dict]) -> None:
    s = _get_storage(backend)
    s.setup()
    for tx in txs:
        s.write_transaction(tx)
        

def load_logs(backend: str, logs: list[dict]) -> None:
    s = _get_storage(backend)
    s.setup()
    for lg in logs:
        s.write_log(lg)
