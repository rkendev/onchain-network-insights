from __future__ import annotations

import os
import sys
from pathlib import Path

def _env_int(name: str, default: int | None) -> int | None:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        print(f"WARN {name} is not an int, got {v}, using default {default}", file=sys.stderr)
        return default

def main() -> None:
    rpc = os.getenv("RPC_URL")
    if not rpc:
        print("ERROR RPC_URL must be set", file=sys.stderr)
        sys.exit(2)
    os.environ["RPC_URL"] = rpc

    sqlite_path = os.getenv("SQLITE_PATH", "/app/data/dev.db")
    Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)

    start_block = _env_int("START_BLOCK", 0) or 0
    end_block = _env_int("END_BLOCK", None)
    contract = os.getenv("CONTRACT") or None

    # optional tunables
    os.environ.setdefault("ETH_LOGS_CHUNK", os.getenv("ETH_LOGS_CHUNK", "50"))
    os.environ.setdefault("RPC_RPS", os.getenv("RPC_RPS", "3"))
    os.environ.setdefault("RPC_MAX_RETRIES", os.getenv("RPC_MAX_RETRIES", "6"))
    os.environ.setdefault("RPC_BASE_SLEEP", os.getenv("RPC_BASE_SLEEP", "0.5"))

    print("INGEST starting")
    print(f"DB {sqlite_path}")
    if end_block is not None and start_block > end_block:
        print("ERROR START_BLOCK must be less than or equal to END_BLOCK", file=sys.stderr)
        sys.exit(2)

    from etl.pipeline import run_etl
    rows = run_etl(
        start_block,
        end_block=end_block,
        backend="sqlite",
        sqlite_path=sqlite_path,
        contract=contract,
    )
    print(f"INGEST done. total rows written {rows}")

if __name__ == "__main__":
    main()
