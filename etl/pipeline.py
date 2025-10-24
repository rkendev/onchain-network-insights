from __future__ import annotations

import os
import time
import random
import math
import requests
from typing import Optional, Iterable, Dict, Any, List

from storage.sqlite_backend import SQLiteStorage

TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

def _rpc_url() -> str:
    return os.environ.get("RPC_URL") or "https://example.invalid"

def _int_env(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(v) if v not in (None, "") else default
    except ValueError:
        return default

def _float_env(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(v) if v not in (None, "") else default
    except ValueError:
        return default

# controls you can tweak via env
LOG_CHUNK = _int_env("ETH_LOGS_CHUNK", 50)            # number of blocks per eth_getLogs
MAX_RETRIES = _int_env("RPC_MAX_RETRIES", 6)          # times to retry 429 or 5xx
BASE_SLEEP = _float_env("RPC_BASE_SLEEP", 0.5)        # base seconds for backoff
RATE_LIMIT_RPS = _float_env("RPC_RPS", 3.0)           # max requests per second

_last_request_ts = 0.0

def _rate_limit():
    global _last_request_ts
    if RATE_LIMIT_RPS <= 0:
        return
    now = time.time()
    min_interval = 1.0 / RATE_LIMIT_RPS
    elapsed = now - _last_request_ts
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    _last_request_ts = time.time()

def _rpc_post(method: str, params: list, timeout: float = 30.0) -> Any:
    url = _rpc_url()
    attempt = 0
    while True:
        _rate_limit()
        try:
            resp = requests.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=timeout)
            # handle http status first
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"{resp.status_code} server retryable")
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                # some providers also encode rate limit in json error
                code = data["error"].get("code")
                msg = str(data["error"])
                if code in (429, -32005) or "rate" in msg.lower() or "too many" in msg.lower():
                    raise requests.HTTPError("429 in json error")
                raise RuntimeError(f"RPC error for {method}: {data['error']}")
            return data["result"]
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            sleep_s = BASE_SLEEP * (2 ** (attempt - 1))
            sleep_s = min(sleep_s, 12.0)
            sleep_s = sleep_s * (0.8 + 0.4 * random.random())
            time.sleep(sleep_s)

def _hex_to_int(x: str) -> int:
    return int(x, 16)

def _topic_to_address(t: str) -> str:
    t = t.lower()
    if not t.startswith("0x"):
        raise ValueError("topic does not start with 0x")
    return "0x" + t[-40:]

def _decode_transfer_log(lg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    topics = lg.get("topics") or []
    if not topics or topics[0].lower() != TRANSFER_TOPIC0:
        return None
    try:
        sender = _topic_to_address(topics[1])
        recipient = _topic_to_address(topics[2])
        amount = int(lg.get("data", "0x0"), 16)
        bn = _hex_to_int(lg["blockNumber"])
        return {
            "tx_hash": lg["transactionHash"],
            "contract": lg["address"],
            "sender": sender,
            "recipient": recipient,
            "value": amount,
            "block_number": bn,
        }
    except Exception:
        return None

def _fetch_logs_range(start_bn: int, end_bn: int, contract: Optional[str]) -> List[Dict[str, Any]]:
    params = {
        "fromBlock": hex(start_bn),
        "toBlock": hex(end_bn),
        # filter by topic0 so providers can optimize
        "topics": [TRANSFER_TOPIC0],
    }
    if contract:
        params["address"] = contract
    return _rpc_post("eth_getLogs", [params])

def run_etl(
    start_block: int,
    *,
    end_block: Optional[int] = None,
    backend: str = "sqlite",
    sqlite_path: str = "data/dev.db",
    contract: Optional[str] = None,
) -> int:
    if backend != "sqlite":
        raise ValueError("only sqlite backend is supported by this minimal pipeline")

    sm = SQLiteStorage(sqlite_path)
    sm.setup()

    lo = int(start_block)
    hi = lo if end_block is None else int(end_block)
    if hi < lo:
        raise ValueError("end_block must be greater than or equal to start_block")

    total_rows = 0
    chunk = max(1, int(LOG_CHUNK))
    rngs = []
    cur = lo
    while cur <= hi:
        r_end = min(cur + chunk - 1, hi)
        rngs.append((cur, r_end))
        cur = r_end + 1

    for s, e in rngs:
        logs = _fetch_logs_range(s, e, contract)
        wrote = 0
        for lg in logs:
            tr = _decode_transfer_log(lg)
            if tr is None:
                continue
            sm.write_transfer(tr)
            wrote += 1
        total_rows += wrote
        print(f"Blocks {s}..{e} transfers {wrote}")

    print(f"ETL done. transfers written {total_rows}")
    return total_rows
