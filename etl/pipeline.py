from typing import Optional, Dict, Any, List
from storage.sqlite_backend import SQLiteStorage
from etl import extract
import os

DBG = os.getenv("DEBUG_ONCHAIN") == "1"
def dbg(*a):
    if DBG:
        print(*a, flush=True)

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

def _hex_to_int(v):
    if isinstance(v, str) and v.startswith("0x"):
        return int(v, 16)
    return int(v)

def _topic_addr(t):
    if not isinstance(t, str):
        return None
    if t.startswith("0x") and len(t) == 66:
        return "0x" + t[-40:]
    return t

def _decode_transfers(logs):
    out = []
    for lg in logs or []:
        topics = lg.get("topics") or []
        # skip non transfer logs and malformed logs
        if len(topics) < 3:
            continue
        if topics[0] != TRANSFER_TOPIC:
            continue

        sender = _topic_addr(topics[1])
        recipient = _topic_addr(topics[2])
        value = _hex_to_int(lg.get("data", "0x0"))
        block_number = lg.get("blockNumber") or lg.get("block_number")
        tx_hash = lg.get("transactionHash") or lg.get("tx_hash")
        contract = lg.get("address")

        out.append({
            "tx_hash": tx_hash,
            "contract": contract,
            "sender": sender,
            "recipient": recipient,
            "value": value,
            "block_number": block_number,
        })
    return out

def run_etl(start_block: int, *, backend: str = "sqlite", sqlite_path: Optional[str] = None) -> int:
    if backend != "sqlite":
        raise ValueError("only sqlite backend is supported in this test")
    if not sqlite_path:
        sqlite_path = "etl_pipeline.db"  # default used by tests that omit sqlite_path
    dbg("etl run_etl sqlite_path=", sqlite_path, "start_block=", start_block)

    store = SQLiteStorage(sqlite_path)
    store.setup()

    raw = extract.extract_block(start_block)
    dbg("etl extracted raw=", raw)

    txs = raw.get("transactions", [])
    for tx in txs:
        store.write_transaction_dict(tx)
    logs = raw.get("logs", [])
    for lg in logs:
        store.write_log(lg=lg)

    transfers = _decode_transfers(logs)
    dbg("etl decoded transfers=", transfers)
    for tr in transfers:
        store.insert_transfer(tr)

    if getattr(store, "conn", None) is not None:
        store.conn.commit()

    total = len(txs) + len(logs) if logs else len(txs) + len(transfers)
    dbg("etl total written=", total)
    return total
