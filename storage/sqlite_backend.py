import os
import sqlite3
from typing import Dict, Any, Optional, List
from .manager import StorageManager
from .schema import CREATE_TABLE_BLOCKS, CREATE_TABLE_TXS, CREATE_TABLE_LOGS
from .schema import (
    CREATE_TABLE_BLOCKS, CREATE_TABLE_TXS, CREATE_TABLE_LOGS, CREATE_TABLE_TRANSFERS
)

class SQLiteStorage(StorageManager):
    def __init__(self, path: str):
        self.path = path
        self.conn = None

    def setup(self) -> None:
        # Ensure parent directory exists
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        c = self.conn.cursor()
        c.execute(CREATE_TABLE_BLOCKS)
        c.execute(CREATE_TABLE_TXS)
        c.execute(CREATE_TABLE_LOGS)
        c.execute(CREATE_TABLE_TRANSFERS)
        self.conn.commit()

    def write_block(self, block: Dict[str, Any]) -> None:
        sql = "INSERT OR REPLACE INTO blocks (block_number, block_hash, timestamp) VALUES (?, ?, ?)"
        data = (block["block_number"], block["block_hash"], block["timestamp"])
        self.conn.execute(sql, data)
        self.conn.commit()

    def read_block(self, block_number: int) -> Optional[Dict[str, Any]]:
        sql = "SELECT block_number, block_hash, timestamp FROM blocks WHERE block_number = ?"
        cur = self.conn.execute(sql, (block_number,))
        row = cur.fetchone()
        if row:
            return {"block_number": row[0], "block_hash": row[1], "timestamp": row[2]}
        return None

    def write_transaction(self, tx: Dict[str, Any]) -> None:
        sql = "INSERT OR REPLACE INTO transactions (tx_hash, from_address, to_address, value) VALUES (?, ?, ?, ?)"
        data = (tx["tx_hash"], tx.get("from"), tx.get("to"), tx.get("value"))
        self.conn.execute(sql, data)
        self.conn.commit()

    def write_log(self, log: Dict[str, Any]) -> None:
        sql = "INSERT OR REPLACE INTO logs (tx_hash, address, data) VALUES (?, ?, ?)"
        data = (log.get("transactionHash"), log.get("address"), log.get("data"))
        self.conn.execute(sql, data)
        self.conn.commit()

    def query_blocks(self, start: int, end: int) -> List[Dict[str, Any]]:
        sql = "SELECT block_number, block_hash, timestamp FROM blocks WHERE block_number BETWEEN ? AND ? ORDER BY block_number"
        cur = self.conn.execute(sql, (start, end))
        rows = cur.fetchall()
        return [
            {"block_number": r[0], "block_hash": r[1], "timestamp": r[2]} for r in rows
        ]

    def write_transfer(self, tr: dict) -> None:
        sql = """
        INSERT OR REPLACE INTO transfers
        (tx_hash, contract, sender, recipient, value, block_number)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        data = (
            tr["tx_hash"],
            tr.get("contract"),
            tr.get("from") or tr.get("sender"),
            tr.get("to") or tr.get("recipient"),
            int(tr.get("value", 0)),
            tr.get("blockNumber") or tr.get("block_number"),
        )
        self.conn.execute(sql, data)
        self.conn.commit()
