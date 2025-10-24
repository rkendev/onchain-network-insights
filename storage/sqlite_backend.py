# storage/sqlite_backend.py
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional


class SQLiteStorage:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def setup(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS blocks(
              block_number INTEGER PRIMARY KEY,
              block_hash   TEXT NOT NULL,
              timestamp    INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS transactions(
              tx_hash      TEXT PRIMARY KEY,
              from_address TEXT,
              to_address   TEXT,
              value        TEXT
            );

            CREATE TABLE IF NOT EXISTS logs(
              tx_hash  TEXT,
              address  TEXT,
              data     TEXT,
              topics   TEXT
            );

            CREATE TABLE IF NOT EXISTS transfers(
              tx_hash      TEXT NOT NULL,
              contract     TEXT NOT NULL,
              sender       TEXT NOT NULL,
              recipient    TEXT NOT NULL,
              value        INTEGER NOT NULL,
              block_number INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_transfers_tx    ON transfers(tx_hash);
            CREATE INDEX IF NOT EXISTS idx_transfers_block ON transfers(block_number);
            """
        )
        self.conn.commit()

    def write_block(self, block: Dict[str, Any]) -> None:
        bn = int(block.get("block_number", 0))
        bh = str(block.get("block_hash", ""))
        ts = int(block.get("timestamp", 0))
        self.conn.execute(
            "INSERT OR REPLACE INTO blocks(block_number, block_hash, timestamp) VALUES(?,?,?)",
            (bn, bh, ts),
        )
        self.conn.commit()

    def write_transaction(self, tx: Dict[str, Any]) -> None:
        """
        Normalize value to a base 10 string so tests can do int(value).
        Accepts hex like 0x10 or decimal inputs.
        """
        tx_hash = tx.get("hash") or tx.get("tx_hash")
        from_addr = tx.get("from") or tx.get("from_address")
        to_addr = tx.get("to") or tx.get("to_address")

        v = tx.get("value")
        if v is None:
            value_str = None
        else:
            if isinstance(v, str) and v.startswith("0x"):
                value_str = str(int(v, 16))
            else:
                value_str = str(int(v))

        self.conn.execute(
            "INSERT OR REPLACE INTO transactions(tx_hash, from_address, to_address, value) VALUES(?,?,?,?)",
            (tx_hash, from_addr, to_addr, value_str),
        )
        self.conn.commit()

    def write_log(self, log: Dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO logs(tx_hash, address, data, topics) VALUES(?,?,?,?)",
            (
                log.get("transactionHash") or log.get("tx_hash"),
                log.get("address"),
                log.get("data"),
                ",".join(log.get("topics") or []),
            ),
        )
        self.conn.commit()

    def write_transfer(self, tr: Dict[str, Any]) -> None:
        tx_hash = tr.get("tx_hash") or tr.get("transactionHash")
        contract = tr.get("contract") or tr.get("address")
        sender = tr.get("sender") or tr.get("from") or tr.get("src")
        recipient = tr.get("recipient") or tr.get("to") or tr.get("dst")

        val = tr.get("value")
        if isinstance(val, str) and val.startswith("0x"):
            value = int(val, 16)
        else:
            value = int(val or 0)

        bn = tr.get("block_number") or tr.get("blockNumber")
        if isinstance(bn, str) and bn.startswith("0x"):
            block_number = int(bn, 16)
        else:
            block_number = int(bn or 0)

        self.conn.execute(
            """
            INSERT INTO transfers(tx_hash, contract, sender, recipient, value, block_number)
            VALUES(?,?,?,?,?,?)
            """,
            (tx_hash, contract, sender, recipient, value, block_number),
        )
        self.conn.commit()

    def read_block(self, block_number: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT block_number, block_hash, timestamp FROM blocks WHERE block_number = ?",
            (int(block_number),),
        ).fetchone()
        if not row:
            return None
        return {
            "block_number": int(row["block_number"]),
            "block_hash": row["block_hash"],
            "timestamp": int(row["timestamp"]),
        }

    def query_blocks(self, start_block: int, end_block: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT block_number, block_hash, timestamp
            FROM blocks
            WHERE block_number BETWEEN ? AND ?
            ORDER BY block_number ASC
            """,
            (int(start_block), int(end_block)),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "block_number": int(r["block_number"]),
                    "block_hash": r["block_hash"],
                    "timestamp": int(r["timestamp"]),
                }
            )
        return out
