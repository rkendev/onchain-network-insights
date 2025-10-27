# streaming/sqlite_sink.py
from __future__ import annotations

import sqlite3
from typing import Dict, Any

from storage.sqlite_backend import SQLiteStorage


class SQLiteSink:
    """
    Wraps SQLiteStorage and adds an idempotent 'seen' ledger per (topic, key).
    """
    def __init__(self, path: str):
        self.storage = SQLiteStorage(path)
        self.storage.setup()
        self.conn: sqlite3.Connection = self.storage.conn
        self._setup_seen()

    def _setup_seen(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS streaming_dedup(
              topic TEXT NOT NULL,
              msg_key TEXT NOT NULL,
              PRIMARY KEY(topic, msg_key)
            );
            """
        )
        self.conn.commit()

    def mark_seen(self, topic: str, key: str) -> bool:
        """
        Returns True if this (topic, key) is new, False if seen before.
        """
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO streaming_dedup(topic, msg_key) VALUES(?, ?)",
            (topic, key),
        )
        self.conn.commit()
        return cur.rowcount == 1

    # ——— parse and write helpers ———

    def write_tx_message(self, msg_value: Dict[str, Any]) -> None:
        """
        Message schema expected from historical_feeder/producer:
        includes 'hash', 'from' (or from_address), 'to', 'value'
        """
        self.storage.write_transaction(msg_value)

    def write_log_message(self, msg_value: Dict[str, Any]) -> None:
        """
        Message schema expected from historical_feeder/producer:
        includes 'transactionHash', 'address', 'data', 'topics'
        """
        self.storage.write_log(msg_value)

    def write_transfer_message(self, msg_value: Dict[str, Any]) -> None:
        """
        Optional: if you later publish 'transfers' topic, this will persist it.
        Expected keys:
          tx_hash or transactionHash, contract|address, sender|from|src, recipient|to|dst,
          value (hex or int), block_number|blockNumber
        """
        self.storage.write_transfer(msg_value)
