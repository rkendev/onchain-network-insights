import os
import sqlite3
from typing import Dict, Any, Iterable, Optional, List

class SQLiteStorage:
    """
    Test-friendly storage with a persistent .conn handle and classic column names.
    Schema:
      blocks(block_number INTEGER PRIMARY KEY, block_hash TEXT, timestamp INTEGER)
      transfers(tx_hash TEXT PRIMARY KEY, contract TEXT, sender TEXT, recipient TEXT, value INTEGER, block_number INTEGER)
      logs(tx_hash TEXT, address TEXT, data TEXT, topics TEXT, block_number INTEGER)
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        # Persistent connection (tests expect .conn attribute)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")

    def setup(self) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS blocks(
              block_number INTEGER PRIMARY KEY,
              block_hash   TEXT,
              timestamp    INTEGER
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transfers(
              tx_hash      TEXT PRIMARY KEY,
              contract     TEXT NOT NULL,
              sender       TEXT,
              recipient    TEXT,
              value        INTEGER NOT NULL,
              block_number INTEGER NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS logs(
              tx_hash      TEXT,
              address      TEXT,
              data         TEXT,
              topics       TEXT,
              block_number INTEGER
            )
        """)
        self.conn.commit()

    def write_block(self, block: Dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO blocks(block_number, block_hash, timestamp) VALUES(?,?,?)",
            (int(block["block_number"]), block.get("block_hash"), int(block.get("timestamp", 0)))
        )
        self.conn.commit()

    def write_transfer(self, t: Dict[str, Any]) -> None:
        """Persist a single transfer. Preserve original casing/strings."""
        t_norm = {
            "tx_hash": t["tx_hash"],
            "contract": t["contract"],
            "sender": t["from"] if "from" in t else t.get("sender"),
            "recipient": t["to"] if "to" in t else t.get("recipient"),
            "value": int(t["value"]),
            "block_number": int(t["blockNumber"] if "blockNumber" in t else t.get("block_number")),
        }
        self.conn.execute(
            """INSERT OR REPLACE INTO transfers(tx_hash, contract, sender, recipient, value, block_number)
               VALUES (:tx_hash, :contract, :sender, :recipient, :value, :block_number)""",
            t_norm
        )
        self.conn.commit()

    def write_log(self, lg: Dict[str, Any]) -> None:
        topics = lg.get("topics") or []
        topics_txt = ",".join(topics) if isinstance(topics, list) else str(topics)
        self.conn.execute(
            """INSERT INTO logs(tx_hash, address, data, topics, block_number)
               VALUES(?,?,?,?,?)""",
            (
                lg.get("transactionHash") or lg.get("tx_hash"),
                lg.get("address"),
                lg.get("data"),
                topics_txt,
                int(lg.get("blockNumber", 0) if isinstance(lg.get("blockNumber", 0), int) else int(str(lg.get("blockNumber","0")).replace("0x",""), 16) if str(lg.get("blockNumber","0")).startswith("0x") else int(lg.get("blockNumber",0))),
            )
        )
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
