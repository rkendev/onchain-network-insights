import sqlite3
from typing import Dict, Any, Optional, List

def _hex_to_int(v):
    if isinstance(v, str) and v.startswith("0x"):
        return int(v, 16)
    return int(v)

class SQLiteStorage:
    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[sqlite3.Connection] = None

    def _ensure(self) -> None:
        if self.conn is not None:
            return
        self.setup()

    def setup(self) -> None:
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute("""
            CREATE TABLE IF NOT EXISTS blocks(
              block_number INTEGER PRIMARY KEY,
              block_hash   TEXT NOT NULL,
              timestamp    INTEGER NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS transactions(
              tx_hash      TEXT PRIMARY KEY,
              from_address TEXT,
              to_address   TEXT,
              value        INTEGER
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS logs(
              tx_hash  TEXT,
              address  TEXT,
              data     TEXT,
              topics   TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS transfers(
              tx_hash      TEXT,
              contract     TEXT,
              sender       TEXT,
              recipient    TEXT,
              value        INTEGER,
              block_number INTEGER
            )
        """)
        con.commit()
        self.conn = con

    def write_block(self, block: Dict[str, Any]) -> None:
        self._ensure()
        self.conn.execute(
            "INSERT OR REPLACE INTO blocks(block_number, block_hash, timestamp) VALUES(?,?,?)",
            (_hex_to_int(block["block_number"]), block["block_hash"], _hex_to_int(block["timestamp"])),
        )
        self.conn.commit()

    def read_block(self, block_number: int) -> Optional[Dict[str, Any]]:
        self._ensure()
        cur = self.conn.execute("SELECT * FROM blocks WHERE block_number = ?", (int(block_number),))
        row = cur.fetchone()
        return dict(row) if row else None

    def query_blocks(self, start: int, end: int) -> List[Dict[str, Any]]:
        self._ensure()
        cur = self.conn.execute(
            "SELECT * FROM blocks WHERE block_number BETWEEN ? AND ? ORDER BY block_number",
            (int(start), int(end)),
        )
        return [dict(r) for r in cur.fetchall()]

    def write_transaction(self, tx_hash: str, from_address: str, to_address: str, value: int) -> None:
        self._ensure()
        self.conn.execute(
            "INSERT OR REPLACE INTO transactions(tx_hash, from_address, to_address, value) VALUES(?,?,?,?)",
            (tx_hash, from_address, to_address, int(value)),
        )
        self.conn.commit()

    def write_transaction_dict(self, tx: Dict[str, Any]) -> None:
        self._ensure()
        tx_hash = tx.get("tx_hash") or tx.get("hash")
        from_address = tx.get("from") or tx.get("from_address")
        to_address = tx.get("to") or tx.get("to_address")
        value_raw = tx.get("value", 0)
        value = _hex_to_int(value_raw)
        self.write_transaction(tx_hash=tx_hash, from_address=from_address, to_address=to_address, value=value)

    def insert_transaction(self, *args, **kwargs) -> None:
        # handle the single positional dict case first
        if len(args) == 1 and isinstance(args[0], dict):
            return self.write_transaction_dict(args[0])
        # handle explicit positional args (tx_hash, from_address, to_address, value)
        if len(args) == 4:
            tx_hash, from_address, to_address, value = args
            return self.write_transaction(tx_hash, from_address, to_address, int(value))
        # handle full kwargs
        if {"tx_hash", "from_address", "to_address", "value"} <= set(kwargs):
            return self.write_transaction(**kwargs)
        # final fallback interpret kwargs as a tx dict
        if kwargs:
            return self.write_transaction_dict(kwargs)
        # nothing to write
        return None

    def write_log(self, lg: Dict[str, Any] = None, *, tx_hash: str = None, address: str = None, data: str = None, topics=None) -> None:
        self._ensure()
        if lg is not None:
            tx_hash = lg.get("transactionHash") or lg.get("tx_hash")
            address = lg["address"]
            data = lg.get("data", "")
            topics = lg.get("topics", [])
        topics_str = ",".join(topics) if isinstance(topics, list) else str(topics or "")
        self.conn.execute(
            "INSERT INTO logs(tx_hash, address, data, topics) VALUES(?,?,?,?)",
            (tx_hash, address, data, topics_str),
        )
        self.conn.commit()

    def insert_log(self, **kwargs) -> None:
        self.write_log(**kwargs)

    def write_transfer(self, tr: Dict[str, Any]) -> None:
        self._ensure()
        tx_hash = tr.get("tx_hash") or tr.get("transactionHash")
        contract = tr.get("contract") or tr.get("token") or ""
        sender = tr.get("sender") or tr.get("from")
        recipient = tr.get("recipient") or tr.get("to")
        value = _hex_to_int(tr["value"])
        bn = tr.get("block_number", tr.get("blockNumber"))
        block_number = _hex_to_int(bn)
        self.conn.execute(
            "INSERT INTO transfers(tx_hash, contract, sender, recipient, value, block_number) VALUES(?,?,?,?,?,?)",
            (tx_hash, contract, sender, recipient, value, block_number),
        )
        self.conn.commit()

    def insert_transfer(self, tr: Dict[str, Any]) -> None:
        self.write_transfer(tr)
