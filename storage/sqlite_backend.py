from __future__ import annotations

import sqlite3
from typing import Dict, Any, Optional, List, Tuple

ZERO = "0x" + "0" * 40

def _hex_to_int(v):
    if isinstance(v, str) and v.startswith("0x"):
        return int(v, 16)
    return int(v)

def _as_decstr(v) -> str:
    """Return a base 10 string for any int like or hex string value."""
    if v is None:
        return "0"
    if isinstance(v, str) and v.startswith("0x"):
        return str(int(v, 16))
    return str(int(v))

class SQLiteStorage:
    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[sqlite3.Connection] = None

    def _ensure(self) -> None:
        if self.conn is not None:
            return
        self.setup()

    def _exec(self, sql: str, params: Tuple = ()) -> None:
        self.conn.execute(sql, params)

    def _drop_all_views(self) -> None:
        """
        Drop every view before any DDL that renames or replaces tables.
        This prevents recompile failures on views that reference legacy schema.
        """
        cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='view'")
        names = [r[0] for r in cur.fetchall()]
        for name in names:
            self._exec(f"DROP VIEW IF EXISTS {name};")

    def _create_views(self) -> None:
        """
        Create views compatible with current schema.
        transfers_norm exposes block_number as blockNumber to keep older code working.
        transfers_enriched builds on top of transfers_norm.
        """
        self._exec("""
            CREATE VIEW IF NOT EXISTS transfers_norm AS
            SELECT
              tx_hash,
              contract,
              sender   AS from_addr,
              recipient AS to_addr,
              value,
              block_number AS blockNumber
            FROM transfers;
        """)

        self._exec(f"""
            CREATE VIEW IF NOT EXISTS transfers_enriched AS
            SELECT
              t.tx_hash,
              t.contract,
              t.from_addr,
              t.to_addr,
              t.value,
              t.blockNumber,
              CASE
                WHEN t.from_addr = '{ZERO}' AND t.to_addr   != '{ZERO}' THEN 'mint'
                WHEN t.to_addr   = '{ZERO}' AND t.from_addr != '{ZERO}' THEN 'burn'
                ELSE 'transfer'
              END AS direction,
              CASE
                WHEN t.from_addr = '{ZERO}' THEN CAST(t.value AS TEXT)
                WHEN t.to_addr   = '{ZERO}' THEN CAST(-1 * CAST(t.value AS INTEGER) AS TEXT)
                ELSE '0'
              END AS base_delta
            FROM transfers_norm t;
        """)

        # optional convenience view for positive balances per address and contract
        self._exec(f"""
            CREATE VIEW IF NOT EXISTS balances_view AS
            WITH src AS (
              SELECT contract, to_addr   AS address, CAST(value AS INTEGER)     AS delta, blockNumber
                FROM transfers_enriched
               WHERE direction IN ('transfer','mint')
              UNION ALL
              SELECT contract, from_addr AS address, -CAST(value AS INTEGER)    AS delta, blockNumber
                FROM transfers_enriched
               WHERE direction IN ('transfer','burn')
            )
            SELECT contract, address, SUM(delta) AS balance
              FROM src
             WHERE address != '{ZERO}'
             GROUP BY contract, address
            HAVING balance > 0;
        """)

    def _maybe_migrate_value_columns_to_text(self) -> None:
        """
        Ensure transactions.value and transfers.value use TEXT affinity.
        Drop all views first to avoid recompile errors, then recreate views.
        """
        def coltype(table: str, col: str) -> Optional[str]:
            cur = self.conn.execute(f"PRAGMA table_info({table})")
            for _, name, ctype, *_ in cur.fetchall():
                if name == col:
                    return ctype.upper() if ctype else None
            return None

        # drop any views that could depend on old schemas
        self._drop_all_views()

        # migrate transactions.value
        if coltype("transactions", "value") not in ("TEXT", None):
            self._exec("""
                CREATE TABLE IF NOT EXISTS transactions_new(
                  tx_hash      TEXT PRIMARY KEY,
                  from_address TEXT,
                  to_address   TEXT,
                  value        TEXT
                );
            """)
            self._exec("""
                INSERT OR REPLACE INTO transactions_new(tx_hash, from_address, to_address, value)
                SELECT tx_hash, from_address, to_address, CAST(value AS TEXT) FROM transactions;
            """)
            self._exec("DROP TABLE transactions;")
            self._exec("ALTER TABLE transactions_new RENAME TO transactions;")

        # migrate transfers.value
        if coltype("transfers", "value") not in ("TEXT", None):
            self._exec("""
                CREATE TABLE IF NOT EXISTS transfers_new(
                  tx_hash      TEXT,
                  contract     TEXT,
                  sender       TEXT,
                  recipient    TEXT,
                  value        TEXT,
                  block_number INTEGER
                );
            """)
            self._exec("""
                INSERT INTO transfers_new(tx_hash, contract, sender, recipient, value, block_number)
                SELECT tx_hash, contract, sender, recipient, CAST(value AS TEXT), block_number FROM transfers;
            """)
            self._exec("DROP TABLE transfers;")
            self._exec("ALTER TABLE transfers_new RENAME TO transfers;")

        self.conn.commit()

        # recreate compatible views after migrations
        self._create_views()

    def setup(self) -> None:
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        # base schema
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
              value        TEXT
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
              value        TEXT,
              block_number INTEGER
            )
        """)
        con.commit()
        self.conn = con
        # migrate and recreate views
        self._maybe_migrate_value_columns_to_text()

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

    def write_transaction(self, tx_hash: str, from_address: str, to_address: str, value: int | str) -> None:
        """
        Persist a transaction. Value is stored as base 10 text to avoid 64 bit overflow.
        """
        self._ensure()
        self.conn.execute(
            "INSERT OR REPLACE INTO transactions(tx_hash, from_address, to_address, value) VALUES(?,?,?,?)",
            (tx_hash, from_address, to_address, _as_decstr(value)),
        )
        self.conn.commit()

    def write_transaction_dict(self, tx: Dict[str, Any]) -> None:
        self._ensure()
        tx_hash = tx.get("tx_hash") or tx.get("hash")
        from_address = tx.get("from") or tx.get("from_address")
        to_address = tx.get("to") or tx.get("to_address")
        value_dec = _as_decstr(tx.get("value", 0))
        self.write_transaction(tx_hash=tx_hash, from_address=from_address, to_address=to_address, value=value_dec)

    def insert_transaction(self, *args, **kwargs) -> None:
        if len(args) == 1 and isinstance(args[0], dict):
            return self.write_transaction_dict(args[0])
        if len(args) == 4:
            tx_hash, from_address, to_address, value = args
            return self.write_transaction(tx_hash, from_address, to_address, value)
        if {"tx_hash", "from_address", "to_address", "value"} <= set(kwargs):
            return self.write_transaction(**kwargs)
        if kwargs:
            return self.write_transaction_dict(kwargs)
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
        """
        Persist a transfer. Value is stored as base 10 text to avoid 64 bit overflow.
        """
        self._ensure()
        tx_hash = tr.get("tx_hash") or tr.get("transactionHash")
        contract = tr.get("contract") or tr.get("token") or ""
        sender = tr.get("sender") or tr.get("from")
        recipient = tr.get("recipient") or tr.get("to")
        value_dec = _as_decstr(tr.get("value"))
        bn = tr.get("block_number", tr.get("blockNumber"))
        block_number = _hex_to_int(bn)
        self.conn.execute(
            "INSERT INTO transfers(tx_hash, contract, sender, recipient, value, block_number) VALUES(?,?,?,?,?,?)",
            (tx_hash, contract, sender, recipient, value_dec, block_number),
        )
        self.conn.commit()

    def insert_transfer(self, tr: Dict[str, Any]) -> None:
        self.write_transfer(tr)
