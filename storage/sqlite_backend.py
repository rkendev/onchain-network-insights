# storage/sqlite_backend.py
from __future__ import annotations

import os
import sqlite3
from . import schema
from typing import Optional, Dict, Any, List


class SQLiteStorage:
    """
    SQLite backend with:
      - blocks   : write_block, read_block, query_blocks
      - txs      : write_transaction
      - logs     : write_log
      - transfers: write_transfer
      - metadata : upsert_erc20_metadata, read_erc20_metadata
    """

    def __init__(self, db_path: str = "data/dev.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None

    # --------------------------- lifecycle ---------------------------

    def setup(self) -> None:
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        cur = self.conn.cursor()

        # Blocks
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS blocks (
                block_number BIGINT PRIMARY KEY,
                block_hash   TEXT,
                timestamp    BIGINT
            )
            """
        )

        # Transactions: canonical + legacy columns
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                tx_hash      TEXT PRIMARY KEY,
                from_address TEXT,
                to_address   TEXT,
                value        BIGINT,
                input        TEXT,
                block_number BIGINT,
                frm          TEXT,
                too          TEXT
            )
            """
        )

        # Logs
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                tx_hash      TEXT,
                address      TEXT,
                topics       TEXT,
                data         TEXT,
                block_number BIGINT
            )
            """
        )

        # Transfers (ERC-20)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS transfers (
                tx_hash      TEXT,
                contract     TEXT,
                sender       TEXT,
                recipient    TEXT,
                value        BIGINT,
                block_number BIGINT,
                PRIMARY KEY (tx_hash, contract, sender, recipient, block_number)
            )
            """
        )

        # ERC-20 metadata cache
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS erc20_metadata (
                contract            TEXT PRIMARY KEY,
                symbol              TEXT,
                decimals            INTEGER,
                total_supply        BIGINT,
                last_updated_block  BIGINT
            )
            """
        )

        cur.executescript(schema.BALANCES_VIEW_SQL)

        self.conn.commit()

    # ----------------------------- blocks ----------------------------

    def write_block(self, blk: Dict[str, Any]) -> None:
        self.setup()
        cur = self.conn.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO blocks (block_number, block_hash, timestamp)
               VALUES (?, ?, ?)""",
            (
                int(blk.get("block_number") or blk.get("number") or 0),
                blk.get("block_hash") or blk.get("hash"),
                int(blk.get("timestamp") or 0),
            ),
        )
        self.conn.commit()

    def read_block(self, number: int) -> Optional[Dict[str, Any]]:
        self.setup()
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT block_number, block_hash, timestamp FROM blocks WHERE block_number=?",
            (int(number),),
        ).fetchone()
        if not row:
            return None
        return {"block_number": row[0], "block_hash": row[1], "timestamp": row[2]}

    def query_blocks(self, start: int, end: int) -> List[Dict[str, Any]]:
        self.setup()
        cur = self.conn.cursor()
        rows = cur.execute(
            "SELECT block_number, block_hash, timestamp "
            "FROM blocks WHERE block_number BETWEEN ? AND ? ORDER BY block_number",
            (int(start), int(end)),
        ).fetchall()
        return [{"block_number": r[0], "block_hash": r[1], "timestamp": r[2]} for r in rows]

    # ---------------------------- helpers ----------------------------

    @staticmethod
    def _parse_value(val: Any) -> int:
        if isinstance(val, str):
            return int(val, 16) if val.startswith("0x") else int(val)
        return int(val or 0)

    @staticmethod
    def _norm_from(tx: Dict[str, Any]) -> Optional[str]:
        return (
            tx.get("from") or tx.get("from_address") or tx.get("frm")
            or tx.get("sender") or tx.get("src")
        )

    @staticmethod
    def _norm_to(tx: Dict[str, Any]) -> Optional[str]:
        return (
            tx.get("to") or tx.get("to_address") or tx.get("too")
            or tx.get("recipient") or tx.get("dst")
        )

    # ---------------------------- writers ----------------------------

    def write_transaction(self, tx: Dict[str, Any]) -> None:
        self.setup()
        cur = self.conn.cursor()
        tx_hash = tx.get("tx_hash") or tx.get("hash")
        from_addr = self._norm_from(tx)
        to_addr = self._norm_to(tx)
        value = self._parse_value(tx.get("value", 0))
        input_hex = tx.get("input") or ""
        block_no = int(tx.get("block_number") or tx.get("blockNumber") or 0)
        cur.execute(
            """INSERT OR REPLACE INTO transactions
               (tx_hash, from_address, to_address, value, input, block_number, frm, too)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (tx_hash, from_addr, to_addr, value, input_hex, block_no, from_addr, to_addr),
        )
        self.conn.commit()

    def write_log(self, lg: Dict[str, Any]) -> None:
        self.setup()
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO logs (tx_hash, address, topics, data, block_number)
               VALUES (?, ?, ?, ?, ?)""",
            (
                lg.get("transactionHash") or lg.get("tx_hash"),
                lg.get("address"),
                ",".join(lg.get("topics", [])),
                lg.get("data"),
                int(lg.get("blockNumber") or lg.get("block_number") or 0),
            ),
        )
        self.conn.commit()

    def write_transfer(self, tr: Dict[str, Any]) -> None:
        self.setup()
        cur = self.conn.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO transfers
               (tx_hash, contract, sender, recipient, value, block_number)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                tr.get("tx_hash"),
                str(tr.get("contract")).lower() if tr.get("contract") else None,
                tr.get("sender") or tr.get("from"),
                tr.get("recipient") or tr.get("to"),
                self._parse_value(tr.get("value", 0)),
                int(tr.get("blockNumber") or tr.get("block_number") or 0),
            ),
        )
        self.conn.commit()

    # --------------------------- metadata ----------------------------

    def upsert_erc20_metadata(
        self,
        contract: str,
        symbol: str,
        decimals: int,
        total_supply: int,
        as_of_block: Optional[int] = None,
    ) -> None:
        self.setup()
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO erc20_metadata (contract, symbol, decimals, total_supply, last_updated_block)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(contract) DO UPDATE SET
                symbol=excluded.symbol,
                decimals=excluded.decimals,
                total_supply=excluded.total_supply,
                last_updated_block=excluded.last_updated_block
            """,
            (contract.lower(), symbol, int(decimals), int(total_supply), as_of_block if as_of_block is not None else 0),
        )
        self.conn.commit()

    def read_erc20_metadata(self, contract: str) -> Optional[dict]:
        self.setup()
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT contract, symbol, decimals, total_supply, last_updated_block "
            "FROM erc20_metadata WHERE contract=?",
            (contract.lower(),),
        ).fetchone()
        if not row:
            return None
        return {
            "contract": row[0],
            "symbol": row[1] or "",
            "decimals": int(row[2] or 0),
            "total_supply": int(row[3] or 0),
            "as_of_block": int(row[4] or 0),
        }
