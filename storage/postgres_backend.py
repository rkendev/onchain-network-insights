import psycopg2
from typing import Dict, Any, Optional, List
from .manager import StorageManager
from .schema import CREATE_TABLE_BLOCKS, CREATE_TABLE_TXS, CREATE_TABLE_LOGS
from .schema import (
    CREATE_TABLE_BLOCKS, CREATE_TABLE_TXS, CREATE_TABLE_LOGS, CREATE_TABLE_TRANSFERS
)

class PostgresStorage(StorageManager):
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None

    def setup(self) -> None:
        self.conn = psycopg2.connect(self.dsn)
        cur = self.conn.cursor()
        cur.execute(CREATE_TABLE_BLOCKS)
        cur.execute(CREATE_TABLE_TXS)
        cur.execute(CREATE_TABLE_LOGS)
        cur.execute(CREATE_TABLE_TRANSFERS)
        self.conn.commit()

    def write_block(self, block: Dict[str, Any]) -> None:
        sql = """
        INSERT INTO blocks (block_number, block_hash, timestamp)
        VALUES (%s, %s, %s)
        ON CONFLICT (block_number) DO UPDATE SET block_hash = EXCLUDED.block_hash
        """
        data = (block["block_number"], block["block_hash"], block["timestamp"])
        cur = self.conn.cursor()
        cur.execute(sql, data)
        self.conn.commit()

    def read_block(self, block_number: int) -> Optional[Dict[str, Any]]:
        sql = "SELECT block_number, block_hash, timestamp FROM blocks WHERE block_number = %s"
        cur = self.conn.cursor()
        cur.execute(sql, (block_number,))
        r = cur.fetchone()
        if r:
            return {"block_number": r[0], "block_hash": r[1], "timestamp": r[2]}
        return None

    def write_transaction(self, tx: Dict[str, Any]) -> None:
        sql = """
        INSERT INTO transactions (tx_hash, from_address, to_address, value)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (tx_hash) DO NOTHING
        """
        val = (tx["tx_hash"], tx.get("from"), tx.get("to"), tx.get("value"))
        cur = self.conn.cursor()
        cur.execute(sql, val)
        self.conn.commit()

    def write_log(self, log: Dict[str, Any]) -> None:
        sql = """
        INSERT INTO logs (tx_hash, address, data)
        VALUES (%s, %s, %s)
        ON CONFLICT (tx_hash, address) DO NOTHING
        """
        val = (log.get("transactionHash"), log.get("address"), log.get("data"))
        cur = self.conn.cursor()
        cur.execute(sql, val)
        self.conn.commit()

    def query_blocks(self, start: int, end: int) -> List[Dict[str, Any]]:
        sql = """
        SELECT block_number, block_hash, timestamp
        FROM blocks
        WHERE block_number BETWEEN %s AND %s
        ORDER BY block_number
        """
        cur = self.conn.cursor()
        cur.execute(sql, (start, end))
        rows = cur.fetchall()
        return [{"block_number": r[0], "block_hash": r[1], "timestamp": r[2]} for r in rows]


    def write_transfer(self, tr: dict) -> None:
        sql = """
        INSERT INTO transfers
        (tx_hash, contract, sender, recipient, value, block_number)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (tx_hash, contract, sender, recipient, block_number) DO NOTHING
        """
        data = (
            tr["tx_hash"],
            tr.get("contract"),
            tr.get("from") or tr.get("sender"),
            tr.get("to") or tr.get("recipient"),
            int(tr.get("value", 0)),
            tr.get("blockNumber") or tr.get("block_number"),
        )
        cur = self.conn.cursor()
        cur.execute(sql, data)
        self.conn.commit()


    def load_transfers(backend: str, transfers: list[dict], *, sqlite_path: str | None = None, pg_dsn: str | None = None):
        sm = _get_storage(backend, sqlite_path=sqlite_path, pg_dsn=pg_dsn)
        sm.setup()
        for tr in transfers:
            # normalize keys to what backends expect
            if "sender" not in tr and "from" in tr:
                tr["sender"] = tr["from"]
            if "recipient" not in tr and "to" in tr:
                tr["recipient"] = tr["to"]
            sm.write_transfer(tr)
