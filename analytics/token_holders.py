from typing import Dict, List, Optional
import sqlite3

DBPath = str

def _connect(db: DBPath) -> sqlite3.Connection:
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    return con

def balances_as_of_sqlite(db: DBPath, contract: str, as_of_block: Optional[int] = None) -> List[Dict]:
    """
    Mirror of holders.holder_balances_sqlite kept for backward calls
    """
    con = _connect(db)
    where = "1=1"
    params = {}
    if as_of_block is not None:
        where += " AND block_number <= :asof"
        params["asof"] = int(as_of_block)
    sql = f"""
        WITH deltas AS (
          SELECT recipient AS addr, value  AS delta FROM transfers WHERE {where}
          UNION ALL
          SELECT sender    AS addr, -value AS delta FROM transfers WHERE {where}
        )
        SELECT addr AS address, SUM(delta) AS balance
        FROM deltas
        GROUP BY addr
        HAVING balance != 0
        ORDER BY balance DESC
    """
    rows = con.execute(sql, params).fetchall()
    return [dict(r) for r in rows]

def top_holders_sqlite(db: DBPath, contract: str, n: int = 10, as_of_block: Optional[int] = None) -> List[Dict]:
    return balances_as_of_sqlite(db, contract, as_of_block)[: int(n)]
