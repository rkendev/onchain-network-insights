from __future__ import annotations

from typing import Dict, List, Tuple, Optional, Union
import sqlite3

DBLike = Union[str, sqlite3.Connection]

__all__ = [
    "balances_as_of_sqlite",
    "top_holders_sqlite",
    "distribution_metrics",
    "holder_deltas_window",
    "top_gainers_spenders",
]

def _connect(db: DBLike) -> sqlite3.Connection:
    """Accept either a sqlite path or an open connection."""
    if isinstance(db, sqlite3.Connection):
        return db
    return sqlite3.connect(db)

def balances_as_of_sqlite(db: DBLike, contract: str, as_of_block: Optional[int] = None) -> Dict[str, int]:
    """
    Return address -> balance using transfers up to (and including) as_of_block.
    Expects a 'transfers' table with columns: contract, "from", "to", value, blockNumber.
    """
    con = _connect(db)
    params = {"contract": contract}
    where = 'contract = :contract'
    if as_of_block is not None:
        where += ' AND blockNumber <= :asof'
        params["asof"] = int(as_of_block)
    sql = f"""
        WITH deltas AS (
          SELECT "to"   AS addr, value  AS delta FROM transfers WHERE {where}
          UNION ALL
          SELECT "from" AS addr, -value AS delta FROM transfers WHERE {where}
        )
        SELECT addr, SUM(delta) AS bal
        FROM deltas
        GROUP BY addr
        HAVING bal != 0
    """
    rows = con.execute(sql, params).fetchall()
    return {addr: int(bal) for addr, bal in rows}

def top_holders_sqlite(db: DBLike, contract: str, n: int = 10, as_of_block: Optional[int] = None) -> List[Tuple[str, int]]:
    bals = balances_as_of_sqlite(db, contract, as_of_block=as_of_block)
    return sorted(bals.items(), key=lambda x: x[1], reverse=True)[:int(n)]

def distribution_metrics(db: DBLike, contract: str, as_of_block: Optional[int] = None) -> Dict[str, float]:
    """Return {holders, total_supply, gini, cr10, last_block} for the contract."""
    con = _connect(db)
    bals = balances_as_of_sqlite(con, contract, as_of_block=as_of_block)
    holders = len(bals)
    total = sum(bals.values()) if holders else 0

    # gini
    if holders <= 1 or total == 0:
        gini = 0.0
    else:
        xs = sorted(bals.values())
        cum = 0
        for i, x in enumerate(xs, 1):
            cum += i * x
        gini = (2 * cum) / (holders * total) - (holders + 1) / holders

    top10 = sum(v for _, v in sorted(bals.items(), key=lambda x: x[1], reverse=True)[:10])
    cr10 = (top10 / total * 100.0) if total else 0.0

    # last_block (<= as_of_block if provided; otherwise max for contract)
    if as_of_block is not None:
        last = con.execute(
            "SELECT COALESCE(MAX(blockNumber), 0) FROM transfers WHERE contract = ? AND blockNumber <= ?",
            (contract, int(as_of_block)),
        ).fetchone()[0]
    else:
        last = con.execute(
            "SELECT COALESCE(MAX(blockNumber), 0) FROM transfers WHERE contract = ?",
            (contract,),
        ).fetchone()[0]

    return {
        "holders": float(holders),
        "total_supply": float(total),
        "gini": float(gini),
        "cr10": float(cr10),
        "last_block": float(last),
    }

def holder_deltas_window(
    db: DBLike,
    contract: str,
    start_block_exclusive: int,
    end_block_inclusive: int,
) -> Dict[str, int]:
    """Balance change between (start,end] — start is exclusive, end is inclusive."""
    before = balances_as_of_sqlite(db, contract, as_of_block=start_block_exclusive)
    after  = balances_as_of_sqlite(db, contract, as_of_block=end_block_inclusive)
    addrs = set(before) | set(after)
    return {a: int(after.get(a, 0) - before.get(a, 0)) for a in addrs}

def top_gainers_spenders(
    db: DBLike,
    contract: str,
    start_block_exclusive: int,
    end_block_inclusive: int,
    top: int = 5,
) -> Tuple[List[str], List[str]]:
    """Return (gainers, spenders) lists ordered by absolute delta size."""
    deltas = holder_deltas_window(db, contract, start_block_exclusive, end_block_inclusive)
    gainers = [a for a, d in sorted(deltas.items(), key=lambda x: x[1], reverse=True) if d > 0][:top]
    spenders = [a for a, d in sorted(deltas.items(), key=lambda x: x[1]) if d < 0][:top]
    return gainers, spenders
