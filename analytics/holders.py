from __future__ import annotations
from typing import Dict, List, Tuple, Optional
import sqlite3
import math

def _connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)

def holder_balances_sqlite(db_path: str, contract: str, as_of_block: Optional[int] = None) -> List[Dict[str, int]]:
    """
    Returns list of {"address": str, "balance": int} as of (<=) as_of_block.
    Uses transfers(sender, recipient, value, block_number).
    """
    con = _connect(db_path)
    params = {"contract": contract}
    clause = "contract = :contract"
    if as_of_block is not None:
        clause += " AND block_number <= :asof"
        params["asof"] = int(as_of_block)
    sql = f"""
      WITH deltas AS (
        SELECT recipient AS address, value      AS delta FROM transfers WHERE {clause}
        UNION ALL
        SELECT sender    AS address, -1*value   AS delta FROM transfers WHERE {clause}
      )
      SELECT address, SUM(delta) AS balance
      FROM deltas
      GROUP BY address
      HAVING balance != 0
      ORDER BY balance DESC
    """
    rows = con.execute(sql, params).fetchall()
    return [{"address": a, "balance": int(b)} for (a, b) in rows]

def holder_deltas_sqlite(db_path: str, contract: str, start_block: int, end_block: int) -> Dict[str, int]:
    """
    Net balance change between (start_block, end_block].
    """
    before = holder_balances_sqlite(db_path, contract, as_of_block=start_block)
    after  = holder_balances_sqlite(db_path, contract, as_of_block=end_block)
    bmap = {x["address"]: int(x["balance"]) for x in before}
    amap = {x["address"]: int(x["balance"]) for x in after}
    addrs = set(bmap) | set(amap)
    return {a: int(amap.get(a, 0) - bmap.get(a, 0)) for a in addrs}

def top_gainers_sqlite(db_path: str, contract: str, n: int, start_block: int, end_block: int) -> List[str]:
    deltas = holder_deltas_sqlite(db_path, contract, start_block, end_block)
    ordered = sorted(deltas.items(), key=lambda kv: kv[1], reverse=True)
    return [a for a, d in ordered if d > 0][:int(n)]

def top_spenders_sqlite(db_path: str, contract: str, n: int, start_block: int, end_block: int) -> List[str]:
    deltas = holder_deltas_sqlite(db_path, contract, start_block, end_block)
    ordered = sorted(deltas.items(), key=lambda kv: kv[1])  # most negative first
    return [a for a, d in ordered if d < 0][:int(n)]

def distribution_metrics_sqlite(db_path: str, contract: str, as_of_block: Optional[int] = None) -> Dict[str, float]:
    """
    Returns {"holders", "total_supply", "gini", "hhi", "last_block"}.
    holders/total_supply are counts from positive balances only for distribution.
    """
    con = _connect(db_path)
    bals = holder_balances_sqlite(db_path, contract, as_of_block)
    positives = [int(x["balance"]) for x in bals if int(x["balance"]) > 0 and str(x["address"]).lower() != "0x0000000000000000000000000000000000000000"]
    holders = len(positives)
    total = int(sum(positives)) if holders else 0

    # gini
    if holders <= 1 or total == 0:
        gini = 0.0
    else:
        xs = sorted(positives)
        n = len(xs)
        cum = sum((i + 1) * x for i, x in enumerate(xs))
        gini = (2 * cum) / (n * total) - (n + 1) / n

    # HHI (shares squared sum)
    hhi = 0.0
    if total > 0:
        shares = [x / total for x in positives]
        hhi = float(sum(s * s for s in shares))

    # last block (respect as_of if provided)
    if as_of_block is not None:
        last = con.execute(
            "SELECT COALESCE(MAX(block_number),0) FROM transfers WHERE contract=? AND block_number <= ?",
            (contract, int(as_of_block))
        ).fetchone()[0]
    else:
        last = con.execute(
            "SELECT COALESCE(MAX(block_number),0) FROM transfers WHERE contract=?",
            (contract,)
        ).fetchone()[0]

    return {
        "holders": float(holders),
        "total_supply": float(total),
        "gini": float(gini),
        "hhi": float(hhi),
        "last_block": float(last),
    }
