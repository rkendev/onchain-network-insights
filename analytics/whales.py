# analytics/whales.py
from __future__ import annotations
from typing import Iterable, Dict, Any, List

from analytics.token_holders import balances_as_of_sqlite
from storage.sqlite_backend import SQLiteStorage


def find_whales_sqlite(
    db_path: str,
    contract: str,
    min_balance: int,
    as_of_block: int | None = None,
) -> List[Dict[str, Any]]:
    """
    Return holders whose balance >= min_balance, sorted DESC by balance.
    """
    balances = balances_as_of_sqlite(db_path, contract, as_of_block)
    whales = [b for b in balances if int(b["balance"]) >= int(min_balance)]
    # balances_as_of_sqlite already returns DESC by balance then address
    return whales


def _balances_direct_sqlite(
    db_path: str,
    contract: str,
    as_of_block: int | None,
) -> List[int]:
    """
    Compute balances directly from transfers, excluding the zero address
    (mints/burns) from holder totals.
    Returns a descending list of integer balances.
    """
    ZERO = "0x0000000000000000000000000000000000000000"

    sm = SQLiteStorage(db_path); sm.setup()
    cur = sm.conn.cursor()

    params = [contract]
    block_clause = ""
    if as_of_block is not None:
        block_clause = "AND block_number <= ?"
        params.append(as_of_block)

    # Exclude the zero address on both sides of the UNION so it never
    # contributes to holder balances or total supply.
    sql = f"""
    WITH moves AS (
        SELECT sender AS addr, -value AS delta
        FROM transfers
        WHERE contract = ? {block_clause} AND sender <> '{ZERO}'
        UNION ALL
        SELECT recipient AS addr, value AS delta
        FROM transfers
        WHERE contract = ? {block_clause} AND recipient <> '{ZERO}'
    ),
    agg AS (
        SELECT addr, COALESCE(SUM(delta), 0) AS balance
        FROM moves
        GROUP BY addr
        HAVING balance != 0
    )
    SELECT balance
    FROM agg
    ORDER BY balance DESC, addr ASC
    """
    cur.execute(sql, params + params)
    rows = cur.fetchall()
    return [int(r[0]) for r in rows]


def concentration_ratios_sqlite(
    db_path: str,
    contract: str,
    ks: Iterable[int] = (1, 5, 10, 50, 100),
    as_of_block: int | None = None,
) -> Dict[int, float]:
    """
    Concentration ratios CR_k = sum(top k balances) / sum(all balances).
    If there are fewer than k holders, CR_k uses all available holders.
    Returns {k: ratio in [0,1]}.
    """
    ks_list = [int(k) for k in ks]

    # Compute balances directly to avoid state/caching discrepancies.
    vals = _balances_direct_sqlite(db_path, contract, as_of_block)
    if not vals:
        return {k: 0.0 for k in ks_list}

    total = sum(vals)
    if total <= 0:
        return {k: 0.0 for k in ks_list}

    # Prefix sums for fast top-k totals
    prefix = []
    run = 0
    for v in vals:
        run += int(v)
        prefix.append(run)

    out: Dict[int, float] = {}
    n = len(prefix)
    for k in ks_list:
        kk = min(int(k), n)
        out[int(k)] = prefix[kk - 1] / total
    return out
