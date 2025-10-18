# analytics/holders.py
from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple

from analytics.token_holders import balances_as_of_sqlite
from storage.sqlite_backend import SQLiteStorage


def holder_balances_sqlite(
    db_path: str, contract: str, as_of_block: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Thin wrapper around balances_as_of_sqlite to centralize holder analytics entry-point.
    Returns [{address, balance}] sorted by balance DESC, then address ASC.
    """
    return balances_as_of_sqlite(db_path, contract, as_of_block)


def _address_deltas_sqlite(
    db_path: str, contract: str, start_block: int, end_block: int
) -> List[Tuple[str, int]]:
    """
    Compute net delta per address in (start_block, end_block] (exclusive of start, inclusive of end),
    using the transfers table directly. Positive = net inflow, Negative = net outflow.
    Returns list of (address, delta) including only non-zero deltas, sorted by |delta| DESC then address.
    """
    sm = SQLiteStorage(db_path)
    sm.setup()
    cur = sm.conn.cursor()

    # Filter transfers strictly after start_block up to and including end_block
    # (so you can use start_block as the previous snapshot boundary).
    sql = """
    WITH windowed AS (
        SELECT sender AS addr, -value AS delta
        FROM transfers
        WHERE contract = ? AND block_number > ? AND block_number <= ?
        UNION ALL
        SELECT recipient AS addr, value AS delta
        FROM transfers
        WHERE contract = ? AND block_number > ? AND block_number <= ?
    ),
    agg AS (
        SELECT addr, COALESCE(SUM(delta),0) AS delta
        FROM windowed
        GROUP BY addr
        HAVING delta <> 0
    )
    SELECT addr, delta
    FROM agg
    ORDER BY ABS(delta) DESC, addr ASC
    """
    params = [contract, start_block, end_block, contract, start_block, end_block]
    cur.execute(sql, params)
    return [(r[0], int(r[1])) for r in cur.fetchall()]


def holder_deltas_sqlite(
    db_path: str, contract: str, start_block: int, end_block: int
) -> List[Dict[str, Any]]:
    """
    Return list of {address, delta} for the given window.
    """
    return [
        {"address": addr, "delta": delta}
        for addr, delta in _address_deltas_sqlite(db_path, contract, start_block, end_block)
    ]


def top_gainers_sqlite(
    db_path: str, contract: str, n: int, start_block: int, end_block: int
) -> List[Dict[str, Any]]:
    """
    Addresses with largest positive delta in the window.
    """
    rows = _address_deltas_sqlite(db_path, contract, start_block, end_block)
    return [{"address": a, "delta": d} for (a, d) in rows if d > 0][:n]


def top_spenders_sqlite(
    db_path: str, contract: str, n: int, start_block: int, end_block: int
) -> List[Dict[str, Any]]:
    """
    Addresses with largest negative (most outflow) delta in the window.
    """
    rows = _address_deltas_sqlite(db_path, contract, start_block, end_block)
    neg = [(a, d) for (a, d) in rows if d < 0]
    # rows already sorted by |delta| DESC, so just take first n of negatives
    return [{"address": a, "delta": d} for (a, d) in neg][:n]


def distribution_metrics_sqlite(
    db_path: str, contract: str, as_of_block: Optional[int] = None
) -> Dict[str, float]:
    """
    Simple distribution metrics for holder balances:
      - gini: Gini coefficient in [0,1]
      - hhi: Herfindahl-Hirschman Index in [0,1] (sum of squared shares)

    Excludes the zero address (mint/burn) and non-positive balances.
    """
    ZERO = "0x0000000000000000000000000000000000000000"

    bals = balances_as_of_sqlite(db_path, contract, as_of_block)

    # Keep only positive balances for real holder distribution; drop zero address.
    vals = [
        int(x["balance"])
        for x in bals
        if int(x["balance"]) > 0 and str(x["address"]).lower() != ZERO
    ]

    if not vals:
        return {"gini": 0.0, "hhi": 0.0}

    total = sum(vals)
    if total <= 0:
        return {"gini": 0.0, "hhi": 0.0}

    # HHI
    shares = [v / total for v in vals]
    hhi = sum(s * s for s in shares)

    # Gini (sorted ascending)
    xs = sorted(vals)
    n = len(xs)
    cum = 0
    weighted_sum = 0
    for i, v in enumerate(xs, start=1):
        cum += v
        weighted_sum += cum
    # Gini using cumulative areas
    gini = 1 - (2 * weighted_sum) / (n * total) + 1 / n

    # Clip to [0,1]
    gini = max(0.0, min(1.0, gini))
    hhi = max(0.0, min(1.0, hhi))
    return {"gini": gini, "hhi": hhi}

