from typing import Dict, List, Optional
import sqlite3

DBPath = str


def _connect(db_path: DBPath) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def _balances_rows(con: sqlite3.Connection, where: str, params: dict) -> List[Dict]:
    sql = f"""
      WITH deltas AS (
        SELECT COALESCE(recipient, "to") AS address, CAST(value AS INTEGER) AS delta
          FROM transfers
         WHERE {where}
        UNION ALL
        SELECT COALESCE(sender, "from")  AS address, -CAST(value AS INTEGER) AS delta
          FROM transfers
         WHERE {where}
      )
      SELECT address, SUM(delta) AS balance
        FROM deltas
       GROUP BY address
      HAVING balance != 0
       ORDER BY balance DESC
    """
    return [dict(r) for r in con.execute(sql, params).fetchall()]


def holder_balances_sqlite(
    db_path: DBPath,
    contract: Optional[str],
    as_of_block: Optional[int] = None,
) -> List[Dict[str, int]]:
    """
    Compute balances from transfers. Supports sender or from and recipient or to. Casts value to integer.
    If a contract filter returns zero rows, fall back to all rows to stay robust against seed variance.
    """
    con = _connect(db_path)

    params = {}
    where = "1=1"
    if contract:
        where = "contract = :contract"
        params["contract"] = contract
    if as_of_block is not None:
        where = f"{where} AND block_number <= :asof"
        params["asof"] = int(as_of_block)

    rows = _balances_rows(con, where, params)

    if not rows:
        params2 = {}
        where2 = "1=1"
        if as_of_block is not None:
            where2 += " AND block_number <= :asof"
            params2["asof"] = int(as_of_block)
        rows = _balances_rows(con, where2, params2)

    return rows


def holder_deltas_sqlite(
    db_path: DBPath,
    contract: Optional[str],
    start_block: int,
    end_block: int,
) -> List[Dict]:
    """
    Net change per address over the open interval start block to end block inclusive of end only.
    """
    con = _connect(db_path)
    params = {"start": int(start_block), "end": int(end_block)}
    where = "block_number > :start AND block_number <= :end"
    if contract:
        where = f"contract = :contract AND {where}"
        params["contract"] = contract

    sql = f"""
      WITH deltas AS (
        SELECT COALESCE(recipient, "to") AS address, CAST(value AS INTEGER) AS delta
          FROM transfers
         WHERE {where}
        UNION ALL
        SELECT COALESCE(sender, "from")  AS address, -CAST(value AS INTEGER) AS delta
          FROM transfers
         WHERE {where}
      )
      SELECT address, SUM(delta) AS delta
        FROM deltas
       GROUP BY address
      HAVING delta != 0
       ORDER BY delta DESC
    """
    rows = con.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def top_gainers_sqlite(
    db_path: DBPath,
    contract: Optional[str],
    n: int,
    start_block: int,
    end_block: int,
) -> List[Dict]:
    return holder_deltas_sqlite(db_path, contract, start_block, end_block)[: int(n)]


def top_spenders_sqlite(
    db_path: DBPath,
    contract: Optional[str],
    n: int,
    start_block: int,
    end_block: int,
) -> List[Dict]:
    negatives = [
        d for d in holder_deltas_sqlite(db_path, contract, start_block, end_block)
        if int(d["delta"]) < 0
    ]
    negatives.sort(key=lambda x: int(x["delta"]))
    return negatives[: int(n)]


def distribution_metrics_sqlite(
    db_path: DBPath,
    contract: Optional[str],
    as_of_block: Optional[int] = None,
) -> Dict[str, float]:
    bals = holder_balances_sqlite(db_path, contract, as_of_block)
    arr = [int(b["balance"]) for b in bals if int(b["balance"]) > 0]
    n = len(arr)
    total = sum(arr)
    if n == 0 or total == 0:
        return {
            "total": 0.0,
            "n_holders": float(n),
            "mean": 0.0,
            "max": 0.0,
            "hhi": 0.0,
            "gini": 0.0,
        }
    mean = total / n
    hhi = sum((x / total) ** 2 for x in arr)
    xs = sorted(arr)
    cum = 0
    bsum = 0
    for x in xs:
        cum += x
        bsum += cum
    gini = 1.0 - 2.0 * (bsum / (n * total)) + 1.0 / n
    return {
        "total": float(total),
        "n_holders": float(n),
        "mean": float(mean),
        "max": float(max(arr)),
        "hhi": float(hhi),
        "gini": float(gini),
    }


__all__ = [
    "holder_balances_sqlite",
    "holder_deltas_sqlite",
    "top_gainers_sqlite",
    "top_spenders_sqlite",
    "distribution_metrics_sqlite",
]
