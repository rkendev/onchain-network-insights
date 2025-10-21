# analytics/token_holders.py
from __future__ import annotations

import math
import sqlite3
from typing import Optional, Tuple, Dict, Any, List

from storage.sqlite_backend import SQLiteStorage
from ingestion.erc20_rpc import fetch_metadata as rpc_fetch_metadata, RpcError


def get_or_fetch_metadata(db_path: str, contract: str, as_of_block: Optional[int] = None) -> dict:
    sm = SQLiteStorage(db_path); sm.setup()
    meta = sm.read_erc20_metadata(contract)
    try:
        if not meta or (as_of_block and (not meta.get("as_of_block") or meta["as_of_block"] < as_of_block)):
            fetched = rpc_fetch_metadata(contract, as_of_block)
            sm.upsert_erc20_metadata(
                contract=fetched["contract"],
                symbol=fetched["symbol"],
                decimals=fetched["decimals"],
                total_supply=fetched["total_supply"],
                as_of_block=fetched["as_of_block"],
            )
            meta = sm.read_erc20_metadata(contract)
    except RpcError as e:
        # keep UI alive; return minimal metadata and let supply fall back to DB sum
        meta = meta or {"contract": contract, "symbol": "", "decimals": 0, "total_supply": 0, "as_of_block": as_of_block or 0}
        meta["_meta_warning"] = str(e)
    return meta or {"contract": contract, "symbol": "", "decimals": 0, "total_supply": 0, "as_of_block": as_of_block or 0}


def _sum_balances_sqlite(con: sqlite3.Connection, contract: str, as_of_block: Optional[int]) -> int:
    if as_of_block and as_of_block > 0:
        row = con.execute(
            """
            SELECT COALESCE(SUM(value), 0) FROM (
              SELECT recipient AS addr, SUM(value) AS value
              FROM transfers
              WHERE contract=? AND block_number <= ?
              GROUP BY recipient
              UNION ALL
              SELECT sender AS addr, -SUM(value) AS value
              FROM transfers
              WHERE contract=? AND block_number <= ?
              GROUP BY sender
            )
            """, (contract.lower(), as_of_block, contract.lower(), as_of_block)
        ).fetchone()
    else:
        row = con.execute(
            """
            SELECT COALESCE(SUM(value), 0) FROM (
              SELECT recipient AS addr, SUM(value) AS value
              FROM transfers
              WHERE contract=?
              GROUP BY recipient
              UNION ALL
              SELECT sender AS addr, -SUM(value) AS value
              FROM transfers
              WHERE contract=?
              GROUP BY sender
            )
            """, (contract.lower(), contract.lower())
        ).fetchone()
    return int(row[0] or 0)


def holder_balances(con, contract: str, as_of: int | None, top_n: int = 10):
    """
    Return (rows_df, meta_dict) for the top-N holders at an optional as-of block.
    Works directly over `transfers_enriched` (no `direction` column required).
    """
    params = [contract.lower()]
    asof_clause = ""
    if as_of is not None and as_of > 0:
        asof_clause = "AND blockNumber <= ?"
        params.append(as_of)

    # Aggregate balances from deltas
    sql = f"""
    WITH addr_bal AS (
      SELECT
        contract,
        address,
        SUM(delta) AS balance
      FROM transfers_enriched
      WHERE lower(contract) = ?
        {asof_clause}
      GROUP BY contract, address
    )
    SELECT address, balance
    FROM addr_bal
    WHERE balance > 0
    ORDER BY balance DESC
    LIMIT {int(top_n)}
    """
    rows = con.execute(sql, tuple(params)).fetchall()

    # Metadata: holders count & total supply proxy (mints - burns)
    holders_sql = f"""
    WITH addr_bal AS (
      SELECT contract, address, SUM(delta) AS balance
      FROM transfers_enriched
      WHERE lower(contract) = ?
        {asof_clause}
      GROUP BY contract, address
    )
    SELECT
      SUM(CASE WHEN balance > 0 THEN 1 ELSE 0 END)             AS holders,
      COALESCE((
        SELECT mb.total_minted - mb.total_burned
        FROM mint_burn mb
        WHERE lower(mb.contract) = ?
      ), 0)                                                    AS total_supply
    FROM addr_bal
    """
    holders_params = [contract.lower()]
    if as_of is not None and as_of > 0:
        holders_params.append(as_of)
    holders_params.append(contract.lower())
    meta_row = con.execute(holders_sql, tuple(holders_params)).fetchone()
    meta = {"holders": meta_row[0] or 0, "total_supply": meta_row[1] or 0}

    # Convert to DataFrame the way your app expects
    import pandas as pd
    df = pd.DataFrame(rows, columns=["address", "balance"])
    return df, meta


def holder_balances(con, contract: str, as_of: int | None, top_n: int = 10):
    """
    Return (top_df, meta) where top_df has columns [address, balance] and meta has
    at least {"symbol": str, "decimals": int}.
    """
    # read metadata (if present)
    meta = {"symbol": None, "decimals": 18}
    try:
        row = con.execute(
            "SELECT symbol, decimals FROM metadata WHERE contract = ? LIMIT 1",
            (contract,),
        ).fetchone()
        if row:
            meta["symbol"] = row[0]
            meta["decimals"] = int(row[1]) if row[1] is not None else 18
    except Exception:
        pass

    # Base query: balances as-of (current or <= block_number)
    params = [contract]
    where_cutoff = ""
    if as_of and as_of > 0:
        where_cutoff = "AND block_number <= ?"
        params.append(as_of)

    # NOTE: adjust this SELECT to your actual balance materialization.
    # If you store latest balances in a table, use that table.
    sql = f"""
    WITH agg AS (
      SELECT
        address,
        SUM(CASE WHEN direction = 'in' THEN value ELSE -value END) AS balance
      FROM balances_view  -- or your materialized balances source
      WHERE contract = ? {where_cutoff}
      GROUP BY address
    )
    SELECT address, balance
    FROM agg
    WHERE balance > 0
    ORDER BY balance DESC
    LIMIT ?
    """
    params.append(top_n)

    rows = con.execute(sql, tuple(params)).fetchall()
    df = pd.DataFrame(rows, columns=["address", "balance"])
    return df, meta


def gini_coefficient(balances_raw: List[int]) -> float:
    if not balances_raw:
        return 0.0
    x = sorted([max(0, int(v)) for v in balances_raw])
    n = len(x)
    s = sum(x)
    if s == 0:
        return 0.0
    cum = 0
    for i, xi in enumerate(x, start=1):
        cum += i * xi
    # Gini = (2*sum(i*xi)/(n*sum(x)) - (n+1)/n)
    return (2 * cum / (n * s)) - (n + 1) / n


def concentration_ratio_k(balances_raw: List[int], k: int, total_supply_raw: int) -> float:
    if total_supply_raw <= 0:
        return 0.0
    top = sum(sorted(balances_raw, reverse=True)[:k])
    return top / float(total_supply_raw)
