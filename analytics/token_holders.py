from typing import Optional, List, Dict, Any
from storage.sqlite_backend import SQLiteStorage

def balances_as_of_sqlite(db_path: str, contract: str, as_of_block: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Compute holder balances for a given ERC-20 contract as of an optional block number.
    Returns a list of {address, balance} (balance is integer).
    """
    sm = SQLiteStorage(db_path)
    sm.setup()
    cur = sm.conn.cursor()

    # Filter on contract, optional block
    params = [contract]
    block_clause = ""
    if as_of_block is not None:
        block_clause = "AND block_number <= ?"
        params.append(as_of_block)

    # Sum inflows - outflows per address
    # We use sender/recipient columns (set in erc20-store branch)
    sql = f"""
    WITH moves AS (
        SELECT sender AS addr, -value AS delta
        FROM transfers
        WHERE contract = ? {block_clause}
        UNION ALL
        SELECT recipient AS addr, value AS delta
        FROM transfers
        WHERE contract = ? {block_clause}
    ),
    agg AS (
        SELECT addr, COALESCE(SUM(delta), 0) AS balance
        FROM moves
        GROUP BY addr
        HAVING balance != 0
    )
    SELECT addr AS address, balance
    FROM agg
    ORDER BY balance DESC, address ASC
    """
    # params need to match placeholders: (contract,[as_of] ; contract,[as_of])
    final_params = params + params  # duplicate for recipient side
    cur.execute(sql, final_params)
    rows = cur.fetchall()
    return [{"address": r[0], "balance": int(r[1])} for r in rows]

def top_holders_sqlite(db_path: str, contract: str, n: int = 10, as_of_block: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Convenience wrapper: return top N holders by balance.
    """
    bals = balances_as_of_sqlite(db_path, contract, as_of_block)
    return bals[:n]
