# dashboard/streamlit_app.py
from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import streamlit as st

# ----------------------------
# UI helpers
# ----------------------------

st.set_page_config(page_title="Onchain Network Insights — Dashboard", layout="wide")

@dataclass
class AppState:
    db_path: str
    contract: Optional[str]
    as_of_block: Optional[int]
    top_n: int
    whale_threshold: int
    delta_start: Optional[int]
    delta_end: Optional[int]

ZERO = "0x0000000000000000000000000000000000000000"

# ----------------------------
# SQL bootstrap (idempotent)
# ----------------------------

CREATE_TRANSFERS_ENRICHED = f"""
CREATE VIEW IF NOT EXISTS transfers_enriched AS
SELECT
  t.tx_hash,
  t.contract,
  t."from" AS from_addr,
  t."to"   AS to_addr,
  t.value,
  t.blockNumber,
  CASE
    WHEN t."from" = '{ZERO}' AND t."to" != '{ZERO}' THEN 'mint'
    WHEN t."to"   = '{ZERO}' AND t."from" != '{ZERO}' THEN 'burn'
    ELSE 'transfer'
  END AS direction,
  -- delta for 'to' address (positive when tokens are received)
  CASE
    WHEN t."from" = '{ZERO}' THEN t.value                      -- mint increases 'to'
    WHEN t."to"   = '{ZERO}' THEN -t.value                     -- burn decreases 'from'
    ELSE 0                                                     -- we handle from/to separately in balances_view
  END AS base_delta
FROM transfers t;
"""

# balances_view computes final balances using:
#  + base_delta for mints/burns (applied to the appropriate holder)
#  + +value for 'to' in transfers
#  + -value for 'from' in transfers
CREATE_BALANCES_VIEW = f"""
CREATE VIEW IF NOT EXISTS balances_view AS
WITH
tx AS (
  SELECT * FROM transfers_enriched
),
-- deltas from regular transfers
transfer_to AS (
  SELECT contract, to_addr  AS address, value    AS delta, blockNumber
  FROM tx WHERE direction = 'transfer'
),
transfer_from AS (
  SELECT contract, from_addr AS address, -value  AS delta, blockNumber
  FROM tx WHERE direction = 'transfer'
),
-- mints/burns already encoded in base_delta on the correct side:
mint_burn_to AS (
  SELECT contract, to_addr   AS address, base_delta AS delta, blockNumber
  FROM tx WHERE direction = 'mint'
),
mint_burn_from AS (
  SELECT contract, from_addr AS address, base_delta AS delta, blockNumber
  FROM tx WHERE direction = 'burn'
),
all_deltas AS (
  SELECT * FROM transfer_to
  UNION ALL
  SELECT * FROM transfer_from
  UNION ALL
  SELECT * FROM mint_burn_to
  UNION ALL
  SELECT * FROM mint_burn_from
)
SELECT
  contract,
  address,
  SUM(delta) AS balance,
  MAX(blockNumber) AS last_block
FROM all_deltas
GROUP BY contract, address;
"""

def ensure_views(con: sqlite3.Connection) -> None:
    """Create the minimal analytics views if they are missing."""
    # Fail fast if the core table is missing
    cur = con.execute("""
        SELECT name FROM sqlite_master
        WHERE type IN ('table','view') AND name='transfers';
    """)
    row = cur.fetchone()
    if not row:
        raise RuntimeError(
            "Database is missing the 'transfers' table. "
            "Seed the DB first or run your ETL."
        )
    con.executescript(CREATE_TRANSFERS_ENRICHED)
    con.executescript(CREATE_BALANCES_VIEW)

# ----------------------------
# Metadata helpers
# ----------------------------

def read_metadata(con: sqlite3.Connection, contract: str) -> dict:
    """Read optional ERC-20 metadata (decimals, symbol) if present."""
    meta = {"symbol": "", "decimals": 0}
    try:
        row = con.execute(
            "SELECT symbol, decimals FROM erc20_metadata WHERE contract = ? LIMIT 1",
            (contract,),
        ).fetchone()
        if row:
            meta["symbol"] = row[0] or ""
            meta["decimals"] = int(row[1] or 0)
    except sqlite3.OperationalError:
        # metadata table may not exist in a demo DB
        pass
    return meta

# ----------------------------
# Analytics
# ----------------------------

def holder_balances(
    con: sqlite3.Connection,
    contract: str,
    as_of: Optional[int],
    top_n: int,
) -> Tuple[pd.DataFrame, dict]:
    """
    Return (top_holders_dataframe, metadata_dict).
    Uses balances_view; optionally filters by as-of block.
    """
    ensure_views(con)
    where = ["contract = ?"]
    params = [contract]
    if as_of is not None and as_of > 0:
        where.append("last_block <= ?")
        params.append(as_of)
    sql = f"""
        SELECT address, balance
        FROM balances_view
        WHERE {' AND '.join(where)}
        ORDER BY balance DESC
        LIMIT ?
    """
    params.append(top_n)
    rows = con.execute(sql, tuple(params)).fetchall()
    df = pd.DataFrame(rows, columns=["address", "balance"])
    meta = read_metadata(con, contract)
    return df, meta

def whales(
    con: sqlite3.Connection,
    contract: str,
    threshold: int,
    as_of: Optional[int],
    top_n: int,
) -> pd.DataFrame:
    ensure_views(con)
    where = ["contract = ?", "balance >= ?"]
    params = [contract, threshold]
    if as_of is not None and as_of > 0:
        where.append("last_block <= ?")
        params.append(as_of)
    sql = f"""
        SELECT address, balance
        FROM balances_view
        WHERE {' AND '.join(where)}
        ORDER BY balance DESC
        LIMIT ?
    """
    params.append(top_n)
    rows = con.execute(sql, tuple(params)).fetchall()
    return pd.DataFrame(rows, columns=["address", "balance"])

def concentration_ratios(
    con: sqlite3.Connection,
    contract: str,
    as_of: Optional[int],
    ks=(1, 2, 3, 5, 10, 50, 100),
) -> pd.DataFrame:
    """CR_k over top-k holders: sum(top k)/sum(all)."""
    ensure_views(con)
    # total supply proxy = sum balances (within as-of)
    total_where = ["contract = ?"]
    total_params = [contract]
    if as_of is not None and as_of > 0:
        total_where.append("last_block <= ?")
        total_params.append(as_of)
    total_sql = f"""
        SELECT COALESCE(SUM(balance),0)
        FROM balances_view
        WHERE {' AND '.join(total_where)}
    """
    total = con.execute(total_sql, tuple(total_params)).fetchone()[0] or 0
    data = []
    for k in ks:
        top_sql = f"""
            SELECT COALESCE(SUM(balance),0)
            FROM (
              SELECT balance
              FROM balances_view
              WHERE {' AND '.join(total_where)}
              ORDER BY balance DESC
              LIMIT {k}
            )
        """
        top_val = con.execute(top_sql, tuple(total_params)).fetchone()[0] or 0
        ratio = (top_val / total) if total else 0.0
        data.append({"k": k, "ratio": ratio, "ratio_pct": round(100 * ratio, 2)})
    return pd.DataFrame(data)

# ----------------------------
# UI
# ----------------------------

def sidebar() -> AppState:
    st.sidebar.header("Settings")

    db_path = st.sidebar.text_input("SQLite DB path", "data/dev.db")
    contract_in = st.sidebar.text_input("ERC-20 contract", "")

    # Also offer a dropdown of contracts found in DB (if any)
    dropdown_val = ""
    try:
        with closing(sqlite3.connect(db_path)) as con:
            con.row_factory = None
            cur = con.execute("""
                SELECT DISTINCT contract
                FROM transfers
                ORDER BY contract
                LIMIT 200
            """)
            choices = [r[0] for r in cur.fetchall()]
        if choices:
            dropdown_val = st.sidebar.selectbox("Pick a contract found in DB", choices, index=0)
    except Exception:
        pass

    # priority: free-text > dropdown > None
    contract = (contract_in or dropdown_val or "").strip() or None

    as_of = st.sidebar.number_input("As-of block (optional)", min_value=0, value=0, step=1)
    as_of_block = int(as_of) if as_of > 0 else None

    st.sidebar.markdown("---")
    st.sidebar.subheader("Window for Deltas")
    delta_start = int(st.sidebar.number_input("Start block (exclusive)", min_value=0, value=0, step=1))
    delta_end   = int(st.sidebar.number_input("End block (inclusive)",   min_value=0, value=0, step=1))

    st.sidebar.markdown("---")
    top_n = int(st.sidebar.slider("Top N (tables)", min_value=5, max_value=100, value=10, step=1))
    whale_threshold = int(st.sidebar.number_input("Whale threshold (units)", min_value=0, value=1000, step=1))

    return AppState(
        db_path=db_path,
        contract=contract,
        as_of_block=as_of_block,
        top_n=top_n,
        whale_threshold=whale_threshold,
        delta_start=delta_start,
        delta_end=delta_end,
    )

def main():
    state = sidebar()

    st.title("Onchain Network Insights — Dashboard")
    st.caption("Local analytics over your ingested ERC-20 data")

    if not state.contract:
        st.info("Enter a full ERC-20 contract (0x + 40 hex chars) or pick one from the DB.")
        return

    dbp = Path(state.db_path)
    if not dbp.exists():
        st.error(f"DB not found: {dbp}")
        return

    try:
        with closing(sqlite3.connect(state.db_path)) as con:
            con.row_factory = None

            # Compute metrics
            top_df, meta = holder_balances(
                con,
                contract=state.contract,
                as_of=state.as_of_block,
                top_n=state.top_n,
            )

            cr_df = concentration_ratios(
                con,
                contract=state.contract,
                as_of=state.as_of_block,
            )

            whales_df = whales(
                con,
                contract=state.contract,
                threshold=state.whale_threshold,
                as_of=state.as_of_block,
                top_n=state.top_n,
            )

            symbol = f"[{meta.get('symbol')}] " if meta.get("symbol") else ""

            # KPIs
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Holders", value=f"{len(top_df)}")
            with col2:
                total_supply = con.execute("""
                    SELECT COALESCE(SUM(balance),0)
                    FROM balances_view
                    WHERE contract = ?
                      AND (? IS NULL OR last_block <= ?)
                """, (state.contract, state.as_of_block, state.as_of_block)).fetchone()[0] or 0
                st.metric(f"Total supply (as-of) {symbol}".strip(), value=f"{total_supply:,}")
            with col3:
                # quick/rough gini based on current sample of top balances
                vals = top_df["balance"].astype(float).clip(lower=0).sort_values().values
                if len(vals) == 0:
                    g = 0.0
                else:
                    # simple Gini on sample
                    n = len(vals)
                    cum = (2 * (vals * (pd.Series(range(1, n+1))).values).sum()) / (n * vals.sum()) - (n + 1) / n
                    g = max(0.0, round(cum, 4))
                st.metric("Gini", value=f"{g:.4f}")
            with col4:
                cr10 = cr_df.loc[cr_df["k"] == 10, "ratio"].values
                cr10_val = f"{(cr10[0]*100):.2f}%" if len(cr10) else "0.00%"
                st.metric("CR10", value=cr10_val)

            # Top holders + Whales charts
            st.subheader("Top Holders")
            st.bar_chart(top_df.set_index("address")["balance"])

            st.subheader(f"Whales (≥ {state.whale_threshold} {meta.get('symbol','').upper()})")
            st.bar_chart(whales_df.set_index("address")["balance"])

            # Tables
            colA, colB = st.columns(2)
            with colA:
                st.dataframe(top_df, use_container_width=True)
            with colB:
                st.dataframe(whales_df, use_container_width=True)

            st.subheader("Concentration Ratios")
            st.line_chart(cr_df.set_index("k")["ratio"])
            st.dataframe(cr_df, use_container_width=True)

            # Windowed deltas (optional)
            if state.delta_start < state.delta_end:
                st.subheader("Holder Deltas (Growth / Spend)")
                # Compute basic deltas on the fly from transfers
                sql = f"""
                    WITH span AS (
                      SELECT *
                      FROM transfers_enriched
                      WHERE contract = ?
                        AND blockNumber >  ?  -- exclusive
                        AND blockNumber <= ?  -- inclusive
                    ),
                    transfer_to AS (
                      SELECT to_addr   AS address, SUM(value)    AS delta_in
                      FROM span WHERE direction='transfer'
                      GROUP BY 1
                    ),
                    transfer_from AS (
                      SELECT from_addr AS address, SUM(value)    AS delta_out
                      FROM span WHERE direction='transfer'
                      GROUP BY 1
                    ),
                    mint AS (
                      SELECT to_addr   AS address, SUM(base_delta) AS mint_in
                      FROM span WHERE direction='mint'
                      GROUP BY 1
                    ),
                    burn AS (
                      SELECT from_addr AS address, SUM(base_delta) AS burn_out
                      FROM span WHERE direction='burn'
                      GROUP BY 1
                    )
                    SELECT
                      COALESCE(ti.address, to2.address, mi.address, bo.address) AS address,
                      COALESCE(ti.delta_in, 0)  AS transfer_in,
                      COALESCE(to2.delta_out, 0) AS transfer_out,
                      COALESCE(mi.mint_in, 0)   AS mint_in,
                      COALESCE(bo.burn_out, 0)  AS burn_out
                    FROM transfer_to ti
                    FULL OUTER JOIN transfer_from to2 ON to2.address = ti.address
                    FULL OUTER JOIN mint mi           ON mi.address  = COALESCE(ti.address, to2.address)
                    FULL OUTER JOIN burn bo           ON bo.address  = COALESCE(ti.address, to2.address, mi.address)
                """
                # SQLite lacks FULL OUTER JOIN; emulate via UNION of LEFT JOINs
                sql = f"""
                    WITH span AS (
                      SELECT *
                      FROM transfers_enriched
                      WHERE contract = ?
                        AND blockNumber >  ?
                        AND blockNumber <= ?
                    ),
                    transfer_in AS (
                      SELECT to_addr AS address, SUM(value) AS transfer_in
                      FROM span WHERE direction='transfer'
                      GROUP BY 1
                    ),
                    transfer_out AS (
                      SELECT from_addr AS address, SUM(value) AS transfer_out
                      FROM span WHERE direction='transfer'
                      GROUP BY 1
                    ),
                    mint AS (
                      SELECT to_addr AS address, SUM(base_delta) AS mint_in
                      FROM span WHERE direction='mint'
                      GROUP BY 1
                    ),
                    burn AS (
                      SELECT from_addr AS address, SUM(base_delta) AS burn_out
                      FROM span WHERE direction='burn'
                      GROUP BY 1
                    ),
                    combined AS (
                      SELECT address FROM transfer_in
                      UNION
                      SELECT address FROM transfer_out
                      UNION
                      SELECT address FROM mint
                      UNION
                      SELECT address FROM burn
                    )
                    SELECT
                      c.address,
                      COALESCE(ti.transfer_in, 0)  AS transfer_in,
                      COALESCE(to2.transfer_out, 0) AS transfer_out,
                      COALESCE(mi.mint_in, 0)     AS mint_in,
                      COALESCE(bo.burn_out, 0)    AS burn_out
                    FROM combined c
                    LEFT JOIN transfer_in  ti  ON ti.address  = c.address
                    LEFT JOIN transfer_out to2 ON to2.address = c.address
                    LEFT JOIN mint         mi  ON mi.address  = c.address
                    LEFT JOIN burn         bo  ON bo.address  = c.address
                    ORDER BY (COALESCE(ti.transfer_in,0)+COALESCE(mi.mint_in,0)) DESC
                    LIMIT ?
                """
                rows = con.execute(
                    sql,
                    (state.contract, state.delta_start, state.delta_end, state.top_n),
                ).fetchall()
                deltas_df = pd.DataFrame(
                    rows,
                    columns=["address", "transfer_in", "transfer_out", "mint_in", "burn_out"],
                )
                st.dataframe(deltas_df, use_container_width=True)
            else:
                st.info("Set a valid window (start < end) to see deltas/gainers/spenders.")

    except RuntimeError as e:
        st.error(str(e))
    except sqlite3.OperationalError as e:
        st.error(f"SQLite error while reading analytics: {e}")

if __name__ == "__main__":
    main()
