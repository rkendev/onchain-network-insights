import io
import os
import sqlite3
import zipfile
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st


# ----------------------------
# DB helpers
# ----------------------------

@dataclass
class DbCfg:
    path: str


def connect(cfg: DbCfg) -> sqlite3.Connection:
    # row_factory gives dict-like rows for direct DataFrame creation
    con = sqlite3.connect(cfg.path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def q(con: sqlite3.Connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    return pd.read_sql_query(sql, con, params=params)


# ----------------------------
# Data queries
# ----------------------------

def list_contracts(con) -> pd.DataFrame:
    # contracts present in balances/transfers
    sql = """
    WITH c AS (
      SELECT DISTINCT contract FROM balances
      UNION
      SELECT DISTINCT contract FROM transfers
    )
    SELECT c.contract
    FROM c
    ORDER BY c.contract;
    """
    try:
        return q(con, sql)
    except Exception:
        return pd.DataFrame(columns=["contract"])


def read_metadata(con, contract: str) -> tuple[str, int]:
    sql = """
    SELECT symbol, COALESCE(decimals, 0) AS decimals
    FROM erc20_metadata
    WHERE LOWER(contract) = LOWER(?)
    LIMIT 1;
    """
    df = q(con, sql, (contract,))
    if df.empty:
        return "N A", 0
    row = df.iloc[0]
    sym = row["symbol"] if isinstance(row["symbol"], str) and row["symbol"].strip() else "N A"
    return sym, int(row["decimals"])


def pick_as_of_block(con, contract: str, default_zero: bool = True) -> int:
    # latest block where we have any balance for the contract
    sql = """
    SELECT MAX(block_number) AS last_block
    FROM balances
    WHERE LOWER(contract) = LOWER(?);
    """
    df = q(con, sql, (contract,))
    last_block = int(df.iloc[0]["last_block"]) if not df.empty and df.iloc[0]["last_block"] is not None else 0
    return 0 if default_zero else last_block


def holders_count(con, contract: str, as_of: int) -> int:
    sql = """
    SELECT COUNT(*) AS holders
    FROM (
        SELECT address
        FROM balances
        WHERE LOWER(contract) = LOWER(?)
          AND block_number <= ?
        GROUP BY address
        HAVING MAX(balance_units) > 0
    ) t;
    """
    df = q(con, sql, (contract, as_of))
    return int(df.iloc[0]["holders"]) if not df.empty else 0


def total_supply(con, contract: str, as_of: int, decimals: int) -> float:
    # Sum balance_units (already scaled to base units in your ETL)
    sql = """
    SELECT SUM(balance_units) AS total_units
    FROM (
        SELECT address, MAX(balance_units) AS balance_units
        FROM balances
        WHERE LOWER(contract) = LOWER(?)
          AND block_number <= ?
        GROUP BY address
    );
    """
    df = q(con, sql, (contract, as_of))
    total_units = float(df.iloc[0]["total_units"]) if not df.empty and df.iloc[0]["total_units"] is not None else 0.0
    # If balances are already in human units, decimals==0 will leave unchanged
    scale = 10 ** 0  # your ETL already writes balance_units as human units
    return total_units / scale


def transfers_count(con, contract: str) -> int:
    sql = """
    SELECT COUNT(*) AS c FROM transfers
    WHERE LOWER(contract) = LOWER(?);
    """
    df = q(con, sql, (contract,))
    return int(df.iloc[0]["c"]) if not df.empty else 0


def gini_coefficient(con, contract: str, as_of: int) -> float:
    sql = """
    SELECT MAX(balance_units) AS bal
    FROM balances
    WHERE LOWER(contract) = LOWER(?)
      AND block_number <= ?
    GROUP BY address
    HAVING MAX(balance_units) > 0
    ORDER BY bal ASC;
    """
    df = q(con, sql, (contract, as_of))
    if df.empty:
        return 0.0
    x = df["bal"].to_numpy(dtype=float)
    if x.size == 0:
        return 0.0
    # standard Gini on non-negative vector
    x = np.sort(x)
    n = x.size
    cum = np.cumsum(x)
    g = (n + 1 - 2 * np.sum(cum) / cum[-1]) / n if cum[-1] > 0 else 0.0
    return float(max(0.0, min(1.0, g)))


def concentration_ratios(con, contract: str, as_of: int, ks=(1, 2, 3, 5, 10)) -> pd.DataFrame:
    sql = """
    SELECT MAX(balance_units) AS bal
    FROM balances
    WHERE LOWER(contract) = LOWER(?)
      AND block_number <= ?
    GROUP BY address
    HAVING MAX(balance_units) > 0
    ORDER BY bal DESC;
    """
    df = q(con, sql, (contract, as_of))
    if df.empty:
        return pd.DataFrame({"k": ks, "ratio": [0]*len(ks), "ratio_pct": [0]*len(ks)})
    balances = df["bal"].to_numpy(dtype=float)
    total = balances.sum()
    out = []
    for k in ks:
        k = min(k, balances.size)
        top_sum = balances[:k].sum() if k > 0 else 0.0
        ratio = (top_sum / total) if total > 0 else 0.0
        out.append((k, ratio, 100.0 * ratio))
    return pd.DataFrame(out, columns=["k", "ratio", "ratio_pct"])


def top_holders(con, contract: str, as_of: int, n: int) -> pd.DataFrame:
    sql = """
    SELECT address, MAX(balance_units) AS balance_units
    FROM balances
    WHERE LOWER(contract) = LOWER(?)
      AND block_number <= ?
    GROUP BY address
    HAVING MAX(balance_units) > 0
    ORDER BY balance_units DESC
    LIMIT ?;
    """
    df = q(con, sql, (contract, as_of, n))
    if df.empty:
        return df
    df["address_short"] = df["address"].str.slice(0, 8) + "…"
    return df


def whales(con, contract: str, as_of: int, threshold_units: float, n: int) -> pd.DataFrame:
    sql = """
    SELECT address, MAX(balance_units) AS balance_units
    FROM balances
    WHERE LOWER(contract) = LOWER(?)
      AND block_number <= ?
    GROUP BY address
    HAVING MAX(balance_units) >= ?
    ORDER BY balance_units DESC
    LIMIT ?;
    """
    df = q(con, sql, (contract, as_of, threshold_units, n))
    if df.empty:
        return df
    df["address_short"] = df["address"].str.slice(0, 8) + "…"
    return df


def holder_deltas(con, contract: str, start_excl: int, end_incl: int) -> pd.DataFrame:
    if start_excl >= end_incl:
        return pd.DataFrame(columns=["address", "transfer_in", "transfer_out", "mint_in", "burn_out"])

    sql = """
    WITH tx AS (
      SELECT
        block_number,
        LOWER(contract) AS contract,
        LOWER(src)  AS src,
        LOWER(dst)  AS dst,
        amount_units AS amt
      FROM transfers
      WHERE LOWER(contract) = LOWER(?)
        AND block_number > ?
        AND block_number <= ?
    )
    SELECT
      a.address,
      SUM(a.transfer_in)  AS transfer_in,
      SUM(a.transfer_out) AS transfer_out,
      SUM(a.mint_in)      AS mint_in,
      SUM(a.burn_out)     AS burn_out
    FROM (
      SELECT dst AS address, SUM(amt) AS transfer_in, 0 AS transfer_out, 0 AS mint_in, 0 AS burn_out FROM tx WHERE src != '0x0000000000000000000000000000000000000000' GROUP BY dst
      UNION ALL
      SELECT src AS address, 0, SUM(amt), 0, 0 FROM tx WHERE dst != '0x0000000000000000000000000000000000000000' GROUP BY src
      UNION ALL
      SELECT dst AS address, 0, 0, SUM(amt), 0 FROM tx WHERE src = '0x0000000000000000000000000000000000000000' GROUP BY dst
      UNION ALL
      SELECT src AS address, 0, 0, 0, SUM(amt) FROM tx WHERE dst = '0x0000000000000000000000000000000000000000' GROUP BY src
    ) a
    GROUP BY a.address
    ORDER BY (COALESCE(SUM(a.transfer_in),0) + COALESCE(SUM(a.mint_in),0) - COALESCE(SUM(a.transfer_out),0) - COALESCE(SUM(a.burn_out),0)) DESC;
    """
    return q(con, sql, (contract, start_excl, end_incl))


# ----------------------------
# UI
# ----------------------------

st.set_page_config(page_title="Onchain Network Insights", layout="wide")
st.title("Onchain Network Insights Dashboard")
st.caption("Local analytics over your ingested ERC-20 data")

with st.sidebar:
    st.header("Settings")

    db_path = st.text_input("SQLite DB path", os.environ.get("SQLITE_PATH", "data/dev.db"))
    cfg = DbCfg(db_path)

    try:
        with closing(connect(cfg)) as con:
            contracts = list_contracts(con)
    except Exception as e:
        st.error(f"Could not open DB at {db_path}: {e}")
        st.stop()

    contract_input = st.text_input("ERC 20 contract", value="")
    # optional list from DB
    pick_from_db = st.selectbox(
        "Pick a contract found in DB",
        options=([""] + contracts["contract"].tolist()),
        index=0,
        format_func=lambda x: x if x else "—"
    )
    contract = (contract_input or pick_from_db or "").strip()
    if not contract:
        st.info("Select or paste an ERC-20 contract to continue.")
        st.stop()

    as_of_block = st.number_input("As of block optional", min_value=0, value=pick_as_of_block(connect(cfg), contract, default_zero=True), step=1)

    st.divider()
    st.subheader("Window for Deltas")
    start_block_excl = st.number_input("Start block exclusive", min_value=0, value=0, step=1)
    end_block_incl   = st.number_input("End block inclusive",   min_value=0, value=0, step=1)

    st.divider()
    topn = st.slider("Top N tables", min_value=5, max_value=100, value=10)
    whale_threshold = st.number_input("Whale threshold units", min_value=0.0, value=1000.0, step=1.0)


# ----------------------------
# Metrics / sections
# ----------------------------

with closing(connect(cfg)) as con:
    symbol, decimals = read_metadata(con, contract)
    holders = holders_count(con, contract, as_of_block)
    total = total_supply(con, contract, as_of_block, decimals)
    gini = gini_coefficient(con, contract, as_of_block)
    xfers = transfers_count(con, contract)
    cr_df = concentration_ratios(con, contract, as_of_block, ks=(1,2,3,5,10))

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Symbol", symbol)
col2.metric("Decimals", decimals)
col3.metric("First block", "N A")  # optional: compute min block if you want
col4.metric("Last block", "N A")
col5.metric("Transfers", xfers)
col6.metric("CR10", f"{cr_df.loc[cr_df['k'] == 10, 'ratio_pct'].values[0]:.2f}%" if (10 in cr_df["k"].values) else "0.00%")

st.metric("Holders", holders)
st.metric(f"Total supply as of [{symbol}]", f"{total:,.4f}")
st.metric("Gini", f"{gini:.4f}")

# Top holders
with closing(connect(cfg)) as con:
    top_df = top_holders(con, contract, as_of_block, topn)
st.subheader("Top Holders")
if top_df.empty:
    st.info("No holders at the selected as-of block.")
else:
    st.bar_chart(top_df.set_index("address_short")["balance_units"])
    st.dataframe(top_df[["address","balance_units"]], use_container_width=True)

# Whales
with closing(connect(cfg)) as con:
    whales_df = whales(con, contract, as_of_block, whale_threshold, topn)
st.subheader(f"Whales ≥ {int(whale_threshold)}")
if whales_df.empty:
    st.info("No whales at the selected threshold and as-of block.")
else:
    st.bar_chart(whales_df.set_index("address_short")["balance_units"])
    c1, c2 = st.columns(2)
    c1.dataframe(whales_df[["address","balance_units"]], use_container_width=True)
    c2.dataframe(whales_df[["address","balance_units"]], use_container_width=True)

# Concentration ratios
st.subheader("Concentration Ratios")
st.line_chart(cr_df.set_index("k")["ratio"])
st.dataframe(cr_df.assign(ratio_pct=cr_df["ratio_pct"].round(2)), use_container_width=True)

# Deltas
with closing(connect(cfg)) as con:
    deltas_df = holder_deltas(con, contract, start_block_excl, end_block_incl)

st.subheader("Holder Deltas")
if start_block_excl <= 0 and end_block_incl <= 0:
    st.info("Set a valid window to see deltas.")
elif deltas_df.empty:
    st.info("No holder deltas in the selected window.")
else:
    st.dataframe(deltas_df, use_container_width=True)

# Snapshot zip
st.divider()
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    prefix = f"oni_snapshot_{symbol or 'token'}_{now}"
    for name, df in [
        ("top_holders.csv", top_df),
        ("whales.csv", whales_df),
        ("concentration_ratios.csv", cr_df),
        ("holder_deltas.csv", deltas_df),
    ]:
        if isinstance(df, pd.DataFrame) and not df.empty:
            z.writestr(f"{prefix}/{name}", df.to_csv(index=False))
st.download_button("Download snapshot zip", data=buf.getvalue(), file_name="snapshot.zip", mime="application/zip")
