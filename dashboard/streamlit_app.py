# dashboard/streamlit_app.py

from __future__ import annotations

import io
import os
import sqlite3
import zipfile
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

import numpy as np
import pandas as pd
import streamlit as st


# ============================================================
# pure helpers and light db utilities only below this line
# nothing here should touch the db by default at import time
# ============================================================

@dataclass
class DbCfg:
    path: str


def connect(cfg: DbCfg) -> sqlite3.Connection:
    con = sqlite3.connect(cfg.path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def q(con: sqlite3.Connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    return pd.read_sql_query(sql, con, params=params)


def list_contracts(con) -> pd.DataFrame:
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


def pick_latest_block(con, contract: str) -> int:
    sql = """
    SELECT MAX(block_number) AS last_block
    FROM balances
    WHERE LOWER(contract) = LOWER(?);
    """
    df = q(con, sql, (contract,))
    return int(df.iloc[0]["last_block"]) if not df.empty and df.iloc[0]["last_block"] is not None else 0


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


def total_supply(con, contract: str, as_of: int) -> float:
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
    return float(df.iloc[0]["total_units"]) if not df.empty and df.iloc[0]["total_units"] is not None else 0.0


def transfers_count(con, contract: str) -> int:
    sql = "SELECT COUNT(*) AS c FROM transfers WHERE LOWER(contract)=LOWER(?);"
    df = q(con, sql, (contract,))
    return int(df.iloc[0]["c"]) if not df.empty else 0


def gini_from_balances(con, contract: str, as_of: int) -> float:
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


def _latest_badge_html(as_of: int, db_max: int) -> str:
    if as_of == 0 or as_of >= db_max:
        return f"<span style='background:#E8F5E9;color:#1B5E20;padding:2px 8px;border-radius:8px;'>as of <b>{db_max}</b> latest</span>"
    return f"<span style='background:#FFF3E0;color:#E65100;padding:2px 8px;border-radius:8px;'>as of <b>{as_of}</b></span>"


def _block_bounds(con, contract: str) -> Tuple[int, int]:
    df = q(con, "SELECT MIN(block_number) AS lo, MAX(block_number) AS hi FROM balances WHERE LOWER(contract)=LOWER(?)", (contract,))
    lo = int(df.iloc[0]["lo"]) if not df.empty and df.iloc[0]["lo"] is not None else 0
    hi = int(df.iloc[0]["hi"]) if not df.empty and df.iloc[0]["hi"] is not None else 0
    return lo, hi


# ============================================================
# everything below runs only when render_app is called
# streamlit will call this in the container
# pytest in ci sets ONI_DASHBOARD_TEST_MODE=1 which skips execution
# ============================================================

def render_app() -> None:
    st.set_page_config(page_title="Onchain Network Insights", layout="wide")
    st.title("Onchain Network Insights Dashboard")
    st.caption("Local analytics over your ingested ERC 20 data")

    with st.sidebar:
        st.header("Settings")
        db_path = st.text_input("SQLite DB path", os.environ.get("SQLITE_PATH", "data/dev.db"))
        cfg = DbCfg(db_path)

        try:
            with closing(connect(cfg)) as con:
                contracts_df = list_contracts(con)
        except Exception as e:
            st.error(f"Could not open DB at {db_path}: {e}")
            st.stop()

        contract_input = st.text_input("ERC 20 contract", value="")
        pick_from_db = st.selectbox(
            "Pick a contract found in DB",
            options=([""] + contracts_df["contract"].tolist()),
            index=0,
            format_func=lambda x: x if x else "—"
        )
        contract = (contract_input or pick_from_db or "").strip()
        if not contract:
            st.info("Select or paste an ERC 20 contract to continue.")
            st.stop()

        # read bounds to guide user
        with closing(connect(cfg)) as con:
            lo_bn, hi_bn = _block_bounds(con, contract)

        default_as_of = 0
        as_of_block = st.number_input(
            f"As of block optional range {lo_bn if lo_bn else 0} to {hi_bn if hi_bn else 0}",
            min_value=0,
            value=default_as_of,
            step=1,
        )

        st.divider()
        st.subheader("Window for Deltas")
        start_block_excl = st.number_input("Start block exclusive", min_value=0, value=0, step=1)
        end_block_incl = st.number_input("End block inclusive", min_value=0, value=0, step=1)

        st.divider()
        topn = st.slider("Top N tables", min_value=5, max_value=100, value=10)
        whale_threshold = st.number_input("Whale threshold units", min_value=0.0, value=1000.0, step=1.0)

    # metrics
    with closing(connect(cfg)) as con:
        symbol, decimals = read_metadata(con, contract)
        # clamp as of into available range if user entered an out of range number
        latest = pick_latest_block(con, contract)
        effective_as_of = as_of_block
        if effective_as_of > 0:
            if latest > 0 and effective_as_of > latest:
                st.warning(f"As of block {effective_as_of} exceeds DB maximum {latest}. Using {latest}.")
                effective_as_of = latest
            if lo_bn > 0 and effective_as_of < lo_bn:
                st.warning(f"As of block {effective_as_of} is below DB minimum {lo_bn}. Using {lo_bn}.")
                effective_as_of = lo_bn

        effective_as_of = 0 if as_of_block == 0 else effective_as_of

        holders = holders_count(con, contract, latest if effective_as_of == 0 else effective_as_of)
        total = total_supply(con, contract, latest if effective_as_of == 0 else effective_as_of)
        gini = gini_from_balances(con, contract, latest if effective_as_of == 0 else effective_as_of)
        xfers = transfers_count(con, contract)
        cr_df = concentration_ratios(con, contract, latest if effective_as_of == 0 else effective_as_of, ks=(1, 2, 3, 5, 10))

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Symbol", symbol)
    c2.metric("Decimals", decimals)
    c3.metric("First block", f"{lo_bn}" if lo_bn else "N A")
    c4.metric("Last block", f"{hi_bn}" if hi_bn else "N A")
    c5.metric("Transfers", xfers)
    c6.metric("CR10", f"{cr_df.loc[cr_df['k'] == 10, 'ratio_pct'].values[0]:.2f}%" if (10 in cr_df["k"].values) else "0.00%")

    st.metric("Holders", holders)
    st.metric(f"Total supply as of [{symbol}]", f"{total:,.4f}")
    st.metric("Gini", f"{gini:.4f}")

    # latest badge
    if hi_bn:
        st.markdown(_latest_badge_html(0 if effective_as_of == 0 else int(effective_as_of), hi_bn), unsafe_allow_html=True)

    # top holders
    with closing(connect(cfg)) as con:
        top_df = top_holders(con, contract, latest if effective_as_of == 0 else effective_as_of, topn)
    st.subheader("Top Holders")
    if top_df.empty:
        st.info("No holders at the selected as of block.")
    else:
        st.bar_chart(top_df.set_index("address_short")["balance_units"])
        st.dataframe(top_df[["address", "balance_units"]], use_container_width=True)

    # whales
    with closing(connect(cfg)) as con:
        whales_df = whales(con, contract, latest if effective_as_of == 0 else effective_as_of, whale_threshold, topn)
    st.subheader(f"Whales ≥ {int(whale_threshold)}")
    if whales_df.empty:
        st.info("No whales at the selected threshold and as of block.")
    else:
        st.bar_chart(whales_df.set_index("address_short")["balance_units"])
        c1t, c2t = st.columns(2)
        c1t.dataframe(whales_df[["address", "balance_units"]], use_container_width=True)
        c2t.dataframe(whales_df[["address", "balance_units"]], use_container_width=True)

    # concentration ratios
    st.subheader("Concentration Ratios")
    st.line_chart(cr_df.set_index("k")["ratio"])
    st.dataframe(cr_df.assign(ratio_pct=cr_df["ratio_pct"].round(2)), use_container_width=True)

    # deltas
    with closing(connect(cfg)) as con:
        deltas_df = holder_deltas(con, contract, int(start_block_excl), int(end_block_incl))
    st.subheader("Holder Deltas")
    if start_block_excl <= 0 and end_block_incl <= 0:
        st.info("Set a valid window to see deltas.")
    elif deltas_df.empty:
        st.info("No holder deltas in the selected window.")
    else:
        st.dataframe(deltas_df, use_container_width=True)

    # snapshot zip
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


# ============================================================
# import safety for tests and ci
# set ONI_DASHBOARD_TEST_MODE=1 in ci so imports never execute the app
# streamlit runtime will execute render_app when serving locally or in the container
# ============================================================

if os.getenv("ONI_DASHBOARD_TEST_MODE") != "1":
    # when running streamlit the file is executed as a script
    # this call makes the ui appear while remaining safe for pytest imports in ci
    render_app()
