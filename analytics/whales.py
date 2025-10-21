# analytics/whales.py
from __future__ import annotations

import sqlite3
from typing import Iterable, Tuple, Dict, Any

from analytics.token_holders import holder_balances


def whales_table(
    db_path: str,
    contract: str,
    as_of: int | None,
    threshold: int | float,
    top_n: int = 1000,
) -> Tuple[Iterable[Tuple[str, int]], Dict[str, Any]]:
    """
    Return (rows, meta) for holders with balance >= threshold as of `as_of`.

    rows: iterable of (address, balance) sorted desc.
    meta: dict returned by holder_balances (augmented with 'threshold').
    """
    with sqlite3.connect(db_path) as con:
        rows_df, meta = holder_balances(
            con=con,
            contract=contract,
            as_of=as_of,
            top_n=top_n,
        )

    # rows_df has columns ['address','balance'] already sorted desc
    filtered = rows_df[rows_df["balance"] >= threshold]
    out_rows = list(filtered[["address", "balance"]].itertuples(index=False, name=None))
    meta = dict(meta or {})
    meta["threshold"] = threshold
    return out_rows, meta
