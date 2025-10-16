def _coerce_int(value, default=0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    s = str(value).lower()
    try:
        return int(s, 16) if s.startswith("0x") else int(s)
    except Exception:
        return default


def transform_transactions(raw_txs: list[dict]) -> list[dict]:
    """
    Accepts raw RPC-style txs (with 'hash' and hex 'value') or pre-normalized
    (with 'tx_hash' and int 'value'). Returns normalized list.
    """
    out = []
    for tx in raw_txs or []:
        h = tx.get("hash") or tx.get("tx_hash")
        if not h:
            continue
        out.append({
            "tx_hash": h,
            "from": tx.get("from"),
            "to": tx.get("to"),
            "value": _coerce_int(tx.get("value"), 0),
        })
    return out


def transform_logs(raw_logs: list[dict]) -> list[dict]:
    """
    Accepts raw logs with 'transactionHash' (RPC) or 'tx_hash' (normalized).
    """
    out = []
    for lg in raw_logs or []:
        th = lg.get("transactionHash") or lg.get("tx_hash")
        if not th:
            continue
        out.append({
            "transactionHash": th,
            "address": lg.get("address"),
            "data": lg.get("data"),
            "topics": lg.get("topics") or [],
        })
    return out
