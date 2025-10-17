# etl/pipeline.py
import etl.extract as extract
import etl.transform as transform
import etl.load as load

def _pick(raw: dict, key: str):
    if not isinstance(raw, dict):
        return None
    if key in raw:
        return raw.get(key)
    res = raw.get("result")
    if isinstance(res, dict):
        return res.get(key)
    return None

def run_etl(block_number: int, backend: str = "sqlite", **backend_opts) -> int:
    raw = extract.extract_block(block_number)

    raw_txs = _pick(raw, "transactions") or []
    raw_logs = _pick(raw, "logs") or []
    if not isinstance(raw_txs, list):
        raw_txs = []
    if not isinstance(raw_logs, list):
        raw_logs = []

    txs = transform.transform_transactions(raw_txs)
    logs = transform.transform_logs(raw_logs)

    # Pass through sqlite_path / pg_dsn etc.
    load.load_transactions(backend, txs, **backend_opts)
    load.load_logs(backend, logs, **backend_opts)

    return len(txs) + len(logs)
