import pytest
from etl.pipeline import run_etl

def test_pipeline_counts_erc20_transfers(monkeypatch, tmp_path):
    # One tx, one ERC-20 Transfer log
    fake_raw = {
        "transactions": [{"hash": "0x1", "from": "0xA", "to": "0xB", "value": "0x10"}],
        "logs": [{
            "address": "0xToken",
            "transactionHash": "0x1",
            "blockNumber": "0x10",
            "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x" + "00"*12 + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0x" + "00"*12 + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ],
            "data": "0x" + "0"*60 + "64"  # 0x64 -> 100
        }]
    }

    # Patch extractor & loads to avoid IO
    monkeypatch.setattr("etl.extract.extract_block", lambda _: fake_raw)
    monkeypatch.setattr("etl.load.load_transactions", lambda backend, txs, **opts: None)
    monkeypatch.setattr("etl.load.load_logs", lambda backend, logs, **opts: None)

    # Use a temp db path even though loads are no-op (just consistent)
    db_path = tmp_path / "pipe.db"
    total = run_etl(0, backend="sqlite", sqlite_path=str(db_path))
    # 1 tx + 1 transfer + 1 log == 3 counted items
    assert total == 3
