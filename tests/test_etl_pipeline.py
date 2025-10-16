import pytest
from etl.pipeline import run_etl

def test_run_etl(monkeypatch):
    monkeypatch.setattr("etl.extract.extract_block", lambda _: {"transactions": [{"hash": "0x1", "from": "0xA", "to": "0xB", "value": "0x10"}], "logs": []})
    monkeypatch.setattr("etl.load.load_transactions", lambda b, t: None)
    monkeypatch.setattr("etl.load.load_logs", lambda b, l: None)
    total = run_etl(0, backend="sqlite")
    assert total == 1
