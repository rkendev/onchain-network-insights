import os
from etl.pipeline import run_etl
from storage.sqlite_backend import SQLiteStorage

def test_pipeline_persists_transfers(tmp_path, monkeypatch):
    fake_raw = {
        "transactions": [],
        "logs": [{
            "address": "0xToken",
            "transactionHash": "0xdead",
            "blockNumber": "0x10",
            "topics": [
                # topic0 = keccak256("Transfer(address,address,uint256)")
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x" + "00"*12 + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0x" + "00"*12 + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ],
            "data": "0x" + "0"*62 + "64"  # 0x64 -> 100
        }]
    }
    monkeypatch.setattr("etl.extract.extract_block", lambda _: fake_raw)
    db_path = tmp_path / "x.db"
    total = run_etl(123, backend="sqlite", sqlite_path=str(db_path))
    assert total == 1  # 1 transfer only

    sm = SQLiteStorage(str(db_path))
    sm.setup()
    rows = sm.conn.execute("SELECT tx_hash, contract, value FROM transfers").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "0xdead"
    assert rows[0][1] == "0xToken"
    assert rows[0][2] == 100
