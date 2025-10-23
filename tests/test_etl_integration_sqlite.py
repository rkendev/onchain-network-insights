import os
from storage.sqlite_backend import SQLiteStorage
from etl.pipeline import run_etl

def test_etl_persists_to_sqlite(tmp_path, monkeypatch):
    # 1) Fake a block with one tx + one log
    fake_raw = {
        "transactions": [{"hash": "0xabc", "from": "0xA", "to": "0xB", "value": "0x10"}],
        "logs": [{"transactionHash": "0xabc", "address": "0xC", "data": "0x00", "topics": []}],
    }
    monkeypatch.setattr("etl.extract.extract_block", lambda _: fake_raw)

    print("--------------------------tmp path------"+str(tmp_path))

    # 2) Use a temp sqlite path for this test
    db_path = tmp_path / "etl_integration.db"

    # 3) Run ETL
    total = run_etl(0, backend="sqlite", sqlite_path=str(db_path))
    assert total == 2  # 1 tx + 1 log

    # 4) Verify rows exist
    sm = SQLiteStorage(str(db_path))
    sm.setup()
    # read back tx via SQL to confirm
    rows = sm.conn.execute("SELECT tx_hash, from_address, to_address, value FROM transactions").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "0xabc"
    assert rows[0][1] == "0xA"
    assert rows[0][2] == "0xB"
    assert int(rows[0][3]) == 16  # 0x10 => 16

    logs = sm.conn.execute("SELECT tx_hash, address, data FROM logs").fetchall()
    assert len(logs) == 1
    assert logs[0][0] == "0xabc"
    assert logs[0][1] == "0xC"
