from storage.sqlite_backend import SQLiteStorage

def test_sqlite_write_transfer(tmp_path):
    db = tmp_path / "t.db"
    sm = SQLiteStorage(str(db))
    sm.setup()
    tr = {
        "tx_hash": "0x1",
        "contract": "0xToken",
        "from": "0xAAA",
        "to": "0xBBB",
        "value": 100,
        "blockNumber": 16,
    }
    sm.write_transfer(tr)
    rows = sm.conn.execute(
        "SELECT tx_hash, contract, sender, recipient, value, block_number FROM transfers"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "0x1"
    assert rows[0][1] == "0xToken"
    assert rows[0][2] == "0xAAA"
    assert rows[0][3] == "0xBBB"
    assert rows[0][4] == 100
    assert rows[0][5] == 16
