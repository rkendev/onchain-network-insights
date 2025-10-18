from storage.sqlite_backend import SQLiteStorage
from analytics.token_holders import balances_as_of_sqlite, top_holders_sqlite

def _seed_transfers(sm: SQLiteStorage, contract="0xToken"):
    sm.setup()
    # Simple distribution:
    # A -> B : 100
    # mint to A : +500 (sender = 0x0...000 treat as mint if appears; here we model as recipient inflow)
    sm.write_transfer({
        "tx_hash": "0x1", "contract": contract,
        "from": "0x0000000000000000000000000000000000000000",
        "to": "0xA", "value": 500, "blockNumber": 10
    })
    sm.write_transfer({
        "tx_hash": "0x2", "contract": contract,
        "from": "0xA", "to": "0xB", "value": 100, "blockNumber": 11
    })
    sm.write_transfer({
        "tx_hash": "0x3", "contract": contract,
        "from": "0xB", "to": "0xC", "value": 40, "blockNumber": 12
    })

def test_balances_as_of_sqlite(tmp_path):
    db = tmp_path / "holders.db"
    sm = SQLiteStorage(str(db))
    _seed_transfers(sm)

    bals_all = balances_as_of_sqlite(str(db), "0xToken")
    # Expected final balances:
    # A: 400  (500 in - 100 out)
    # B: 60   (100 in - 40 out)
    # C: 40   (40 in)
    by_addr = {x["address"]: x["balance"] for x in bals_all}
    assert by_addr["0xA"] == 400
    assert by_addr["0xB"] == 60
    assert by_addr["0xC"] == 40

def test_balances_as_of_block_cutoff(tmp_path):
    db = tmp_path / "holders2.db"
    sm = SQLiteStorage(str(db))
    _seed_transfers(sm)

    bals_cut = balances_as_of_sqlite(str(db), "0xToken", as_of_block=11)
    # After block 11:
    # A: 400 (same as final: 500 in - 100 out)
    # B: 100 (only 0x2 applied)
    by_addr = {x["address"]: x["balance"] for x in bals_cut}
    assert by_addr["0xA"] == 400
    assert by_addr["0xB"] == 100
    assert "0xC" not in by_addr  # transfer at block 12 not yet included

def test_top_holders_sqlite(tmp_path):
    db = tmp_path / "holders3.db"
    sm = SQLiteStorage(str(db))
    _seed_transfers(sm)

    top2 = top_holders_sqlite(str(db), "0xToken", n=2)
    assert len(top2) == 2
    assert top2[0]["address"] == "0xA"
    assert top2[0]["balance"] == 400
    assert top2[1]["address"] == "0xB"
