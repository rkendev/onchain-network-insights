# tests/test_analytics_whales.py
from storage.sqlite_backend import SQLiteStorage
from analytics.whales import find_whales_sqlite, concentration_ratios_sqlite

CONTRACT = "0xToken"

def _seed_transfers(sm: SQLiteStorage, contract=CONTRACT):
    sm.setup()
    # Mint-like inflow to A: +500
    sm.write_transfer({
        "tx_hash": "0x1", "contract": contract,
        "from": "0x0000000000000000000000000000000000000000",
        "to": "0xA", "value": 500, "blockNumber": 10
    })
    # A -> B: 100
    sm.write_transfer({
        "tx_hash": "0x2", "contract": contract,
        "from": "0xA", "to": "0xB", "value": 100, "blockNumber": 11
    })
    # B -> C: 40
    sm.write_transfer({
        "tx_hash": "0x3", "contract": contract,
        "from": "0xB", "to": "0xC", "value": 40, "blockNumber": 12
    })


def test_find_whales_sqlite(tmp_path):
    db = tmp_path / "whales.db"
    sm = SQLiteStorage(str(db))
    _seed_transfers(sm)

    # Final balances: A=400, B=60, C=40
    whales = find_whales_sqlite(str(db), CONTRACT, min_balance=100)
    addresses = [w["address"] for w in whales]
    assert addresses == ["0xA"]  # only A >= 100

    whales2 = find_whales_sqlite(str(db), CONTRACT, min_balance=50)
    addresses2 = [w["address"] for w in whales2]
    assert addresses2 == ["0xA", "0xB"]


def test_concentration_ratios_sqlite(tmp_path):
    db = tmp_path / "cr.db"
    sm = SQLiteStorage(str(db))
    _seed_transfers(sm)

    # Final balances: A=400, B=60, C=40 -> total = 500
    cr = concentration_ratios_sqlite(str(db), CONTRACT, ks=(1, 2, 3, 10))
    # CR1 = top1 / total = 400/500 = 0.8
    assert abs(cr[1] - 0.8) < 1e-9
    # CR2 = (400 + 60) / 500 = 0.92
    assert abs(cr[2] - 0.92) < 1e-9
    # CR3 = (400 + 60 + 40) / 500 = 1.0
    assert abs(cr[3] - 1.0) < 1e-9
    # CR10 (more than number of holders) == CR3 == 1.0
    assert abs(cr[10] - 1.0) < 1e-9


def test_as_of_block(tmp_path):
    db = tmp_path / "asof.db"
    sm = SQLiteStorage(str(db))
    _seed_transfers(sm)

    # Up to block 11: A=400, B=100, total=500
    whales = find_whales_sqlite(str(db), CONTRACT, min_balance=100, as_of_block=11)
    # A and B are >=100 at this point
    addrs = [w["address"] for w in whales]
    assert addrs == ["0xA", "0xB"]

    cr = concentration_ratios_sqlite(str(db), CONTRACT, ks=(1, 2, 3), as_of_block=11)
    # balances as of 11: A=400, B=100, (C=0)
    # CR1 = 400/500 = 0.8
    assert abs(cr[1] - 0.8) < 1e-9
    # CR2 = (400+100)/500 = 1.0
    assert abs(cr[2] - 1.0) < 1e-9
    # CR3 = still 1.0 (no third holder with nonzero balance)
    assert abs(cr[3] - 1.0) < 1e-9
