# tests/test_analytics_token_holders_extras.py
from storage.sqlite_backend import SQLiteStorage
from analytics.holders import (
    holder_balances_sqlite, holder_deltas_sqlite,
    top_gainers_sqlite, top_spenders_sqlite, distribution_metrics_sqlite
)

CONTRACT = "0xToken"

def _seed(sm: SQLiteStorage):
    sm.setup()
    # Mint to A: +500 at block 10
    sm.write_transfer({
        "tx_hash": "0x1", "contract": CONTRACT,
        "from": "0x0000000000000000000000000000000000000000",
        "to": "0xA", "value": 500, "blockNumber": 10
    })
    # A -> B: 100 at block 11
    sm.write_transfer({
        "tx_hash": "0x2", "contract": CONTRACT,
        "from": "0xA", "to": "0xB", "value": 100, "blockNumber": 11
    })
    # B -> C: 40 at block 12
    sm.write_transfer({
        "tx_hash": "0x3", "contract": CONTRACT,
        "from": "0xB", "to": "0xC", "value": 40, "blockNumber": 12
    })

def test_holder_balances_sqlite(tmp_path):
    db = tmp_path / "h.db"
    sm = SQLiteStorage(str(db)); _seed(sm)
    bals = holder_balances_sqlite(str(db), CONTRACT)
    by = {x["address"]: x["balance"] for x in bals}
    assert by["0xA"] == 400
    assert by["0xB"] == 60
    assert by["0xC"] == 40

def test_holder_deltas_window(tmp_path):
    db = tmp_path / "w.db"
    sm = SQLiteStorage(str(db)); _seed(sm)
    # Window (10, 12] includes transfers at 11 and 12
    deltas = holder_deltas_sqlite(str(db), CONTRACT, start_block=10, end_block=12)
    dd = {x["address"]: x["delta"] for x in deltas}
    # A sent 100 -> -100; B +100 -40 = +60; C +40
    assert dd["0xA"] == -100
    assert dd["0xB"] == 60
    assert dd["0xC"] == 40

def test_top_gainers_spenders(tmp_path):
    db = tmp_path / "g.db"
    sm = SQLiteStorage(str(db)); _seed(sm)
    gainers = top_gainers_sqlite(str(db), CONTRACT, n=2, start_block=10, end_block=12)
    spenders = top_spenders_sqlite(str(db), CONTRACT, n=2, start_block=10, end_block=12)
    assert [g["address"] for g in gainers] == ["0xB", "0xC"]  # +60, +40
    assert [s["address"] for s in spenders] == ["0xA"]        # -100

def test_distribution_metrics(tmp_path):
    db = tmp_path / "d.db"
    sm = SQLiteStorage(str(db)); _seed(sm)
    m = distribution_metrics_sqlite(str(db), CONTRACT)
    # sanity: hhi in (0,1], gini in [0,1]
    assert 0 < m["hhi"] <= 1
    assert 0 <= m["gini"] <= 1
