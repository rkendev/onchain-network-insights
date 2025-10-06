import os
import tempfile
import pytest
from storage.sqlite_backend import SQLiteStorage

def test_sqlite_write_read_block(tmp_path):
    db = tmp_path / "test.db"
    ss = SQLiteStorage(str(db))
    ss.setup()
    blk = {"block_number": 5, "block_hash": "0xabc", "timestamp": 12345}
    ss.write_block(blk)
    got = ss.read_block(5)
    assert got["block_hash"] == "0xabc"
    assert got["block_number"] == 5

def test_sqlite_query_blocks(tmp_path):
    db = tmp_path / "test2.db"
    ss = SQLiteStorage(str(db))
    ss.setup()
    for i in range(3):
        ss.write_block({"block_number": i, "block_hash": f"h{i}", "timestamp": i*10})
    lst = ss.query_blocks(0, 2)
    assert len(lst) == 3
    assert lst[1]["block_number"] == 1
