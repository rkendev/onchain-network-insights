import pytest
from storage.postgres_backend import PostgresStorage

@pytest.mark.skip(reason="Requires Postgres DSN / setup")
def test_postgres_storage(tmp_path):
    # Example DSN: "postgresql://user:pass@localhost:5432/dbname"
    dsn = "postgresql://user:password@localhost:5432/testdb"
    ps = PostgresStorage(dsn)
    ps.setup()
    blk = {"block_number": 2, "block_hash": "0xdef", "timestamp": 222}
    ps.write_block(blk)
    got = ps.read_block(2)
    assert got["block_hash"] == "0xdef"
