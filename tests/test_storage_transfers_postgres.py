import pytest
from storage.postgres_backend import PostgresStorage

@pytest.mark.skip(reason="Requires a running Postgres instance and DSN")
def test_pg_write_transfer():
    ps = PostgresStorage("postgresql://user:pass@localhost:5432/db")
    ps.setup()
    ps.write_transfer({
        "tx_hash": "0x1",
        "contract": "0xToken",
        "from": "0xAAA",
        "to": "0xBBB",
        "value": 100,
        "blockNumber": 16,
    })
