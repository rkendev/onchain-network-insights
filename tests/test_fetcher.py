import pytest
from ingestion.fetcher import fetch_blocks, fetch_transactions

def test_fetch_blocks_not_implemented():
with pytest.raises(NotImplementedError):
fetch_blocks(0, 0)

def test_fetch_transactions_not_implemented():
with pytest.raises(NotImplementedError):
fetch_transactions((0, 0))
