import pytest
from ingestion.fetcher import fetch_transaction, fetch_logs

def test_fetch_transaction_invalid_input():
    with pytest.raises(ValueError):
        fetch_transaction(123)  # not a string

def test_fetch_logs_invalid_range():
    with pytest.raises(ValueError):
        fetch_logs("0xABC", 100, 50)  # from > to
