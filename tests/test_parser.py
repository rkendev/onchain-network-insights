# tests/test_parser.py
import pytest
from ingestion.parser import parse_blocks, parse_transactions, parse_logs, parse_transaction, parse_log

def test_parse_blocks_not_implemented():
    with pytest.raises(NotImplementedError):
        parse_blocks([])

def test_parse_transactions_not_implemented():
    with pytest.raises(NotImplementedError):
        parse_transactions([])

def test_parse_logs_not_implemented():
    with pytest.raises(NotImplementedError):
        parse_logs([])

def test_parse_transaction_invalid():
    with pytest.raises(ValueError):
        parse_transaction({})

def test_parse_log_invalid():
    with pytest.raises(ValueError):
        parse_log({})
