# storage/schema.py
CREATE_TABLE_BLOCKS = """
CREATE TABLE IF NOT EXISTS blocks (
    block_number BIGINT PRIMARY KEY,
    block_hash TEXT,
    timestamp BIGINT
);
"""

CREATE_TABLE_TXS = """
CREATE TABLE IF NOT EXISTS transactions (
    tx_hash TEXT PRIMARY KEY,
    from_address TEXT,
    to_address TEXT,
    value TEXT
);
"""

CREATE_TABLE_LOGS = """
CREATE TABLE IF NOT EXISTS logs (
    tx_hash TEXT,
    address TEXT,
    data TEXT,
    PRIMARY KEY (tx_hash, address)
);
"""
