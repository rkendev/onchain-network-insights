"""
ingestion.fetcher

Module to fetch raw blockchain data: blocks, transactions, logs.
"""
def fetch_blocks(start_block: int, end_block: int):
    raise NotImplementedError("fetch_blocks not implemented")

def fetch_transactions(block_range):
    raise NotImplementedError("fetch_transactions not implemented")
