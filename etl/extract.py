from ingestion.fetcher import fetch_block

def extract_block(block_number: int) -> dict:
    return fetch_block(block_number)
