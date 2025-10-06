# ingestion/parser.py
"""
ingestion.parser
Module to parse raw blockchain data into normalized schema.
"""

def parse_blocks(raw_blocks):
    raise NotImplementedError("parse_blocks not implemented")

def parse_transactions(raw_txns):
    raise NotImplementedError("parse_transactions not implemented")

def parse_logs(raw_logs):
    raise NotImplementedError("parse_logs not implemented")

def parse_transaction(tx_json: dict) -> dict:
    if not tx_json or "hash" not in tx_json:
        raise ValueError("Invalid transaction JSON")
    return {
        "tx_hash": tx_json["hash"],
        "from": tx_json.get("from"),
        "to": tx_json.get("to"),
        "value": tx_json.get("value"),
        "input_data": tx_json.get("input"),
    }

def parse_log(log_json: dict) -> dict:
    if not log_json or "topics" not in log_json:
        raise ValueError("Invalid log JSON")
    return {
        "address": log_json.get("address"),
        "topics": log_json.get("topics"),
        "data": log_json.get("data"),
        "blockNumber": log_json.get("blockNumber"),
        "transactionHash": log_json.get("transactionHash"),
    }
