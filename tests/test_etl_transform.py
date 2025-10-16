from etl.transform import transform_transactions, transform_logs

def test_transform_transactions_basic():
    raw = [{"hash": "0x1", "from": "0xA", "to": "0xB", "value": "0x10"}]
    out = transform_transactions(raw)
    assert out[0]["tx_hash"] == "0x1"
    assert out[0]["value"] == 16

def test_transform_logs_basic():
    raw = [{"transactionHash": "0x1", "address": "0xC", "data": "0x0"}]
    out = transform_logs(raw)
    assert out[0]["transactionHash"] == "0x1"
