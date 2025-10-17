from etl.erc20 import is_erc20_transfer, decode_erc20_transfer, TRANSFER_TOPIC0

def test_is_erc20_transfer():
    log = {"topics": [TRANSFER_TOPIC0, "0x" + "00"*31 + "aa", "0x" + "00"*31 + "bb"], "data": "0x0"}
    assert is_erc20_transfer(log) is True

def test_decode_erc20_transfer_basic():
    log = {
        "address": "0xToken",
        "transactionHash": "0xTX",
        "blockNumber": "0x10",
        "topics": [
            TRANSFER_TOPIC0,
            "0x" + "00"*12 + "1111111111111111111111111111111111111111",  # from
            "0x" + "00"*12 + "2222222222222222222222222222222222222222",  # to
        ],
        "data": "0x00000000000000000000000000000000000000000000000000000000000003e8"  # 1000
    }
    rec = decode_erc20_transfer(log)
    assert rec["tx_hash"] == "0xTX"
    assert rec["contract"] == "0xToken"
    assert rec["from"].lower().endswith("1111111111111111111111111111111111111111")
    assert rec["to"].lower().endswith("2222222222222222222222222222222222222222")
    assert rec["value"] == 1000
    assert rec["blockNumber"] == int("0x10", 16)
