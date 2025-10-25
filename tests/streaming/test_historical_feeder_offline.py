import asyncio
import pytest

from common.kafka_sim.memory import MemoryBroker
from streaming.historical_feeder import produce_historical_blocks


def fake_extract_block(bn: int):
    # two transactions and two logs per block
    return {
        "hash": f"0xB{bn}",
        "timestamp": 1700000000 + bn,
        "parentHash": f"0xP{bn-1}",
        "transactions": [{"hash": f"0xT{bn}-0"}, {"hash": f"0xT{bn}-1"}],
        "logs": [
            {"transactionHash": f"0xT{bn}-0", "address": "0xC", "logIndex": 0, "topics": [], "data": "0x00"},
            {"transactionHash": f"0xT{bn}-1", "address": "0xC", "logIndex": 1, "topics": [], "data": "0x00"},
        ],
    }


@pytest.mark.asyncio
async def test_feeder_publishes_expected_counts():
    broker = MemoryBroker()
    blocks, txs, logs = await produce_historical_blocks(
        start_block=3,
        end_block=5,
        broker=broker,
        extract_block=fake_extract_block,
        concurrency=3,
    )
    assert blocks == 3
    assert txs == 6
    assert logs == 6

    # minimal spot checks through a subscription
    got_blocks = []
    async def read_three():
        async for m in broker.subscribe("blocks", "g1"):
            got_blocks.append(m.value["block_number"])
            if len(got_blocks) == 3:
                await broker.commit("blocks", "g1", m.offset)
                break
    await asyncio.wait_for(read_three(), timeout=1.0)
    assert got_blocks == [3, 4, 5]


@pytest.mark.asyncio
async def test_feeder_contract_filter():
    broker = MemoryBroker()

    def fake_extract_block_mixed(bn: int):
        # one matching address and one non matching
        return {
            "transactions": [],
            "logs": [
                {"transactionHash": f"0xT{bn}-A", "address": "0xC", "logIndex": 0, "topics": [], "data": "0x00"},
                {"transactionHash": f"0xT{bn}-B", "address": "0xD", "logIndex": 1, "topics": [], "data": "0x00"},
            ],
        }

    blocks, txs, logs = await produce_historical_blocks(
        start_block=10,
        end_block=12,
        broker=broker,
        extract_block=fake_extract_block_mixed,
        contract_filter="0xC",
        concurrency=2,
    )
    assert blocks == 3
    assert txs == 0
    assert logs == 3  # only the matching address is published
