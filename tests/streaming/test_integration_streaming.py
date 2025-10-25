import asyncio
import pytest

from common.kafka_sim.memory import MemoryBroker, Message
from streaming.historical_feeder import produce_historical_blocks


def fake_extract_block(bn: int):
    return {
        "hash": f"0xB{bn}",
        "timestamp": 1800000000 + bn,
        "parentHash": f"0xP{bn-1}",
        "transactions": [{"hash": f"0xT{bn}-0"}, {"hash": f"0xT{bn}-1"}],
        "logs": [
            {"transactionHash": f"0xT{bn}-0", "address": "0xC", "logIndex": 0, "topics": [], "data": "0x00"},
            {"transactionHash": f"0xT{bn}-1", "address": "0xC", "logIndex": 1, "topics": [], "data": "0x00"},
        ],
    }


async def consume_topic_until(broker: MemoryBroker, topic: str, group: str, target: int):
    processed = 0
    async for msg in broker.subscribe(topic, group):
        processed += 1
        await broker.commit(topic, group, msg.offset)
        if processed >= target:
            return processed
    return processed


@pytest.mark.asyncio
async def test_end_to_end_streaming_flow():
    b = MemoryBroker()

    # produce three blocks of synthetic data
    blocks, txs, logs = await produce_historical_blocks(
        100, 102, b, fake_extract_block, concurrency=3
    )
    assert blocks == 3 and txs == 6 and logs == 6

    # consume transactions and logs concurrently
    tx_task = asyncio.create_task(consume_topic_until(b, "transactions", "cg_tx", txs))
    lg_task = asyncio.create_task(consume_topic_until(b, "logs", "cg_lg", logs))

    tx_done, lg_done = await asyncio.gather(tx_task, lg_task)
    assert tx_done == txs
    assert lg_done == logs

    # verify committed offsets advanced to last message
    tx_last = await b.get_offset("transactions", "cg_tx")
    lg_last = await b.get_offset("logs", "cg_lg")
    assert tx_last == txs - 1
    assert lg_last == logs - 1
