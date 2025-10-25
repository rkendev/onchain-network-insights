# tests/streaming/test_producer_offline.py
import asyncio
import pytest
from common.kafka_sim.memory import MemoryBroker
from streaming.producer import produce_blocks

# def fake_extract_block(bn: int):
#     return {
#         "hash": f"0xB{bn}",
#         "timestamp": 1000 + bn,
#         "parentHash": f"0xP{bn-1}",
#         "transactions": [{"hash": f"0xT{bn}-0"}, {"hash": f"0xT{bn}-1"}],
#         "logs": [
#             {"transactionHash": f"0xT{bn}-0", "address": "0xC", "logIndex": 0, "topics": [], "data": "0x00"},
#             {"transactionHash": f"0xT{bn}-1", "address": "0xC", "logIndex": 1, "topics": [], "data": "0x00"},
#         ],
#     }

# @pytest.mark.asyncio
# async def test_producer_publishes_blocks_txs_logs():
#     b = MemoryBroker()
#     counts = await produce_blocks(3, 5, b, fake_extract_block, concurrency=2)
#     assert counts == (3, 6, 6)

#     # read back quickly by peeking offsets
#     off_blocks = await b.get_offset("blocks", "g")
#     off_txs = await b.get_offset("transactions", "g")
#     off_logs = await b.get_offset("logs", "g")
#     # no commits yet, so offsets remain default -1
#     assert off_blocks == -1
#     assert off_txs == -1
#     assert off_logs == -1

#     # now consume a few to prove they exist
#     got_blocks = []

#     async def read_some():
#         async for m in b.subscribe("blocks", "g"):
#             got_blocks.append(m.value["block_number"])
#             if len(got_blocks) == 3:
#                 await b.commit("blocks", "g", m.offset)
#                 break

#     await asyncio.wait_for(read_some(), timeout=1.0)
#     assert got_blocks == [3, 4, 5]

@pytest.mark.asyncio
async def test_produce_blocks_basic():
    broker = MemoryBroker()
    blocks = [{"number": i, "hash": f"0x{i:02x}"} for i in range(3)]
    await produce_blocks(broker, blocks)
    assert len(broker._topics["blocks"]) == 3
