import asyncio
import pytest

from common.kafka_sim import MemoryBroker
from streaming.consumers import consume_topic, noop_handler


@pytest.mark.asyncio
async def test_consumer_commits_offsets():
    b = MemoryBroker()
    topic = "transactions"
    group = "cg"

    for i in range(3):
        await b.publish(topic, f"k{i}", {"i": i})

    # run consumer until it processes three messages
    processed = []

    async def handler(msg):
        processed.append(msg.value["i"])

    async def runner():
        await asyncio.wait_for(consume_topic(b, topic, group, handler), timeout=0.05)

    # consume in the background for a short time
    try:
        await runner()
    except asyncio.TimeoutError:
        pass

    # It should have processed at least one item and committed the last offset it saw
    assert processed  # non empty
    last = await b.get_offset(topic, group)
    assert last >= 0
