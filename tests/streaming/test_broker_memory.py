import asyncio
import pytest

from common.kafka_sim import MemoryBroker


@pytest.mark.asyncio
async def test_publish_and_subscribe_in_order():
    b = MemoryBroker()
    topic = "logs"
    group = "g1"

    # publish three messages
    await b.publish(topic, "k1", {"i": 1})
    await b.publish(topic, "k2", {"i": 2})
    await b.publish(topic, "k3", {"i": 3})

    got = []

    async def reader():
        async for m in b.subscribe(topic, group):
            got.append((m.offset, m.key, m.value["i"]))
            if len(got) == 3:
                await b.commit(topic, group, m.offset)
                break

    await asyncio.wait_for(reader(), timeout=1.0)
    assert got == [(0, "k1", 1), (1, "k2", 2), (2, "k3", 3)]


@pytest.mark.asyncio
async def test_resume_from_committed_offset():
    b = MemoryBroker()
    topic = "txs"
    group = "g2"

    for i in range(5):
        await b.publish(topic, f"k{i}", {"i": i})

    # commit up to offset 2
    await b.commit(topic, group, 2)

    got = []

    async def reader():
        async for m in b.subscribe(topic, group):
            got.append(m.value["i"])
            if len(got) == 2:
                await b.commit(topic, group, m.offset)
                break

    await asyncio.wait_for(reader(), timeout=1.0)
    # should resume at offset 3 and 4
    assert got == [3, 4]
