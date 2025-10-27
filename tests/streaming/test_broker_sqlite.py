import asyncio
import json
import os
import pytest

from common.kafka_sim import SQLiteBroker


@pytest.mark.asyncio
async def test_sqlite_publish_persist_and_read(tmp_path):
    db = tmp_path / "broker.db"
    b = SQLiteBroker(str(db))

    # publish three messages and capture offsets
    offs = []
    for i in range(3):
        o = await b.publish("logs", f"k{i}", {"i": i})
        offs.append(o)

    assert offs == [0, 1, 2]

    # new instance should see the same messages
    b2 = SQLiteBroker(str(db))

    got = []

    async def read_three():
        async for m in b2.subscribe("logs", "g1"):
            got.append((m.offset, m.key, m.value["i"]))
            if len(got) == 3:
                await b2.commit("logs", "g1", m.offset)
                break

    await asyncio.wait_for(read_three(), timeout=1.0)
    assert got == [(0, "k0", 0), (1, "k1", 1), (2, "k2", 2)]

    # committed offset is durable
    b3 = SQLiteBroker(str(db))
    off = await b3.get_offset("logs", "g1")
    assert off == 2


@pytest.mark.asyncio
async def test_sqlite_resume_from_committed_offset(tmp_path):
    db = tmp_path / "broker.db"
    topic = "txs"
    group = "cg"

    b = SQLiteBroker(str(db))
    for i in range(5):
        await b.publish(topic, f"k{i}", {"i": i})

    # commit up to offset 2
    await b.commit(topic, group, 2)

    got = []

    async def reader():
        async for m in b.subscribe(topic, group):
            got.append(m.value["i"])
            await b.commit(topic, group, m.offset)
            if len(got) == 2:
                break

    await asyncio.wait_for(reader(), timeout=1.0)
    # should resume at 3 and 4
    assert got == [3, 4]

    # restart and ensure commit persisted at 4
    b2 = SQLiteBroker(str(db))
    off = await b2.get_offset(topic, group)
    assert off == 4
