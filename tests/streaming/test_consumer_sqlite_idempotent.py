import asyncio
import sqlite3
import pytest

from common.kafka_sim import MemoryBroker
from streaming.consumers import consume_transactions_to_sqlite, consume_logs_to_sqlite


@pytest.mark.asyncio
async def test_transactions_consumer_idempotent(tmp_path):
    db = tmp_path / "sink.db"
    broker = MemoryBroker()

    # publish three tx messages, then repeat them
    txs = [
        {"hash": "0xA", "from": "0x1", "to": "0x2", "value": "0x10"},
        {"hash": "0xB", "from": "0x1", "to": "0x3", "value": "0x01"},
        {"hash": "0xC", "from": "0x2", "to": "0x3", "value": "0x02"},
    ]
    for tx in txs:
        await broker.publish("transactions", tx["hash"], {**tx})
    for tx in txs:
        await broker.publish("transactions", tx["hash"], {**tx})  # duplicates

    # run consumer until it processes 6 messages
    async def run_consumer():
        # consume in the background for a short bounded time
        task = asyncio.create_task(consume_transactions_to_sqlite(broker, "cg_tx", str(db)))
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    await run_consumer()

    # verify only 3 unique rows exist in transactions
    con = sqlite3.connect(str(db))
    cnt = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert cnt == 3
    vals = [r[0] for r in con.execute("SELECT tx_hash FROM transactions ORDER BY tx_hash").fetchall()]
    assert vals == ["0xA", "0xB", "0xC"]


@pytest.mark.asyncio
async def test_logs_consumer_idempotent(tmp_path):
    db = tmp_path / "sink.db"
    broker = MemoryBroker()

    # publish two logs and duplicates
    logs = [
        {"transactionHash": "0xT1", "address": "0xC", "logIndex": 0, "topics": [], "data": "0x00"},
        {"transactionHash": "0xT1", "address": "0xC", "logIndex": 1, "topics": [], "data": "0x00"},
    ]
    for lg in logs:
        key = f"{lg['transactionHash']}:{int(lg['logIndex'])}"
        await broker.publish("logs", key, {**lg})
    for lg in logs:
        key = f"{lg['transactionHash']}:{int(lg['logIndex'])}"
        await broker.publish("logs", key, {**lg})  # duplicates

    async def run_consumer():
        task = asyncio.create_task(consume_logs_to_sqlite(broker, "cg_lg", str(db)))
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    await run_consumer()

    con = sqlite3.connect(str(db))
    cnt = con.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
    assert cnt == 2
