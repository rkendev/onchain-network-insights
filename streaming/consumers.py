# streaming/consumers.py
from __future__ import annotations

from typing import Awaitable, Callable

from common.kafka_sim import Broker, Message
from streaming.sqlite_sink import SQLiteSink


async def consume_topic(
    broker: Broker,
    topic: str,
    group_id: str,
    on_message: Callable[[Message], Awaitable[None]],
) -> None:
    """
    Generic loop for a single topic.
    Consumes from the committed offset plus one, calls the handler, then commits.
    """
    async for msg in broker.subscribe(topic, group_id):
        await on_message(msg)
        await broker.commit(topic, group_id, msg.offset)


# simple no op handler used by tests
async def noop_handler(msg: Message) -> None:
    return None


async def consume_transactions_to_sqlite(broker: Broker, group_id: str, sqlite_path: str) -> None:
    """
    Read the transactions topic and persist into SQLite with idempotency by topic and key.
    """
    sink = SQLiteSink(sqlite_path)
    topic = "transactions"

    async def _handler(msg: Message) -> None:
        if sink.mark_seen(topic, msg.key):
            sink.write_tx_message(msg.value)

    await consume_topic(broker, topic, group_id, _handler)


async def consume_logs_to_sqlite(broker: Broker, group_id: str, sqlite_path: str) -> None:
    """
    Read the logs topic and persist into SQLite with idempotency by topic and key.
    """
    sink = SQLiteSink(sqlite_path)
    topic = "logs"

    async def _handler(msg: Message) -> None:
        if sink.mark_seen(topic, msg.key):
            sink.write_log_message(msg.value)

    await consume_topic(broker, topic, group_id, _handler)


async def consume_transfers_to_sqlite(broker: Broker, group_id: str, sqlite_path: str) -> None:
    """
    Read the transfers topic and persist into SQLite with idempotency by topic and key.
    """
    sink = SQLiteSink(sqlite_path)
    topic = "transfers"

    async def _handler(msg: Message) -> None:
        if sink.mark_seen(topic, msg.key):
            sink.write_transfer_message(msg.value)

    await consume_topic(broker, topic, group_id, _handler)
