# streaming/consumers.py

from __future__ import annotations

from typing import Any, Dict

from common.kafka_sim import Broker, Message


async def consume_topic(broker: Broker, topic: str, group_id: str, on_message):
    """
    Generic loop for a single topic.
    on_message is an async function that accepts Message and returns None.
    """
    async for msg in broker.subscribe(topic, group_id):
        await on_message(msg)
        await broker.commit(topic, group_id, msg.offset)


async def noop_handler(msg: Message):
    # placeholder useful in tests
    return None
