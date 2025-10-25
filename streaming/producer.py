# streaming/producer.py
import asyncio
from typing import Any, Dict
from common.kafka_sim.memory import MemoryBroker

async def produce_blocks(broker: MemoryBroker, blocks: list[Dict[str, Any]], topic: str = "blocks"):
    """Publish each block into the in-memory broker."""
    for block in blocks:
        await broker.publish(topic, key=str(block.get("number")), value=block)
        await asyncio.sleep(0)  # yield control
