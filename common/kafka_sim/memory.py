# common/kafka_sim/memory.py

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List


@dataclass(frozen=True)
class Message:
    topic: str
    offset: int
    key: str
    value: Dict[str, Any]
    produced_at: float
    schema_version: str = "v1"


class Broker:
    async def publish(self, topic: str, key: str, value: Dict[str, Any]) -> int:
        raise NotImplementedError

    async def subscribe(self, topic: str, group_id: str) -> AsyncIterator[Message]:
        raise NotImplementedError

    async def commit(self, topic: str, group_id: str, offset: int) -> None:
        raise NotImplementedError

    async def get_offset(self, topic: str, group_id: str) -> int:
        raise NotImplementedError


class MemoryBroker(Broker):
    """
    Single partition per topic in memory broker.

    rules
    one never yield while holding a lock
    two reads compute the next message under lock then release before returning it
    three commits only update the committed offset if it moves forward
    """

    def __init__(self) -> None:
        self._topics: Dict[str, List[Message]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._offsets: Dict[str, Dict[str, int]] = {}  # topic -> group -> offset

    def _lock(self, topic: str) -> asyncio.Lock:
        if topic not in self._locks:
            self._locks[topic] = asyncio.Lock()
        return self._locks[topic]

    async def publish(self, topic: str, key: str, value: Dict[str, Any]) -> int:
        async with self._lock(topic):
            seq = self._topics.setdefault(topic, [])
            offset = len(seq)
            msg = Message(
                topic=topic,
                offset=offset,
                key=str(key),
                value=json.loads(json.dumps(value)),  # json safe copy
                produced_at=time.time(),
                schema_version="v1",
            )
            seq.append(msg)
            return offset

    async def subscribe(self, topic: str, group_id: str) -> AsyncIterator[Message]:
        # start from committed offset plus one
        start = await self.get_offset(topic, group_id)
        next_offset = start + 1

        while True:
            # fetch next message reference under lock then release before yielding
            async with self._lock(topic):
                seq = self._topics.setdefault(topic, [])
                msg = seq[next_offset] if next_offset < len(seq) else None

            if msg is not None:
                next_offset += 1
                # yield outside the lock to avoid deadlocks with commit
                yield msg
                continue

            # no new messages yet
            await asyncio.sleep(0.01)

    async def commit(self, topic: str, group_id: str, offset: int) -> None:
        async with self._lock(topic):
            groups = self._offsets.setdefault(topic, {})
            current = groups.get(group_id, -1)
            if offset > current:
                groups[group_id] = offset

    async def get_offset(self, topic: str, group_id: str) -> int:
        async with self._lock(topic):
            return self._offsets.setdefault(topic, {}).get(group_id, -1)
