# common/kafka_sim/sqlite_backend.py

from __future__ import annotations

import sqlite3
from typing import Any, AsyncIterator, Dict

from common.kafka_sim import Broker, Message


class SQLiteBroker(Broker):
    """
    Placeholder for a durable broker backed by SQLite.
    Not used yet. Tests will be added in the next step.
    """

    def __init__(self, path: str):
        self.path = path
        self._conn = sqlite3.connect(self.path, check_same_thread=False)

    async def publish(self, topic: str, key: str, value: Dict[str, Any]) -> int:
        raise NotImplementedError

    async def subscribe(self, topic: str, group_id: str) -> AsyncIterator[Message]:
        raise NotImplementedError

    async def commit(self, topic: str, group_id: str, offset: int) -> None:
        raise NotImplementedError

    async def get_offset(self, topic: str, group_id: str) -> int:
        return -1
