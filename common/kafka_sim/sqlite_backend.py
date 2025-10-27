# common/kafka_sim/sqlite_backend.py

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Optional

# reuse the Broker and Message types so tests stay consistent
from .memory import Broker, Message


_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS messages (
  topic         TEXT NOT NULL,
  offset        INTEGER NOT NULL,
  key           TEXT NOT NULL,
  value_json    TEXT NOT NULL,
  produced_at   REAL NOT NULL,
  schema_version TEXT NOT NULL,
  PRIMARY KEY(topic, offset)
);
CREATE INDEX IF NOT EXISTS idx_messages_topic_offset ON messages(topic, offset);

CREATE TABLE IF NOT EXISTS consumer_offsets (
  topic      TEXT NOT NULL,
  group_id   TEXT NOT NULL,
  offset     INTEGER NOT NULL,
  updated_at REAL NOT NULL,
  PRIMARY KEY(topic, group_id)
);
"""


class SQLiteBroker(Broker):
    """
    Durable single-partition-per-topic broker using SQLite.

    contract
    publish returns the assigned offset for the topic
    subscribe yields messages after the committed offset for group_id
    commit stores the highest processed offset for group_id
    get_offset returns the committed offset for group_id or -1 if none
    """

    def __init__(self, path: str):
        self.path = path
        # a single connection is fine for our usage here
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._setup_done = False
        self._setup()

    def _setup(self) -> None:
        if self._setup_done:
            return
        cur = self._conn.cursor()
        cur.executescript(_SCHEMA)
        self._conn.commit()
        self._setup_done = True

    # --------------- sync helpers run in a thread ---------------

    def _publish_sync(self, topic: str, key: str, value: Dict[str, Any]) -> int:
        cur = self._conn.cursor()
        # compute next offset per topic
        row = cur.execute("SELECT COALESCE(MAX(offset), -1) AS last FROM messages WHERE topic = ?", (topic,)).fetchone()
        next_offset = int(row["last"]) + 1
        cur.execute(
            """
            INSERT INTO messages(topic, offset, key, value_json, produced_at, schema_version)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (topic, next_offset, str(key), json.dumps(value), float(time.time()), "v1"),
        )
        self._conn.commit()
        return next_offset

    def _get_row_sync(self, topic: str, offset: int) -> Optional[sqlite3.Row]:
        cur = self._conn.cursor()
        return cur.execute(
            "SELECT topic, offset, key, value_json, produced_at, schema_version FROM messages WHERE topic = ? AND offset = ?",
            (topic, int(offset)),
        ).fetchone()

    def _commit_sync(self, topic: str, group_id: str, offset: int) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO consumer_offsets(topic, group_id, offset, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(topic, group_id) DO UPDATE SET
              offset = CASE WHEN excluded.offset > consumer_offsets.offset THEN excluded.offset ELSE consumer_offsets.offset END,
              updated_at = excluded.updated_at
            """,
            (topic, group_id, int(offset), float(time.time())),
        )
        self._conn.commit()

    def _get_offset_sync(self, topic: str, group_id: str) -> int:
        cur = self._conn.cursor()
        row = cur.execute(
            "SELECT offset FROM consumer_offsets WHERE topic = ? AND group_id = ?",
            (topic, group_id),
        ).fetchone()
        return int(row["offset"]) if row else -1

    # --------------- async interface ---------------

    async def publish(self, topic: str, key: str, value: Dict[str, Any]) -> int:
        return await asyncio.to_thread(self._publish_sync, topic, key, value)

    async def subscribe(self, topic: str, group_id: str) -> AsyncIterator[Message]:
        # start after committed offset
        start = await self.get_offset(topic, group_id)
        next_offset = start + 1
        while True:
            # fetch the next row without blocking the event loop
            row = await asyncio.to_thread(self._get_row_sync, topic, next_offset)
            if row is not None:
                msg = Message(
                    topic=row["topic"],
                    offset=int(row["offset"]),
                    key=row["key"],
                    value=json.loads(row["value_json"]),
                    produced_at=float(row["produced_at"]),
                    schema_version=row["schema_version"],
                )
                next_offset += 1
                yield msg
                continue
            await asyncio.sleep(0.01)

    async def commit(self, topic: str, group_id: str, offset: int) -> None:
        await asyncio.to_thread(self._commit_sync, topic, group_id, offset)

    async def get_offset(self, topic: str, group_id: str) -> int:
        return await asyncio.to_thread(self._get_offset_sync, topic, group_id)
