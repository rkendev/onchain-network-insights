# streaming/historical_feeder.py

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, Tuple, Callable

from common.kafka_sim.memory import MemoryBroker


ExtractFn = Callable[[int], Dict[str, Any]]


async def produce_historical_blocks(
    start_block: int,
    end_block: Optional[int],
    broker: MemoryBroker,
    extract_block: ExtractFn,
    *,
    contract_filter: Optional[str] = None,
    concurrency: int = 8,
) -> Tuple[int, int, int]:
    """
    Asynchronously fetch blocks in an inclusive range and publish to topics
    blocks transactions and logs.

    Returns a tuple
    number_of_blocks_published
    number_of_transactions_published
    number_of_logs_published
    """
    if end_block is None:
        end_block = start_block
    if end_block < start_block:
        start_block, end_block = end_block, start_block

    sem = asyncio.Semaphore(max(1, int(concurrency)))

    blocks_count = 0
    tx_count = 0
    log_count = 0

    async def handle_one(bn: int) -> Tuple[int, int, int]:
        async with sem:
            # run the sync extractor off the event loop
            raw = await asyncio.to_thread(extract_block, bn)
            if raw is None:
                raw = {}

            # publish block header
            await broker.publish(
                "blocks",
                key=str(bn),
                value={
                    "block_number": bn,
                    "block_hash": raw.get("hash", f"0x{bn:064x}"),
                    "timestamp": int(raw.get("timestamp", 0)),
                    "parent_hash": raw.get("parentHash", "0x" + "0" * 64),
                },
            )

            # publish transactions
            txs = list(raw.get("transactions") or [])
            for tx in txs:
                await broker.publish(
                    "transactions",
                    key=str(tx.get("hash")),
                    value={**tx, "block_number": bn},
                )

            # publish logs with optional contract filter
            logs = list(raw.get("logs") or [])
            logs_to_publish = []
            if contract_filter:
                cf = contract_filter.lower()
                for lg in logs:
                    if str(lg.get("address", "")).lower() == cf:
                        logs_to_publish.append(lg)
            else:
                logs_to_publish = logs

            for lg in logs_to_publish:
                key = f"{lg.get('transactionHash')}:{int(lg.get('logIndex', 0))}"
                await broker.publish(
                    "logs",
                    key=key,
                    value={**lg, "block_number": bn},
                )

            return 1, len(txs), len(logs_to_publish)

    tasks = [asyncio.create_task(handle_one(bn)) for bn in range(start_block, end_block + 1)]
    for b, t, l in await asyncio.gather(*tasks):
        blocks_count += b
        tx_count += t
        log_count += l

    return blocks_count, tx_count, log_count
