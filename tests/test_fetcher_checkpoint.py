import pytest
from ingestion.fetcher import ingest_incremental
from ingestion.checkpoint import Checkpoint

def test_ingest_incremental_creates_checkpoint(tmp_path):
    ckpt = tmp_path / "ckpt.json"
    def fake_fetch(n): return {"number": hex(n)}
    new_last = ingest_incremental(batch_size=3, checkpoint_path=str(ckpt), fetch_block_fn=fake_fetch)
    assert new_last == 2
    assert Checkpoint(str(ckpt)).get_last() == 2
