import os
import tempfile
import pytest
from ingestion.checkpoint import Checkpoint, CheckpointError

def test_checkpoint_read_none(tmp_path):
    cp = Checkpoint(str(tmp_path / "ckpt.json"))
    assert cp.get_last() is None

def test_checkpoint_read_and_update(tmp_path):
    cp_path = tmp_path / "ckpt.json"
    cp = Checkpoint(str(cp_path))
    cp.update(123)
    assert cp.get_last() == 123

def test_checkpoint_bad_json(tmp_path):
    cp_file = tmp_path / "ckpt.json"
    cp_file.write_text("not a valid json")
    cp = Checkpoint(str(cp_file))
    with pytest.raises(CheckpointError):
        _ = cp.get_last()
