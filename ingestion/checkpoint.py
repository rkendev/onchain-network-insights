import json
import os
from typing import Optional

class CheckpointError(Exception):
    pass

class Checkpoint:
    def __init__(self, path: str):
        self.path = path

    def get_last(self) -> Optional[int]:
        """Return the last ingested block number, or None if none."""
        if not os.path.exists(self.path):
            return None
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
            if "last_block" in data:
                return int(data["last_block"])
        except (OSError, ValueError, json.JSONDecodeError) as e:
            raise CheckpointError(f"Failed to read checkpoint: {e}")
        return None

    def update(self, block_number: int):
        """Atomically update the checkpoint to block_number."""
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump({"last_block": block_number}, f)
            os.replace(tmp, self.path)
        except OSError as e:
            raise CheckpointError(f"Failed to write checkpoint: {e}")
