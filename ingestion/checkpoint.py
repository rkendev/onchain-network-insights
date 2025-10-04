"""
ingestion.checkpoint

Manage checkpoint (last ingested block) logic.
"""
def get_last_ingested() -> int:
    raise NotImplementedError("get_last_ingested not implemented")

def update_last_ingested(block_number: int):
    raise NotImplementedError("update_last_ingested not implemented")
