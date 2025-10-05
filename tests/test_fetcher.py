import pytest
from ingestion.fetcher import fetch_block

def test_fetch_block_invalid_raises():
    with pytest.raises(ValueError):
        fetch_block(-1)

def test_fetch_block_mocked(monkeypatch):
    # Mock requests.post so we don't hit a real RPC
    import requests

    class FakeResp:
        def __init__(self, json_data, status_code=200):
            self._json = json_data
            self.status_code = status_code
        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"status {self.status_code}")
        def json(self):
            return self._json

    def fake_post(url, json, timeout):
        # minimal emulation of JSON-RPC success for block 0
        return FakeResp({"jsonrpc": "2.0", "id": 1, "result": {"number": "0x0"}})

    monkeypatch.setattr(requests, "post", fake_post)

    blk = fetch_block(0)
    assert int(blk["number"], 16) == 0
