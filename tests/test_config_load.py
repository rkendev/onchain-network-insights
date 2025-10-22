import os
import pathlib

def test_config_file_exists_and_has_placeholders():
    root = pathlib.Path(__file__).resolve().parents[1]
    cfg = root / "config.yaml"
    assert cfg.exists(), "config.yaml missing at project root"
    text = cfg.read_text(encoding="utf-8")
    # Ensure no obvious secrets are present
    forbidden = ["http://", "https://", "AKIA", "AIza", "secret:", "token:", "key:"]
    # allow placeholders like YOUR_RPC_URL
    assert all(k not in text for k in forbidden), "config.yaml contains potential secrets or live URLs"
