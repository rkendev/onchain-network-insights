import pathlib

def test_config_file_exists_and_has_placeholders():
    root = pathlib.Path(__file__).resolve().parents[1]
    cfg = root / "config.yaml"
    assert cfg.exists(), "config.yaml missing at project root"
    text = cfg.read_text(encoding="utf-8")

    forbidden = ["http://", "https://", "AKIA", "AIza", "secret:", "token:", "key:"]

    def safe(line: str) -> bool:
        if "${" in line:
            return True
        return not any(bad in line for bad in forbidden)

    assert all(safe(line) for line in text.splitlines()), "config.yaml contains potential secrets or live URLs"
