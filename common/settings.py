import os
from pydantic import BaseModel, field_validator, ValidationError

class RPC(BaseModel):
    url: str
    timeout: int = 30

    @field_validator("url")
    @classmethod
    def must_be_https(cls, v: str) -> str:
        # allow placeholder during tests by swapping in a safe default
        if "${" in v:
            return "https://example.invalid"
        if not v.startswith("https://"):
            raise ValueError("RPC URL must be HTTPS")
        return v

class DB(BaseModel):
    driver: str = "sqlite"
    sqlite_path: str = "sandbox_whales.db"

class Ingestion(BaseModel):
    start_block: int
    end_block: int
    max_retries: int = 3
    backoff_seconds: float = 1.5

class CheckpointCfg(BaseModel):
    file: str = "checkpoint.json"

class Providers(BaseModel):
    class Etherscan(BaseModel):
        api_key: str | None = None
    etherscan: Etherscan = Etherscan()

class Settings(BaseModel):
    data_source: str = "rpc"
    network: str = "ethereum"
    rpc: RPC
    db: DB
    ingestion: Ingestion
    checkpoint: CheckpointCfg
    providers: Providers = Providers()

def load_settings(path: str = "config.yaml") -> Settings:
    import yaml
    with open(path, "r") as f:
        cfg = yaml.safe_load(f) or {}

    # allow secure override via env at runtime
    if "rpc" in cfg:
        env_rpc = os.environ.get("RPC_URL_OVERRIDE")
        if env_rpc:
            cfg["rpc"]["url"] = env_rpc

    try:
        return Settings.model_validate(cfg)
    except ValidationError as e:
        raise RuntimeError(f"Configuration error in {path}: {e}") from e
