import os
from typing import Literal, Optional, Any
from pydantic import BaseModel, Field, model_validator, ValidationError


class RPCConfig(BaseModel):
    url: str
    timeout: int = Field(default=10, ge=1)
    max_retries: int = Field(default=3, ge=0)


class BigQueryConfig(BaseModel):
    project_id: str
    dataset: str


class APIConfig(BaseModel):
    etherscan_key: Optional[str]


class CheckpointConfig(BaseModel):
    file: str


class Settings(BaseModel):
    data_source: Literal["bigquery", "rpc", "api"]
    bigquery: Optional[BigQueryConfig]
    rpc: Optional[RPCConfig]
    api: Optional[APIConfig]
    checkpoint: CheckpointConfig

    @model_validator(mode="after")
    def validate_source(self) -> "Settings":
        ds = self.data_source
        if ds == "rpc" and self.rpc is None:
            raise ValueError("RPC mode but missing rpc configuration")
        if ds == "bigquery" and self.bigquery is None:
            raise ValueError("BigQuery mode but missing bigquery configuration")
        if ds == "api" and self.api is None:
            raise ValueError("API mode but missing api configuration")
        # Enforce HTTPS for RPC
        if ds == "rpc":
            url = self.rpc.url
            if not url.lower().startswith("https://"):
                raise ValueError("RPC URL must be HTTPS")
        return self

def load_settings(path: str = "config.yaml") -> Settings:
    import yaml

    with open(path, "r") as f:
        cfg = yaml.safe_load(f) or {}

    # Override sensitive parts from environment
    if "rpc" in cfg:
        env_rpc = os.environ.get("RPC_URL_OVERRIDE")
        if env_rpc:
            cfg["rpc"]["url"] = env_rpc

    try:
        settings = Settings.model_validate(cfg)
    except ValidationError as e:
        raise RuntimeError(f"Configuration error in {path}: {e}") from e

    return settings
