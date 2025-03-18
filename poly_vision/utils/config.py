from pydantic import BaseModel, HttpUrl, PostgresDsn, validator
from typing import Optional
from dynaconf import Dynaconf
from poly_vision.configs.config import settings


class BlockchainConfig(BaseModel):
    """Blockchain RPC configuration."""

    rpc_url: HttpUrl
    ws_url: Optional[HttpUrl]
    chain_id: int = 137
    max_retries: int = 3
    timeout: int = 30
    use_websocket: bool = False

    @validator("rpc_url")
    def validate_rpc_url(cls, v):
        if not str(v).startswith(("http://", "https://")):
            raise ValueError("RPC URL must start with http:// or https://")
        return v

    @validator("ws_url")
    def validate_ws_url(cls, v):
        if v and not str(v).startswith(("ws://", "wss://")):
            raise ValueError("Websocket URL must start with ws:// or wss://")
        return v


class DatabaseConfig(BaseModel):
    """Database configuration."""

    url: PostgresDsn
    max_connections: int = 20
    connection_timeout: int = 30


class IndexerConfig(BaseModel):
    """Main indexer configuration."""

    blockchain: BlockchainConfig
    database: DatabaseConfig
    batch_size: int = 100
    max_blocks_per_request: int = 1000
    worker_count: int = 4


def load_config() -> IndexerConfig:
    """Load configuration from environment variables."""

    return IndexerConfig(
        blockchain=BlockchainConfig(
            rpc_url=settings.get("POLYGON_RPC_URL", "https://polygon-rpc.com"),
            chain_id=settings.get("CHAIN_ID", 137),
            max_retries=settings.get("RPC_MAX_RETRIES", 3),
            timeout=settings.get("RPC_TIMEOUT", 30),
        ),
        database=DatabaseConfig(
            url=settings.get("DATABASE_URL"),
            max_connections=settings.get("DB_MAX_CONNECTIONS", 20),
            connection_timeout=settings.get("DB_CONNECTION_TIMEOUT", 30),
        ),
        batch_size=settings.get("INDEXER_BATCH_SIZE", 100),
        max_blocks_per_request=settings.get("MAX_BLOCKS_PER_REQUEST", 1000),
        worker_count=settings.get("WORKER_COUNT", 4),
    )
