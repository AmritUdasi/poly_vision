from pydantic import BaseModel
from enum import Enum, auto
from typing import Optional, List, Dict, Any
from datetime import datetime


class IndexingStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"
    PENDING = "pending"
    COMPLETED = "completed"
    SKIPPED = "skipped"

class TransactionType(Enum):
    LEGACY = 0
    ACCESS_LIST = 1
    DYNAMIC_FEE = 2

class BlockchainNetwork(Enum):
    POLYGON_MAINNET = 137
    POLYGON_MUMBAI = 80001

class IndexingErrorType(Enum):
    RPC_ERROR = auto()
    DB_ERROR = auto()
    VALIDATION_ERROR = auto()
    TIMEOUT_ERROR = auto()
    UNKNOWN_ERROR = auto()

# Pydantic models for validation
class BlockData(BaseModel):
    """Block data validation model."""
    number: int
    hash: str
    parent_hash: str
    nonce: str
    timestamp: datetime
    transactions_root: str
    state_root: str
    receipts_root: str
    miner: str
    difficulty: int
    total_difficulty: int
    size: int
    gas_limit: int
    gas_used: int
    base_fee_per_gas: Optional[int]
    transaction_count: int

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }

    def dict(self, *args, **kwargs):
        # Convert all values to JSON serializable format
        d = super().dict(*args, **kwargs)
        # Convert any large integers to strings to avoid JSON serialization issues
        d['difficulty'] = str(d['difficulty'])
        d['total_difficulty'] = str(d['total_difficulty'])
        return d

class TransactionData(BaseModel):
    """Transaction data validation model."""
    hash: str
    nonce: int
    block_hash: str
    block_number: int
    transaction_index: int
    from_address: str
    to_address: Optional[str]
    value: int
    gas: int
    gas_price: int
    input: str
    block_timestamp: datetime
    transaction_type: Optional[int]
    max_fee_per_gas: Optional[int]
    max_priority_fee_per_gas: Optional[int]
    receipt_cumulative_gas_used: int
    receipt_gas_used: int
    receipt_contract_address: Optional[str]
    receipt_status: int
    receipt_effective_gas_price: Optional[int]

class IndexingResult(BaseModel):
    """Indexing result model."""
    status: IndexingStatus
    block_number: int
    transactions_indexed: Optional[int]
    error_type: Optional[IndexingErrorType]
    error_message: Optional[str]
    timestamp: datetime = None
