from pydantic import BaseModel, Field
from enum import Enum, auto
from typing import Optional, List, Dict, Any
from datetime import datetime


class TransactionType(Enum):
    LEGACY = 0
    ACCESS_LIST = 1
    DYNAMIC_FEE = 2

class BlockchainNetwork(Enum):
    POLYGON_MAINNET = 137
    POLYGON_MUMBAI = 80001


# Updated Pydantic models
class TransactionReceipt(BaseModel):
    transaction_hash: str
    transaction_index: int
    block_hash: str
    block_number: int
    from_: str = Field(alias="from")
    to: Optional[str]
    cumulative_gas_used: int
    gas_used: int
    contract_address: Optional[str]
    logs: List[Dict]
    status: int
    effective_gas_price: int

class Transaction(BaseModel):
    hash: str
    blockHash: str
    blockNumber: int
    from_: str
    to: Optional[str]
    gas: int
    gasPrice: int
    maxFeePerGas: Optional[int]
    maxPriorityFeePerGas: Optional[int]
    input: str
    nonce: int
    value: int
    type: int
    chainId: int
    v: int
    r: str
    s: str
    yParity: Optional[int]
    gas_used: int
    status: int
    traces: Optional[Dict]

class BlockData(BaseModel):
    block_number: int
    timestamp: int
    hash: str
    parent_hash: str
    nonce: str
    sha3_uncles: Optional[str]
    logs_bloom: Optional[str]
    transactions_root: str
    state_root: str
    receipts_root: str
    miner: str
    difficulty: str  # Using str for Decimal
    total_difficulty: str  # Using str for Decimal
    size: int
    extra_data: Optional[str]
    gas_limit: int
    gas_used: int
    transaction_count: int
    base_fee_per_gas: Optional[int]
    transactions: List[Transaction] = []

    class Config:
        arbitrary_types_allowed = True


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
