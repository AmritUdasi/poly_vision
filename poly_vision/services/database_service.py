from prisma.client import Prisma
from typing import Optional, List, Dict, Any
from datetime import datetime
from poly_vision.utils.config import DatabaseConfig
from poly_vision.utils.enums import (
    BlockData,
    Transaction,
)
import asyncio

class DatabaseService:
    """Service for database operations."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.prisma = Prisma()
        self._is_connected = False
        self._lock = asyncio.Lock()  # Add lock for connection management

    async def connect(self):
        """Connect to database if not already connected."""
        async with self._lock:  # Use lock to prevent concurrent connections
            if not self._is_connected:
                try:
                    await self.prisma.connect()
                    self._is_connected = True
                except Exception as e:
                    if "Already connected" not in str(e):
                        raise e

    async def disconnect(self):
        """Disconnect from database if connected."""
        async with self._lock:  # Use lock to prevent concurrent disconnections
            if self._is_connected:
                try:
                    await self.prisma.disconnect()
                    self._is_connected = False
                except Exception as e:
                    if "Client is not connected" not in str(e):
                        raise e

    async def ensure_connection(self):
        """Ensure database connection is active."""
        if not self._is_connected:
            await self.connect()

    async def block_exists(self, block_number: int) -> bool:
        """Check if a block already exists in the database."""
        try:
            await self.ensure_connection()
            block = await self.prisma.blocks.find_unique(
                where={
                    "number": block_number
                }
            )
            return block is not None
        except Exception as e:
            return False

    async def save_block(self, block_data: BlockData):
        """Save block data to database using upsert."""
        try:
            block = {
                "timestamp": datetime.fromtimestamp(block_data.timestamp),
                "number": block_data.block_number,
                "hash": block_data.hash,
                "parent_hash": block_data.parent_hash,
                "nonce": block_data.nonce,
                "sha3_uncles": block_data.sha3_uncles,
                "logs_bloom": block_data.logs_bloom,
                "transactions_root": block_data.transactions_root,
                "state_root": block_data.state_root,
                "receipts_root": block_data.receipts_root,
                "miner": block_data.miner,
                "difficulty": 0,
                "total_difficulty": 0,
                "size": self.validate_int4(block_data.size),
                "extra_data": block_data.extra_data,
                "gas_limit": -2,
                "gas_used": -2,
                "transaction_count": block_data.transaction_count,
                "base_fee_per_gas": -2,
            }

            await self.prisma.blocks.upsert(
                where={
                    "number": block_data.block_number,
                },
                data={
                    "create": block,
                    "update": block,
                }
            )
        except Exception as e:
            raise Exception(f"Failed to save block {block_data.block_number}: {str(e)}")

    async def save_transaction(self, tx: Transaction):
        """Save transaction data to database."""
        try:
            # Convert transaction data to match schema
            MAX_INT_32 = 2147483647

            tx_data = {
                "hash": tx.hash,
                "nonce": tx.nonce,
                "transaction_index": 0,  # Default value since it's required
                "from_address": tx.from_,
                "to_address": tx.to,  # Optional in schema
                "value": (
                    str(tx.value) if tx.value is not None else None
                ),  # Optional Decimal
                "gas": tx.gas if tx.gas is not None else None,  # Optional Int
                "gas_price": (
                    tx.gasPrice
                    if tx.gasPrice is not None and tx.gasPrice <= MAX_INT_32
                    else (-2 if tx.gasPrice is not None else None)
                ),
                "input": tx.input if tx.input else None,  # Optional String
                "receipt_cumulative_gas_used": None,  # Optional Int
                "receipt_gas_used": (
                    tx.gas_used
                    if tx.gas_used is not None and tx.gas_used <= MAX_INT_32
                    else (-2 if tx.gas_used is not None else None)
                ),
                "receipt_contract_address": None,  # Optional String
                "receipt_root": None,  # Optional String
                "receipt_status": (
                    tx.status if tx.status is not None else None
                ),  # Optional Int
                "block_timestamp": datetime.fromtimestamp(
                    tx.blockNumber
                ),  # Required DateTime
                "block_number": tx.blockNumber,  # Required Int
                "block_hash": tx.blockHash,  # Required String
                "max_fee_per_gas": (
                    tx.maxFeePerGas
                    if tx.maxFeePerGas is not None and tx.maxFeePerGas <= MAX_INT_32
                    else (-2 if tx.maxFeePerGas is not None else None)
                ),
                "max_priority_fee_per_gas": (
                    tx.maxPriorityFeePerGas
                    if tx.maxPriorityFeePerGas is not None
                    and tx.maxPriorityFeePerGas <= MAX_INT_32
                    else (-2 if tx.maxPriorityFeePerGas is not None else None)
                ),
                "transaction_type": (
                    tx.type if tx.type is not None else None
                ),  # Optional Int
                "receipt_effective_gas_price": None,  # Optional Int
            }

            await self.prisma.transactions.create(data=tx_data)

            # Save traces if they exist
            if tx.traces:
                await self.save_traces(tx.traces, tx.hash, tx.blockNumber, tx.blockHash)

        except Exception as e:
            raise Exception(f"Failed to save transaction {tx.hash}: {str(e)}")

    def validate_int4(self, value: int) -> int:
        """Validate and convert integer to int4 range."""
        MAX_INT32 = 2147483647
        MIN_INT32 = -2147483648
        try:
            if value > MAX_INT32 or value < MIN_INT32:
                return -2
            return value
        except (TypeError, ValueError):
            return -2

    async def save_traces(
        self, traces: Dict[str, Any], tx_hash: str, block_number: int, block_hash: str
    ):
        """Save transaction traces to database."""
        try:
            # Process single trace
            trace_data = {
                "transaction_hash": tx_hash,
                "transaction_index": 0,
                "from_address": traces.get("from"),
                "to_address": traces.get("to"),
                "value": str(traces.get("value", "0")),
                "input": traces.get("input"),
                "output": traces.get("output"),
                "trace_type": traces.get("type", "call"),
                "call_type": traces.get("callType"),
                "reward_type": None,
                "gas": self._safe_hex_to_int(traces.get("gas")),
                "gas_used": self._safe_hex_to_int(traces.get("gasUsed")),
                "subtraces": len(traces.get("calls", [])),
                "trace_address": "",  # Root trace has empty trace_address
                "error": traces.get("error"),
                "status": 1 if not traces.get("error") else 0,
                "trace_id": tx_hash,
                "block_number": block_number,
                "block_hash": block_hash,
                "block_timestamp": datetime.fromtimestamp(block_number),
            }

            await self.prisma.traces.create(data=trace_data)

        except Exception as e:
            raise Exception(
                f"Failed to save traces for transaction {tx_hash}: {str(e)}"
            )

    async def save_transactions_batch(self, transactions: List[Transaction]):
        """Save multiple transactions in a batch."""
        try:
            await self.ensure_connection()
            tx_data_batch = [
                {
                    "hash": tx.hash,
                    "nonce": tx.nonce,
                    "transaction_index": 0,  # We'll need to get this from somewhere
                    "from_address": tx.from_,
                    "to_address": tx.to,
                    "value": str(tx.value),  # Convert to Decimal
                    "gas": tx.gas,
                    "gas_price": tx.gasPrice if tx.gasPrice <= 2147483647 else -2,
                    "input": tx.input,
                    "receipt_cumulative_gas_used": None,  # Need to get from receipt
                    "receipt_gas_used": None,  # Need to get from receipt
                    "receipt_contract_address": None,  # Need to get from receipt
                    "receipt_root": None,  # Need to get from receipt
                    "receipt_status": None,  # Need to get from receipt
                    "block_timestamp": datetime.fromtimestamp(tx.blockNumber),
                    "block_number": tx.blockNumber,
                    "block_hash": tx.blockHash,
                    "max_fee_per_gas": tx.maxFeePerGas if tx.maxFeePerGas and tx.maxFeePerGas <= 2147483647 else -2,
                    "max_priority_fee_per_gas": tx.maxPriorityFeePerGas if tx.maxPriorityFeePerGas and tx.maxPriorityFeePerGas <= 2147483647 else -2,
                    "transaction_type": tx.type,
                    "receipt_effective_gas_price": None,  # Need to get from receipt
                } for tx in transactions
            ]
            await self.prisma.transactions.create_many(data=tx_data_batch)
        except Exception as e:
            raise Exception(f"Failed to save transactions batch: {str(e)}")

    async def get_latest_block_number(self) -> Optional[int]:
        """Get the latest block number from the database."""
        try:
            await self.ensure_connection()
            latest_block = await self.prisma.blocks.find_first(order={"number": "desc"})
            return latest_block.number if latest_block else None
        except Exception as e:
            raise Exception(f"Failed to get latest block: {str(e)}")

    async def process_traces_for_saving(
        self,
        trace: Dict[str, Any],
        tx_hash: str,
        block_number: int,
        block_hash: str,
        block_timestamp: int,
        parent_address: str = "",
        depth: int = 0,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Process traces recursively and prepare them for saving.

        Args:
            trace: The trace data to process
            tx_hash: Transaction hash
            block_number: Block number
            block_hash: Block hash
            block_timestamp: Block timestamp
            parent_address: Parent trace address for nested calls
            depth: Current recursion depth
            max_depth: Maximum recursion depth

        Returns:
            List of processed traces ready for database insertion
        """
        if depth > max_depth:
            print(f"Max depth {max_depth} reached for tx {tx_hash}")
            return []

        traces_to_save = []

        try:
            # Process single trace
            trace_data = {
                "transaction_hash": tx_hash,
                "transaction_index": 0,
                "from_address": trace.get("from"),
                "to_address": trace.get("to"),
                "value": str(
                    trace.get("value", "0")
                ),  # Ensure string for large numbers
                "input": trace.get("input"),
                "output": trace.get("output"),
                "trace_type": trace.get("type", "call"),
                "call_type": trace.get("callType"),
                "reward_type": None,
                "gas": self._safe_hex_to_int(trace.get("gas")),
                "gas_used": self._safe_hex_to_int(trace.get("gasUsed")),
                "subtraces": len(trace.get("calls", [])),
                "trace_address": parent_address,
                "error": trace.get("error"),
                "status": 1 if not trace.get("error") else 0,
                "trace_id": (
                    f"{tx_hash}-{parent_address}" if parent_address else tx_hash
                ),
                "block_number": block_number,
                "block_hash": block_hash,
                "block_timestamp": self._safe_timestamp_to_datetime(block_timestamp),
            }

            traces_to_save.append(trace_data)

            # Process nested calls
            for i, call in enumerate(trace.get("calls", [])):
                if not isinstance(call, dict):
                    print(f"Invalid call data in tx {tx_hash}")
                    continue

                trace_address = f"{parent_address}-{i}" if parent_address else str(i)
                nested_traces = await self.process_traces_for_saving(
                    call,
                    tx_hash,
                    block_number,
                    block_hash,
                    block_timestamp,
                    trace_address,
                    depth + 1,
                    max_depth,
                )
                traces_to_save.extend(nested_traces)

            return traces_to_save

        except Exception as e:
            print(f"Error processing trace for tx {tx_hash}: {str(e)}")
            return traces_to_save

    def _safe_hex_to_int(self, hex_value: Any) -> int:
        """Safely convert hex value to int."""
        try:
            if isinstance(hex_value, str):
                return int(hex_value, 16)
            elif isinstance(hex_value, int):
                return hex_value
            return 0
        except ValueError:
            return 0

    def _safe_timestamp_to_datetime(self, timestamp: Any) -> datetime:
        """Safely convert timestamp to datetime."""
        try:
            return datetime.fromtimestamp(int(timestamp))
        except (ValueError, TypeError):
            return datetime.utcnow()

    async def save_traces_batch(self, traces: List[Dict[str, Any]]):
        """Save multiple traces in a batch."""
        try:
            await self.ensure_connection()
            # Convert the traces to match schema
            formatted_traces = [
                {
                    "transaction_hash": trace["transaction_hash"],
                    "transaction_index": trace["transaction_index"],
                    "from_address": trace["from_address"],
                    "to_address": trace["to_address"],
                    "value": trace["value"],
                    "input": trace["input"],
                    "output": trace["output"],
                    "trace_type": trace["trace_type"],
                    "call_type": trace["call_type"],
                    "reward_type": trace["reward_type"],
                    "gas": int(trace["gas"], 16) if isinstance(trace["gas"], str) and trace["gas"].startswith("0x") else trace["gas"],
                    "gas_used": int(trace["gas_used"], 16) if isinstance(trace["gas_used"], str) and trace["gas_used"].startswith("0x") else trace["gas_used"],
                    "subtraces": trace["subtraces"],
                    "trace_address": trace["trace_address"],
                    "error": trace["error"],
                    "status": trace["status"],
                    "trace_id": trace["trace_id"],
                    "block_number": trace["block_number"],
                    "block_hash": trace["block_hash"],
                    "block_timestamp": trace["block_timestamp"],
                } for trace in traces
            ]
            await self.prisma.traces.create_many(data=formatted_traces)
        except Exception as e:
            raise Exception(f"Failed to save traces batch: {str(e)}")
